#!/usr/bin/env python3
import csv
import json
import logging
import os
import signal
import socket
import subprocess
import threading
import time
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from ipaddress import ip_address, ip_network

import inotify_simple
import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

POOLS_FILE       = os.environ.get('POOLS_FILE', '/data/dhcp-pools/pools.json')
RESERVATIONS_FILE = os.environ.get('RESERVATIONS_FILE', '/data/dhcp-pools/reservations.csv')
WATCH_DIR        = os.environ.get('WATCH_DIR', '/data/dhcp-pools')
DB_HOST          = os.environ.get('DB_HOST', 'dhcp-postgres-rw.dhcp-system.svc.cluster.local')
DB_NAME          = os.environ.get('DB_NAME', 'dhcp')
DB_USER          = os.environ.get('DB_USER', 'app')
DB_PASS          = os.environ.get('DHCP_SQL_PASSWORD', '')
PORT             = int(os.environ.get('SIDECAR_PORT', '8080'))

_data_lock = threading.Lock()
_pools = {}         # vlan_id -> pool dict
_reservations = []  # list of {mac, ip, name, vlan_id}
_leases = []        # list of {pool_name, ip, mac, gateway, expiry, lease_start}


# ── data loading ─────────────────────────────────────────────────────────────

def load_static_data():
    global _pools, _reservations
    try:
        with open(POOLS_FILE) as f:
            raw = json.load(f)
        pools = {}
        for vlan_id, cfg in raw.get('vlans', {}).items():
            pools[vlan_id] = {
                'vlan_id': vlan_id,
                'network': cfg['network'],
                'gateway': cfg['gateway'],
                'range_start': ip_address(cfg['range_start']),
                'range_stop': ip_address(cfg['range_stop']),
                'lease_time': cfg.get('lease_time_seconds', 86400),
                'pool_name': f'vlan{vlan_id}',
                'total': int(ip_address(cfg['range_stop'])) - int(ip_address(cfg['range_start'])) + 1,
            }

        reservations = []
        with open(RESERVATIONS_FILE, newline='') as f:
            for row in csv.DictReader(f):
                reservations.append({
                    'mac': row['mac'].lower(),
                    'ip': row['ip'],
                    'name': row['name'],
                    'vlan_id': row['vlan_id'],
                })

        with _data_lock:
            _pools.clear()
            _pools.update(pools)
            _reservations.clear()
            _reservations.extend(reservations)
        log.info("Loaded %d pools, %d reservations", len(pools), len(reservations))
    except Exception:
        log.exception("Failed to load static data")


def load_leases():
    global _leases
    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS,
                                connect_timeout=5)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT pool_name, framedipaddress::text AS ip, pool_key AS mac,
                       gateway, expiry_time
                FROM dhcpippool
                WHERE pool_key != '0' AND expiry_time > NOW()
                ORDER BY pool_name, framedipaddress
            """)
            rows = cur.fetchall()
        conn.close()

        with _data_lock:
            pools_snap = dict(_pools)

        leases = []
        for row in rows:
            pool_name = row['pool_name']
            vlan_id = pool_name.replace('vlan', '')
            pool_cfg = pools_snap.get(vlan_id, {})
            lease_time = pool_cfg.get('lease_time', 86400)
            expiry = row['expiry_time']
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            leases.append({
                'pool_name': pool_name,
                'vlan_id': vlan_id,
                'ip': row['ip'],
                'mac': row['mac'].lower(),
                'gateway': row['gateway'],
                'expiry': expiry,
                'lease_start': expiry - timedelta(seconds=lease_time),
            })

        with _data_lock:
            _leases.clear()
            _leases.extend(leases)
        log.debug("Loaded %d active leases", len(leases))
    except Exception:
        log.exception("Failed to load leases from DB")


def lease_refresh_loop():
    while True:
        load_leases()
        time.sleep(30)


def ptr_lookup(ip, timeout=0.5):
    try:
        socket.setdefaulttimeout(timeout)
        return socket.gethostbyaddr(ip)[0]
    except Exception:
        return None
    finally:
        socket.setdefaulttimeout(None)


# ── hot reload ────────────────────────────────────────────────────────────────

def reload_freeradius():
    try:
        result = subprocess.run(['pgrep', '-x', 'freeradius'], capture_output=True, text=True)
        pid = int(result.stdout.strip())
        os.kill(pid, signal.SIGHUP)
        log.info("Sent SIGHUP to freeradius PID %d", pid)
    except Exception:
        log.exception("Failed to reload freeradius")


def watch_config():
    inotify = inotify_simple.INotify()
    inotify.add_watch(WATCH_DIR,
                      inotify_simple.flags.MODIFY |
                      inotify_simple.flags.CREATE |
                      inotify_simple.flags.MOVED_TO)
    log.info("Watching %s for config changes", WATCH_DIR)
    while True:
        events = inotify.read(timeout=None)
        if not events:
            continue
        log.info("Config change detected (%s), reloading", [e.name for e in events])
        time.sleep(1)  # debounce
        load_static_data()
        reload_freeradius()


# ── snapshot helpers ──────────────────────────────────────────────────────────

def snapshot():
    with _data_lock:
        pools = dict(_pools)
        reservations = list(_reservations)
        leases = list(_leases)
    return pools, reservations, leases


def build_vlan_view(pools, reservations, leases):
    """Returns list of vlan dicts sorted by vlan_id, each with entries and stats."""
    now = datetime.now(timezone.utc)
    lease_by_ip = {l['ip']: l for l in leases}

    res_by_vlan = {}
    for r in reservations:
        res_by_vlan.setdefault(r['vlan_id'], []).append(r)

    views = []
    for vlan_id in sorted(pools.keys(), key=int):
        pool = pools[vlan_id]
        pool_reservations = res_by_vlan.get(vlan_id, [])

        # dynamic pool IPs in range that have active leases
        active_in_range = [
            l for l in leases
            if l['vlan_id'] == vlan_id and
               pool['range_start'] <= ip_address(l['ip']) <= pool['range_stop']
        ]
        available = pool['total'] - len(active_in_range)

        # build unified entry list
        entries = []
        for r in sorted(pool_reservations, key=lambda x: ip_address(x['ip'])):
            lease = lease_by_ip.get(r['ip'])
            entries.append({
                'ip': r['ip'],
                'name': r['name'],
                'mac': r['mac'],
                'type': 'static',
                'lease_start': lease['lease_start'] if lease else None,
                'expiry': lease['expiry'] if lease else None,
                'online': lease is not None,
            })

        for l in sorted(active_in_range, key=lambda x: ip_address(x['ip'])):
            if any(e['ip'] == l['ip'] for e in entries):
                continue
            entries.append({
                'ip': l['ip'],
                'name': ptr_lookup(l['ip']) or '—',
                'mac': l['mac'],
                'type': 'dynamic',
                'lease_start': l['lease_start'],
                'expiry': l['expiry'],
                'online': True,
            })

        views.append({
            'vlan_id': vlan_id,
            'pool': pool,
            'entries': entries,
            'total': pool['total'],
            'available': available,
            'active': len(active_in_range),
            'static_count': len(pool_reservations),
        })
    return views


# ── HTTP handler ──────────────────────────────────────────────────────────────

def fmt_ts(dt):
    if dt is None:
        return '—'
    return dt.strftime('%Y-%m-%d %H:%M')


def fmt_delta(dt):
    if dt is None:
        return '—'
    now = datetime.now(timezone.utc)
    delta = dt - now
    if delta.total_seconds() < 0:
        return 'expired'
    h, rem = divmod(int(delta.total_seconds()), 3600)
    m = rem // 60
    if h > 24:
        return f'{h // 24}d {h % 24}h'
    return f'{h}h {m}m'


CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font: 14px/1.5 'SF Mono', 'Fira Code', monospace; background: #0f0f0f; color: #d4d4d4; padding: 24px; }
h1 { font-size: 18px; color: #fff; margin-bottom: 24px; }
.vlan { margin-bottom: 40px; }
.vlan-header { display: flex; align-items: baseline; gap: 16px; margin-bottom: 12px; }
.vlan-title { font-size: 15px; color: #fff; font-weight: bold; }
.vlan-sub { font-size: 12px; color: #888; }
.pool-bar-wrap { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; }
.pool-bar { height: 6px; width: 160px; background: #2a2a2a; border-radius: 3px; overflow: hidden; }
.pool-bar-fill { height: 100%; background: #3fb950; border-radius: 3px; transition: width 0.3s; }
.pool-label { font-size: 12px; color: #888; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; padding: 6px 12px; color: #888; font-weight: normal;
     border-bottom: 1px solid #2a2a2a; white-space: nowrap; }
td { padding: 6px 12px; border-bottom: 1px solid #1a1a1a; white-space: nowrap; }
tr:hover td { background: #1a1a1a; }
.badge { display: inline-block; padding: 1px 7px; border-radius: 10px; font-size: 11px; }
.badge-static  { background: #1f3a2e; color: #3fb950; }
.badge-dynamic { background: #1f2d3a; color: #58a6ff; }
.online  { color: #3fb950; }
.offline { color: #484848; }
.ts { color: #888; font-size: 12px; }
.exp-soon { color: #d29922; }
"""


def render_html(views):
    now = datetime.now(timezone.utc)
    parts = [f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>DHCP Leases</title>
<meta http-equiv="refresh" content="60">
<style>{CSS}</style></head>
<body>
<h1>DHCP Leases</h1>
"""]
    for v in views:
        pool = v['pool']
        pct = int((v['active'] / v['total']) * 100) if v['total'] else 0
        avail_pct = 100 - pct
        parts.append(f"""<div class="vlan">
<div class="vlan-header">
  <span class="vlan-title">VLAN {v['vlan_id']}</span>
  <span class="vlan-sub">{pool['network']} &mdash; gw {pool['gateway']}</span>
</div>
<div class="pool-bar-wrap">
  <div class="pool-bar"><div class="pool-bar-fill" style="width:{avail_pct}%"></div></div>
  <span class="pool-label">Dynamic pool {pool['range_start']}–{pool['range_stop']}: {v['available']}/{v['total']} available &nbsp;|&nbsp; {v['static_count']} static reservations</span>
</div>
<table>
<thead><tr>
  <th>IP</th><th>Name</th><th>MAC</th><th>Type</th><th>Lease Start</th><th>Expires</th><th>Remaining</th>
</tr></thead><tbody>
""")
        for e in v['entries']:
            online_cls = 'online' if e['online'] else 'offline'
            dot = '●' if e['online'] else '○'
            badge = f'<span class="badge badge-{e["type"]}">{e["type"]}</span>'
            ls = f'<span class="ts">{fmt_ts(e["lease_start"])}</span>'
            exp = f'<span class="ts">{fmt_ts(e["expiry"])}</span>'
            remaining = ''
            if e['expiry']:
                delta = e['expiry'] - now
                remaining_cls = 'exp-soon' if 0 < delta.total_seconds() < 3600 else 'ts'
                remaining = f'<span class="{remaining_cls}">{fmt_delta(e["expiry"])}</span>'
            parts.append(
                f'<tr><td><span class="{online_cls}">{dot}</span> {e["ip"]}</td>'
                f'<td>{e["name"]}</td>'
                f'<td>{e["mac"]}</td>'
                f'<td>{badge}</td>'
                f'<td>{ls}</td>'
                f'<td>{exp}</td>'
                f'<td>{remaining}</td></tr>\n'
            )
        parts.append('</tbody></table></div>\n')

    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    parts.append(f'<p style="color:#484848;font-size:11px;margin-top:24px">Updated {ts} &nbsp;|&nbsp; auto-refresh 60s</p></body></html>')
    return ''.join(parts)


def render_metrics(views):
    lines = []
    lines.append('# HELP dhcp_pool_total Total IPs in dynamic pool range')
    lines.append('# TYPE dhcp_pool_total gauge')
    for v in views:
        pool = v['pool']
        lbl = f'vlan="{v["vlan_id"]}",pool="{pool["pool_name"]}",network="{pool["network"]}"'
        lines.append(f'dhcp_pool_total{{{lbl}}} {v["total"]}')

    lines.append('# HELP dhcp_pool_available Available IPs in dynamic pool')
    lines.append('# TYPE dhcp_pool_available gauge')
    for v in views:
        pool = v['pool']
        lbl = f'vlan="{v["vlan_id"]}",pool="{pool["pool_name"]}",network="{pool["network"]}"'
        lines.append(f'dhcp_pool_available{{{lbl}}} {v["available"]}')

    lines.append('# HELP dhcp_pool_leases_active Active dynamic leases')
    lines.append('# TYPE dhcp_pool_leases_active gauge')
    for v in views:
        pool = v['pool']
        lbl = f'vlan="{v["vlan_id"]}",pool="{pool["pool_name"]}"'
        lines.append(f'dhcp_pool_leases_active{{{lbl}}} {v["active"]}')

    lines.append('# HELP dhcp_static_reservations_total Static DHCP reservations')
    lines.append('# TYPE dhcp_static_reservations_total gauge')
    for v in views:
        lbl = f'vlan="{v["vlan_id"]}",pool="{v["pool"]["pool_name"]}"'
        lines.append(f'dhcp_static_reservations_total{{{lbl}}} {v["static_count"]}')

    return '\n'.join(lines) + '\n'


def render_json(views):
    out = []
    for v in views:
        entries = []
        for e in v['entries']:
            entries.append({
                'ip': e['ip'],
                'name': e['name'],
                'mac': e['mac'],
                'type': e['type'],
                'online': e['online'],
                'lease_start': e['lease_start'].isoformat() if e['lease_start'] else None,
                'expiry': e['expiry'].isoformat() if e['expiry'] else None,
            })
        out.append({
            'vlan_id': v['vlan_id'],
            'network': v['pool']['network'],
            'pool_total': v['total'],
            'pool_available': v['available'],
            'pool_active': v['active'],
            'static_count': v['static_count'],
            'entries': entries,
        })
    return json.dumps(out, indent=2)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        pools, reservations, leases = snapshot()
        views = build_vlan_view(pools, reservations, leases)

        if self.path == '/metrics':
            body = render_metrics(views).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; version=0.0.4')
        elif self.path == '/leases':
            body = render_json(views).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
        elif self.path == '/reload':
            reload_freeradius()
            body = b'OK\n'
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
        elif self.path in ('/', '/index.html'):
            body = render_html(views).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'not found\n')
            return

        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == '__main__':
    load_static_data()
    load_leases()

    threading.Thread(target=lease_refresh_loop, daemon=True).start()
    threading.Thread(target=watch_config, daemon=True).start()

    log.info("Listening on :%d", PORT)
    HTTPServer(('', PORT), Handler).serve_forever()
