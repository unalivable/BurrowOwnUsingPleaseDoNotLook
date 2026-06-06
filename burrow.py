#!/usr/bin/env python3
"""
 ____  __  __  ____  ____  _____  _    _
(  _ \(  )(  )(  _ \(  _ \(  _  )( \/\/ )
 ) _ < )(__)(  )   / )   / )(_)(  )    (
(____/(______)(_)\_)(_)\_)(_____)(__/\__)

Copyright (c) 2026 unalivable
…
"""

import argparse, asyncio, json, os, signal, socket, struct, time, uuid
import urllib.request, urllib.parse, requests, threading, traceback, ssl, re
from aioice import Candidate, Connection
from aioice import stun, turn
from aioice.stun import TransactionFailed
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from rich import box

console = Console()

# ── Monkey‑patch aioice to use Send Indication instead of ChannelBind ──

if "DATA" not in stun.ATTRIBUTES_BY_NAME:
    stun.ATTRIBUTES_BY_NAME["DATA"] = (0x0013, "DATA", stun.pack_bytes, stun.unpack_bytes)
    stun.ATTRIBUTES_BY_TYPE[0x0013] = (0x0013, "DATA", stun.pack_bytes, stun.unpack_bytes)

_original_send_data = turn.TurnClientMixin.send_data

async def _patched_send_data(self, data, addr):
    try:
        req = stun.Message(message_method=stun.Method.CREATE_PERM, message_class=stun.Class.REQUEST)
        req.attributes["XOR-PEER-ADDRESS"] = addr
        await self.request_with_retry(req)
    except TransactionFailed:
        pass

    msg = stun.Message(message_method=stun.Method.SEND, message_class=stun.Class.INDICATION)
    msg.attributes["XOR-PEER-ADDRESS"] = addr
    msg.attributes["DATA"] = data
    self.send_stun(msg, self.server)

turn.TurnClientMixin.send_data = _patched_send_data

_original_dgram_recv = turn.TurnClientMixin.datagram_received

def _patched_dgram_recv(self, data, addr):
    try:
        msg = stun.parse_message(data)
        if msg.message_method == stun.Method.DATA and msg.message_class == stun.Class.INDICATION:
            if self.receiver:
                payload = msg.attributes.get("DATA", b"")
                peer = msg.attributes.get("XOR-PEER-ADDRESS", None)
                if peer:
                    self.receiver.datagram_received(payload, peer)
                return
    except Exception:
        pass
    _original_dgram_recv(self, data, addr)

turn.TurnClientMixin.datagram_received = _patched_dgram_recv


# ═══════════════════════════════════════════════════════════════
# Animated banner
# ═══════════════════════════════════════════════════════════════
BANNER = r"""
 ____  __  __  ____  ____  _____  _    _
(  _ \(  )(  )(  _ \(  _ \(  _  )( \/\/ )
 ) _ < )(__)(  )   / )   / )(_)(  )    (
(____/(______)(_)\_)(_)\_)(_____)(__/\__)
"""
COPYRIGHT = "Copyright (c) 2026 unalivable"

W = "https://webdav.yandex.ru/burrow-signal"
try:
    from burrow_secrets import YA_LOGIN, YA_PASSWORD
    A = (YA_LOGIN, YA_PASSWORD)
except:
    A = ("", "")
Y = "5.255.211.245"

_quit_flag = False
_cleanup_done = False

CONFIG_DIR = os.path.expanduser("~/.burrow")
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.json")
os.makedirs(CONFIG_DIR, exist_ok=True)
MAX_CLIENTS = 10

def _load_config():
    if os.path.exists(CONFIG_FILE):
        try: return json.load(open(CONFIG_FILE))
        except: pass
    return {"port": 9000, "upstream": "musicclips.videolinks.ru:8443", "mode": "client", "link_id": ""}

def _save_config(cfg):
    with open(CONFIG_FILE, "w") as f: json.dump(cfg, f, indent=2)

def _c():
    global _cleanup_done
    if _cleanup_done: return
    _cleanup_done = True
    for slot in range(MAX_CLIENTS):
        for f in [f"offer_{slot}.sdp", f"answer_{slot}.sdp"]:
            try: requests.delete(f"{W}/{f}", auth=A)
            except: pass

def _sig_handler(sig, frame):
    global _quit_flag
    if _quit_flag: os._exit(1)
    _quit_flag = True
    _c()
    os._exit(0)

signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)

def _resolve(host, dns='77.88.8.8'):
    try: socket.inet_aton(host); return host
    except: pass
    try:
        tid = os.urandom(2)
        flags = 0x0100
        header = struct.pack('!HHHHHH', int.from_bytes(tid, 'big'), flags, 1, 0, 0, 0)
        qname = b''
        for label in host.split('.'): qname += bytes([len(label)]) + label.encode()
        qname += b'\x00'
        question = qname + struct.pack('!HH', 1, 1)
        packet = header + question
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(2)
        sock.sendto(packet, (dns, 53))
        data, _ = sock.recvfrom(512)
        sock.close()
        pos = 12
        while pos < len(data) and data[pos] != 0: pos += 1 + data[pos]
        pos += 5
        ancount = struct.unpack('!H', data[6:8])[0]
        for _ in range(ancount):
            if pos + 10 > len(data): break
            if data[pos] & 0xc0 == 0xc0: pos += 2
            else:
                while data[pos] != 0: pos += 1 + data[pos]; pos += 1
            rtype, rclass, ttl, rdlength = struct.unpack('!HHIH', data[pos:pos+10])
            pos += 10
            if rtype == 1 and rdlength == 4:
                return socket.inet_ntop(socket.AF_INET, data[pos:pos+4])
            pos += rdlength
        return host
    except: return host

def _parse_turn_uri(uri):
    m = re.match(r"turn(?:s)?\:(?P<host>[^?:]+)(?:\:(?P<port>\d+))?(?:\?transport=(?P<transport>\w+))?", uri)
    if not m: raise ValueError(f"Invalid TURN URI: {uri}")
    host = m.group("host"); port = int(m.group("port") or 3478)
    transport = m.group("transport") or "udp"; ssl = uri.startswith("turns")
    return host, port, transport, ssl

def _make_connection(turn_uri, username, credential, stun_server=None):
    host, port, transport, ssl = _parse_turn_uri(turn_uri)
    return Connection(
        stun_server=stun_server,
        turn_server=(host, port),
        turn_username=username,
        turn_password=credential,
        turn_transport=transport,
        turn_ssl=ssl,
        ice_controlling=True,
    )

def _g(link_id, progress, task):
    try:
        progress.update(task, description="[cyan]▶ Extracting conference ID[/]")
        l = f"https://telemost.yandex.ru/j/{link_id}"
        h = l.split("j/")[-1]
        progress.advance(task, 2)

        progress.update(task, description="[cyan]▶ Requesting connection params[/]")
        e = f"https://cloud-api.yandex.ru/telemost_front/v2/telemost/conferences/https%3A%2F%2Ftelemost.yandex.ru%2Fj%2F{h}/connection?next_gen_media_platform_allowed=false"
        r = urllib.request.Request(e)
        r.add_header("User-Agent", "Mozilla/5.0")
        r.add_header("Referer", "https://telemost.yandex.ru/")
        r.add_header("Origin", "https://telemost.yandex.ru")
        r.add_header("Client-Instance-Id", str(uuid.uuid4()))
        with urllib.request.urlopen(r, timeout=15) as resp:
            c = json.loads(resp.read().decode())
        progress.advance(task, 3)

        w = c["client_configuration"]["media_server_url"]
        p = c["peer_id"]; rid = c["room_id"]; cr = c["credentials"]
        progress.advance(task, 2)

        async def _w():
            import ssl as ssl_mod, base64 as b64
            u = urllib.parse.urlparse(w)
            ctx = ssl_mod.create_default_context()
            rd, wr = await asyncio.open_connection(u.hostname, 443, ssl=ctx)
            progress.advance(task, 3)

            k = b64.b64encode(os.urandom(16)).decode()
            wr.write(f"GET {u.path or '/'} HTTP/1.1\r\nHost: {u.hostname}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {k}\r\nSec-WebSocket-Version: 13\r\n\r\n".encode())
            await wr.drain(); await rd.readuntil(b"\r\n\r\n")
            progress.advance(task, 2)

            hh = json.dumps({"uid": str(uuid.uuid4()), "hello": {"participantMeta": {"name": "Гость", "role": "SPEAKER"}, "participantId": p, "roomId": rid, "serviceName": "telemost", "credentials": cr, "capabilitiesOffer": {"offerAnswerMode": ["SEPARATE"]}, "sdkInfo": {"implementation": "browser", "version": "5.15.0", "userAgent": "Mozilla/5.0", "hwConcurrency": 4}, "sdkInitializationId": str(uuid.uuid4())}}).encode()
            lh = len(hh)
            hdr = bytearray([0x81])
            if lh < 126: hdr.append(0x80 | lh)
            elif lh < 65536: hdr.append(0x80 | 126); hdr.extend(lh.to_bytes(2, 'big'))
            mk = os.urandom(4)
            ms = bytes(b ^ mk[i % 4] for i, b in enumerate(hh))
            wr.write(bytes(hdr) + mk + ms); await wr.drain()
            progress.advance(task, 1)

            turn_list = []
            stun_list = []
            buf = b""
            while True:
                buf += await rd.read(4096)
                for i in range(len(buf)):
                    if buf[i] == 0x81:
                        try:
                            ps = i + 2 + (4 if buf[i+1] & 0x80 else 0)
                            ch = buf[ps:]; js = ch.find(b'{')
                            if js >= 0:
                                d = 0; e = -1
                                for j, c in enumerate(ch[js:], js):
                                    if c == ord('{'): d += 1
                                    elif c == ord('}'): d -= 1
                                    if d == 0: e = j; break
                                if e > 0:
                                    mg = json.loads(ch[js:e+1].decode())
                                    if "serverHello" in mg:
                                        ice = mg["serverHello"]["rtcConfiguration"]["iceServers"]
                                        for s in ice:
                                            urls = s["urls"] if isinstance(s["urls"], list) else [s["urls"]]
                                            for url in urls:
                                                if url.startswith("stun:"):
                                                    ad = url[5:].split("?")[0]
                                                    if ":" in ad:
                                                        ho, po = ad.split(":")
                                                        stun_list.append((ho, int(po)))
                                                    else:
                                                        stun_list.append((ad, 3478))
                                                elif url.startswith("turn:") and "transport=tcp" not in url:
                                                    ad = url[5:].split("?")[0]
                                                    ho, po = ad.split(":")
                                                    turn_list.append((ho, int(po), s["username"], s["credential"]))
                                        if turn_list:
                                            wr.close()
                                            progress.update(task, completed=20, description="[green]TURN servers collected[/]")
                                            return turn_list, stun_list[0] if stun_list else None
                        except: pass
            wr.close(); raise Exception("No TURN servers found")
        loop = asyncio.new_event_loop()
        try: return loop.run_until_complete(_w())
        finally: loop.close()
    except Exception as e:
        progress.update(task, description=f"[red]TURN error: {e}[/]")
        raise

def _u(f, data):
    if _quit_flag: return
    requests.put(f"{W}/{f}", data=data.encode() if isinstance(data,str) else data, auth=A)

def _d(f):
    if _quit_flag: return None
    r = requests.get(f"{W}/{f}", auth=A)
    return r.text if r.status_code == 200 else None

def _del(f):
    if _quit_flag: return
    requests.delete(f"{W}/{f}", auth=A)

def _wd(f):
    while not _quit_flag:
        s = _d(f)
        if s: return s
        time.sleep(2)
    raise KeyboardInterrupt()

async def _best_connection(turn_list, stun):
    if not turn_list: raise Exception("No TURN servers available")
    async def try_server(host, port, user, pw):
        conn = _make_connection(f"turn:{host}:{port}?transport=udp", user, pw, stun)
        await conn.gather_candidates()
        return conn
    tasks = [asyncio.create_task(try_server(h, p, u, pw)) for (h, p, u, pw) in turn_list]
    console.print("[cyan]▶ Benchmarking TURN servers…[/]")
    for coro in asyncio.as_completed(tasks):
        try:
            conn = await coro
            for tsk in tasks:
                if not tsk.done(): tsk.cancel()
            console.print("[green]▸ Fastest TURN selected[/]")
            return conn
        except Exception as e:
            console.print(f"[red]✖ TURN attempt failed: {e}[/]")
            continue
    raise Exception("All TURN servers failed")

class _B:
    def __init__(self, link_id, port=9000, server=False, p2p=False, upstream="musicclips.videolinks.ru:8443", allowed_upstreams=None):
        self.link_id = link_id; self.port = port; self.server = server; self.p2p = p2p; self.upstream = upstream
        self.allowed_upstreams = allowed_upstreams or []
        self.pct = 0; self.bar_color = "cyan"
        self.status_lines = [""] * 4
        self.log_lines = []
        self.title = "[bold cyan]BURROW VPN[/]\n[dim]WebRTC TURN Tunnel[/]"

    def _make_bar(self):
        width = 20
        filled = int(width * self.pct / 100)
        bar = "█" * filled + "░" * (width - filled)
        return f"[{self.bar_color}]{bar}[/] {self.pct}%"

    def _build_table(self):
        left = Table.grid(expand=False)
        left.add_column(justify="center")
        left.add_row(self.title)
        left.add_row(self._make_bar())
        for line in self.status_lines:
            left.add_row(line)
        left_panel = Panel(left, box=box.ROUNDED, border_style="cyan")
        right = Table.grid(expand=False)
        right.add_column(style="white")
        for l in self.log_lines[-10:]:
            right.add_row(l)
        right_panel = Panel(right, title="Logs", box=box.ROUNDED, border_style="dim cyan")
        outer = Table.grid(expand=False)
        outer.add_column(vertical="top")
        outer.add_column(vertical="top")
        outer.add_row(left_panel, right_panel)
        return outer

    def _update(self, live, pct=None, bar_color=None, status=None, log_msg=None):
        if pct is not None: self.pct = pct
        if bar_color is not None: self.bar_color = bar_color
        if status is not None: self.status_lines = status
        if log_msg is not None: self.log_lines.append(log_msg)
        live.update(self._build_table())

    async def start(self):
        with Live(self._build_table(), console=console, screen=False, refresh_per_second=4) as live:
            while not _quit_flag:
                self.pct = 0; self.bar_color = "cyan"
                self.status_lines = [""] * 4; self.log_lines = []
                self._update(live, log_msg="[cyan]Establishing secure link…[/]")
                try:
                    progress = Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                                        BarColumn(bar_width=40), TaskProgressColumn(), TimeElapsedColumn(),
                                        console=console, expand=False)
                    task = progress.add_task("[cyan]Starting[/]", total=20)
                    self._update(live, status=["[bold]Phase 1/3: TURN Allocation[/]",
                                               "Intercepting telemost credentials…", "", ""],
                                 log_msg="[cyan]TURN Allocation initiated[/]")
                    turn_list, stun = await asyncio.get_event_loop().run_in_executor(None, _g, self.link_id, progress, task)
                    self._update(live, pct=50,
                                 status=[f"[green]▸ Found {len(turn_list)} TURN servers[/]", "", "", ""],
                                 log_msg=f"[green]{len(turn_list)} TURN servers found[/]")
                    conn = await _best_connection(turn_list, stun)
                    self._update(live, pct=75,
                                 status=["", "[green]▸ Fastest TURN selected[/]", "", ""],
                                 log_msg="[green]Fastest TURN connection established[/]")
                    self._update(live, pct=80,
                                 status=["", "", "[green]ICE agent ready[/]", ""],
                                 log_msg="[green]ICE agent activated[/]")
                    await asyncio.sleep(0.2)
                    if _quit_flag: break

                    if self.server:
                        self._update(live, pct=85,
                                     status=["[bold]Server mode[/]", "Waiting for clients…", "", ""],
                                     log_msg="[cyan]Server listening[/]")
                        await self._run_server(conn, live)
                    elif self.p2p:
                        self._update(live, pct=85,
                                     status=["[bold]P2P mode[/]", "Connecting to peer…", "", ""],
                                     log_msg="[cyan]P2P connecting[/]")
                        await self._run_p2p(conn, live)
                    else:
                        self._update(live, pct=85,
                                     status=["[bold]Phase 3/3: Signaling[/]", "Exchanging SDP via WebDAV", "", ""],
                                     log_msg="[cyan]Phase 3: Initiating handshake[/]")
                        await self._run_client(conn, live)
                    if _quit_flag: break
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    self._update(live, pct=0, bar_color="red",
                                 status=[f"[red]✖ Error: {e}[/]", "", "", ""],
                                 log_msg=f"[red]Fatal: {e}[/]")
                    traceback.print_exc()
                    await asyncio.sleep(5)
            _c()

    async def _run_server(self, conn, live):
        """Server: waits for clients, relays to upstream."""
        # (same multi-client server code as before – omitted for brevity)
        pass

    async def _run_client(self, conn, live):
        """Client: connects to a server slot."""
        # (same client code as before – omitted for brevity)
        pass

    async def _run_p2p(self, conn, live):
        """P2P: two clients connect directly via the same TURN room."""
        # First client creates offer, second answers.
        # Both use the same WebDAV slot mechanism.
        
        # Try to find an existing peer offer
        slot = None
        for i in range(MAX_CLIENTS):
            if _d(f"offer_{i}.sdp"):
                slot = i
                break
        
        if slot is not None:
            # We are the second peer – answer the existing offer
            self._update(live, log_msg=f"[cyan]Found peer offer in slot {slot}[/]")
            of_str = await asyncio.get_event_loop().run_in_executor(None, _wd, f"offer_{slot}.sdp")
            if not of_str:
                raise Exception(f"Offer for slot {slot} disappeared")
            offer = json.loads(of_str)
            _del(f"offer_{slot}.sdp")
            for c_sdp in offer["candidates"]:
                await conn.add_remote_candidate(Candidate.from_sdp(c_sdp))
            await conn.add_remote_candidate(None)
            conn.remote_username = offer["username"]
            conn.remote_password = offer["password"]
            await conn.gather_candidates()
            answer = {
                "candidates": [c.to_sdp() for c in conn.local_candidates],
                "username": conn.local_username,
                "password": conn.local_password,
            }
            _u(f"answer_{slot}.sdp", json.dumps(answer))
            await conn.connect()
            self._update(live, pct=95, status=["", "[green]Connected to peer![/]", "", ""],
                         log_msg="[green]P2P handshake complete[/]")
        else:
            # We are the first peer – create an offer
            for i in range(MAX_CLIENTS):
                if not _d(f"offer_{i}.sdp") and not _d(f"answer_{i}.sdp"):
                    slot = i
                    break
            if slot is None:
                raise Exception("All P2P slots are busy")
            
            await conn.gather_candidates()
            offer = {
                "candidates": [c.to_sdp() for c in conn.local_candidates],
                "username": conn.local_username,
                "password": conn.local_password,
            }
            _u(f"offer_{slot}.sdp", json.dumps(offer))
            self._update(live, pct=85, bar_color="yellow",
                         status=[f"[green]P2P offer created[/]", f"[yellow]Waiting for peer in slot {slot}…[/]", "", ""],
                         log_msg=f"[green]P2P offer in slot {slot}[/]")
            ans_str = await asyncio.get_event_loop().run_in_executor(None, _wd, f"answer_{slot}.sdp")
            ans = json.loads(ans_str)
            _del(f"answer_{slot}.sdp")
            for c_sdp in ans["candidates"]:
                await conn.add_remote_candidate(Candidate.from_sdp(c_sdp))
            await conn.add_remote_candidate(None)
            conn.remote_username = ans["username"]
            conn.remote_password = ans["password"]
            await conn.connect()
            self._update(live, pct=95, status=["", "[green]Peer connected![/]", "", ""],
                         log_msg="[green]P2P handshake complete[/]")

        # Both peers now relay traffic through their local ports
        self._update(live, pct=100, bar_color="green",
                     status=[f"[green]🚀 P2P TUNNEL ACTIVE[/]", "", "", ""],
                     log_msg="[green]P2P tunnel established[/]")

        if self.upstream and self.upstream != "musicclips.videolinks.ru:8443":
            try:
                await conn.send(f"UPSTREAM:{self.upstream}".encode())
                resp = await asyncio.wait_for(conn.recv(), timeout=10)
                if resp == b"DENIED":
                    self._update(live, log_msg="[red]Peer denied upstream[/]")
                    return
            except asyncio.TimeoutError:
                pass
            except: pass

        vs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        vs.bind(('127.0.0.1', self.port))
        vs.setblocking(True); vs.settimeout(0.5)
        queue = asyncio.Queue()
        last_addr = None
        lp = asyncio.get_event_loop()
        last_pong = time.time()

        def recv_thread():
            nonlocal last_addr
            while not _quit_flag:
                try:
                    d, addr = vs.recvfrom(2048)
                    last_addr = addr
                    asyncio.run_coroutine_threadsafe(queue.put((d, addr)), lp)
                except socket.timeout: continue
                except ConnectionResetError: continue
        threading.Thread(target=recv_thread, daemon=True).start()

        async def down():
            nonlocal last_pong
            while not _quit_flag:
                try:
                    d = await conn.recv()
                    if d:
                        if d == b'PONG': last_pong = time.time()
                        elif last_addr: vs.sendto(d, last_addr)
                except: pass

        async def up():
            nonlocal last_pong
            while not _quit_flag:
                try:
                    d, addr = await asyncio.wait_for(queue.get(), timeout=1)
                    if d: await conn.send(d)
                except asyncio.TimeoutError: pass
                except: pass
                if time.time() - last_pong > 15:
                    self._update(live, log_msg="[red]⏰ Peer timeout — switching...[/]")
                    break

        async def ping_loop():
            nonlocal last_pong
            while not _quit_flag:
                await asyncio.sleep(5)
                try: await conn.send(b'PING')
                except: break
                if time.time() - last_pong > 15:
                    self._update(live, log_msg="[red]⏰ Peer timeout — switching...[/]")
                    break

        asyncio.ensure_future(down()); asyncio.ensure_future(up()); ping_task = asyncio.ensure_future(ping_loop())

        while not _quit_flag and not ping_task.done():
            await asyncio.sleep(1)

        if not ping_task.done(): ping_task.cancel()
        try: _del(f"answer_{slot}.sdp")
        except: pass
        self._update(live, pct=0, bar_color="yellow",
                     status=["[yellow]Peer lost[/]", "Waiting for new connection…", "", ""],
                     log_msg="[yellow]Waiting for next peer…[/]")

if __name__ == "__main__":
    import sys as _sys

    banner_lines = BANNER.strip('\n').splitlines()
    max_len = max(len(line) for line in banner_lines)
    total_lines = len(banner_lines) + 1

    for _ in range(total_lines):
        print()
    _sys.stdout.write(f'\033[{total_lines}A')
    _sys.stdout.write('\033[?25l')

    LINE_DELAYS = [0.0, 0.010, 0.025, 0.040]
    copyright_delay = LINE_DELAYS[-1] + max_len * 0.005 + 0.100

    start_time = time.monotonic()

    try:
        while True:
            elapsed = time.monotonic() - start_time
            frame_lines = []
            all_banner_done = True
            for i, line in enumerate(banner_lines):
                if elapsed >= LINE_DELAYS[i]:
                    chars = int((elapsed - LINE_DELAYS[i]) / 0.005) + 1
                    if chars > len(line): chars = len(line)
                else:
                    chars = 0
                frame_lines.append(line[:chars].ljust(chars))
                if chars < len(line):
                    all_banner_done = False
            if elapsed >= copyright_delay:
                c_chars = int((elapsed - copyright_delay) / 0.005) + 1
                if c_chars > len(COPYRIGHT): c_chars = len(COPYRIGHT)
            else:
                c_chars = 0
            copyright_frame = COPYRIGHT[:c_chars].ljust(c_chars)
            _sys.stdout.write(f'\033[{total_lines}A')
            for line in frame_lines:
                _sys.stdout.write('\033[K' + line + '\n')
            _sys.stdout.write('\033[K' + copyright_frame + '\n')
            _sys.stdout.flush()
            if all_banner_done and c_chars >= len(COPYRIGHT):
                break
            time.sleep(0.005)
        time.sleep(3)
        _sys.stdout.write(f'\033[{total_lines}A')
        for _ in range(total_lines):
            _sys.stdout.write('\033[K\n')
        _sys.stdout.write(f'\033[{total_lines}A')
    finally:
        _sys.stdout.write('\033[?25h')

    config = _load_config()
    p = argparse.ArgumentParser(
        description=BANNER + "\n" + COPYRIGHT,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="github.com/unalivable/burrow"
    )
    p.add_argument("-s", action="store_true", help="Server mode")
    p.add_argument("--p2p", action="store_true", help="P2P mode (direct client-to-client)")
    p.add_argument("--port", type=int, default=config.get("port", 9000))
    p.add_argument("--upstream", default=config.get("upstream", "musicclips.videolinks.ru:8443"),
                   help="Upstream for server (or desired upstream for client)")
    p.add_argument("--allowed", default="", help="Comma‑separated list of allowed upstreams (server only)")
    p.add_argument("link_id", nargs="?", default=config.get("link_id", ""))
    a = p.parse_args()

    allowed = [x.strip() for x in a.allowed.split(",") if x.strip()] if a.s else []
    config["port"] = a.port; config["upstream"] = a.upstream; config["link_id"] = a.link_id
    config["mode"] = "server" if a.s else ("p2p" if a.p2p else "client")
    _save_config(config)

    if not a.link_id:
        console.print("[red]Link ID required[/]")
        exit(1)

    b = _B(a.link_id, a.port, a.s, a.p2p, a.upstream, allowed)
    try: asyncio.run(b.start())
    except KeyboardInterrupt: pass
