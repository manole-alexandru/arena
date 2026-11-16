#!/usr/bin/env python3
"""
Tiny zero-dependency server for the ARENA online game (Python 3.8+).

  python server.py            -> http://localhost:8080
  python server.py 9000       -> http://localhost:9000
  ngrok http 8080             -> share the https URL with your friends

It serves the game files and relays WebSocket messages between the HOST (the one
browser that runs the simulation) and the other players. It runs no game logic.

  /        online.html   (the online version)
  /solo    index.html    (the original single-player version, untouched)
"""
import asyncio, base64, hashlib, json, os, struct, sys

HERE = os.path.dirname(os.path.abspath(__file__))
PAGES = {'/': 'online.html', '/online': 'online.html', '/online.html': 'online.html',
         '/solo': 'index.html', '/index.html': 'index.html'}
GUID = b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11'

clients = {}        # id -> Client
host_id = None
next_id = 1


class Client:
    def __init__(self, writer, cid):
        self.writer = writer
        self.id = cid
        self.alive = True

    def send(self, text):
        if not self.alive:
            return
        try:   # a slow client: skip stale world snapshots instead of queueing them (that is what causes growing lag)
            if text.startswith('{"t":"snap"') and self.writer.transport.get_write_buffer_size() > 40000:
                return
        except Exception:
            pass
        data = text.encode('utf-8')
        n = len(data)
        if n < 126:
            head = struct.pack('!BB', 0x81, n)
        elif n < 65536:
            head = struct.pack('!BBH', 0x81, 126, n)
        else:
            head = struct.pack('!BBQ', 0x81, 127, n)
        try:
            self.writer.write(head + data)
        except Exception:
            self.alive = False

    def pong(self, payload):
        try:
            self.writer.write(struct.pack('!BB', 0x8A, len(payload)) + payload)
        except Exception:
            self.alive = False


async def read_frame(reader):
    """Returns (opcode, payload) of a complete message (handles fragmentation)."""
    msg = b''
    first_op = None
    while True:
        h = await reader.readexactly(2)
        fin = h[0] & 0x80
        op = h[0] & 0x0F
        masked = h[1] & 0x80
        ln = h[1] & 0x7F
        if ln == 126:
            ln = struct.unpack('!H', await reader.readexactly(2))[0]
        elif ln == 127:
            ln = struct.unpack('!Q', await reader.readexactly(8))[0]
        mask = await reader.readexactly(4) if masked else b''
        data = await reader.readexactly(ln) if ln else b''
        if masked:
            data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        if op >= 8:                      # control frame
            return op, data
        if first_op is None:
            first_op = op
        msg += data
        if fin:
            return first_op, msg


def broadcast(text, skip=None):
    for c in list(clients.values()):
        if c.id != host_id and c.id != skip:
            c.send(text)


async def ws_session(reader, writer):
    global host_id, next_id
    cid = next_id
    next_id += 1
    me = Client(writer, cid)
    clients[cid] = me
    me.send(json.dumps({'t': 'hello', 'id': cid, 'hostPresent': host_id is not None}))
    try:
        while True:
            op, data = await read_frame(reader)
            if op == 8:
                break
            if op == 9:
                me.pong(data)
                continue
            if op != 1:
                continue
            try:
                msg = json.loads(data.decode('utf-8'))
            except Exception:
                continue
            t = msg.get('t')
            if t == 'host':
                if host_id is None or host_id not in clients:
                    host_id = cid
                    me.send(json.dumps({'t': 'hosted', 'ok': True, 'id': cid}))
                else:
                    me.send(json.dumps({'t': 'hosted', 'ok': False}))
                continue
            if cid == host_id:
                to = msg.pop('to', None)
                text = json.dumps(msg, separators=(',', ':'))
                if to is None:
                    broadcast(text)
                elif to in clients:
                    clients[to].send(text)
            else:
                msg['from'] = cid
                h = clients.get(host_id)
                if h:
                    h.send(json.dumps(msg, separators=(',', ':')))
                elif t == 'join':
                    me.send(json.dumps({'t': 'nohost'}))
    except (asyncio.IncompleteReadError, ConnectionError, OSError):
        pass
    finally:
        me.alive = False
        clients.pop(cid, None)
        if cid == host_id:
            host_id = None
            broadcast(json.dumps({'t': 'hostgone'}))
        else:
            h = clients.get(host_id)
            if h:
                h.send(json.dumps({'t': 'peerleft', 'from': cid}))
        try:
            writer.close()
        except Exception:
            pass


async def handle(reader, writer):
    try:
        head = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), 15)
    except Exception:
        writer.close()
        return
    lines = head.decode('latin-1').split('\r\n')
    try:
        method, path, _ = lines[0].split(' ', 2)
    except ValueError:
        writer.close()
        return
    hdr = {}
    for ln in lines[1:]:
        if ':' in ln:
            k, v = ln.split(':', 1)
            hdr[k.strip().lower()] = v.strip()
    path = path.split('?')[0]
    if path == '/ws' and 'websocket' in hdr.get('upgrade', '').lower():
        key = hdr.get('sec-websocket-key', '').encode()
        acc = base64.b64encode(hashlib.sha1(key + GUID).digest()).decode()
        writer.write(('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n'
                      'Connection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n' % acc).encode())
        await writer.drain()
        await ws_session(reader, writer)
        return
    name = PAGES.get(path)
    if name and os.path.exists(os.path.join(HERE, name)):
        with open(os.path.join(HERE, name), 'rb') as f:
            body = f.read()
        writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n'
                     b'Cache-Control: no-store\r\nContent-Length: ' + str(len(body)).encode() + b'\r\n\r\n' + body)
    else:
        body = b'Not found'
        writer.write(b'HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\n' + body)
    try:
        await writer.drain()
    finally:
        writer.close()


async def main(port):
    server = await asyncio.start_server(handle, '0.0.0.0', port)
    print('ARENA server running:')
    print('  host (you) open:   http://localhost:%d/' % port)
    print('  single-player:     http://localhost:%d/solo' % port)
    print('  share with friends: run   ngrok http %d   and send them the https URL' % port)
    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    try:
        asyncio.run(main(port))
    except KeyboardInterrupt:
        pass
