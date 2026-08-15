#!/usr/bin/env python3
"""Switchable loopback PostgreSQL proxy for the restore qualification drill.

The gateway keeps one DSN for the lifetime of a replica.  A restore therefore
has to be exercised by changing what that DSN reaches, not by restarting the
replica with a different config.  This proxy forwards the PostgreSQL startup
packet, rewrites only its database name, and switches its upstream on signals:

* SIGUSR1: the logical-restore database on the live PostgreSQL listener;
* SIGHUP: the live database again;
* SIGUSR2: the promoted PITR cluster on the recovered listener.

Every switch closes active sockets so the replica observes a real connection
loss and must reconnect.  No credentials or packet bodies are logged.
"""

from __future__ import annotations

import argparse
import signal
import socket
import struct
import threading
from dataclasses import dataclass


@dataclass(frozen=True)
class Target:
    name: str
    port: int
    database: str


class Proxy:
    def __init__(self, listen_port: int, targets: list[Target]) -> None:
        self.targets = targets
        self.mode = 0
        self.lock = threading.Lock()
        self.sockets: set[socket.socket] = set()
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", listen_port))
        self.listener.listen(32)
        self.listener.settimeout(0.5)
        self.stopping = threading.Event()

    def target(self) -> Target:
        with self.lock:
            return self.targets[self.mode]

    def switch(self, mode: int) -> None:
        with self.lock:
            if mode >= len(self.targets):
                return
            self.mode = mode
            sockets = list(self.sockets)
            target = self.targets[mode]
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass
        print(f"switched to {target.name}; closed {len(sockets)} active connections", flush=True)

    def close(self) -> None:
        self.stopping.set()
        try:
            self.listener.close()
        except OSError:
            pass
        self.switch(self.mode)

    def serve(self) -> None:
        while not self.stopping.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            thread = threading.Thread(target=self.connection, args=(client,), daemon=True)
            thread.start()

    def connection(self, client: socket.socket) -> None:
        upstream: socket.socket | None = None
        with self.lock:
            self.sockets.add(client)
            target = self.targets[self.mode]
        try:
            upstream = socket.create_connection(("127.0.0.1", target.port), timeout=10)
            upstream.settimeout(None)
            with self.lock:
                self.sockets.add(upstream)
            client.settimeout(None)
            startup = self.startup_packet(client, upstream)
            if startup is None:
                return
            upstream.sendall(rewrite_database(startup, target.database))
            self.copy_bidirectionally(client, upstream)
        except (OSError, ValueError):
            pass
        finally:
            with self.lock:
                self.sockets.discard(client)
                if upstream is not None:
                    self.sockets.discard(upstream)
            for sock in (client, upstream):
                if sock is None:
                    continue
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass

    @staticmethod
    def startup_packet(client: socket.socket, upstream: socket.socket) -> bytes | None:
        """Forward SSL negotiation, then return the plaintext startup packet."""
        while True:
            packet = read_packet(client)
            if packet is None:
                return None
            if len(packet) == 8 and struct.unpack("!I", packet[4:])[0] == 80877103:
                upstream.sendall(packet)
                response = recv_exact(upstream, 1)
                if response is None:
                    return None
                client.sendall(response)
                if response != b"N":
                    # The drill uses plaintext loopback DSNs. Refuse a TLS
                    # negotiation rather than forwarding an opaque channel.
                    return None
                continue
            return packet

    @staticmethod
    def copy_bidirectionally(left: socket.socket, right: socket.socket) -> None:
        finished = threading.Event()

        def pump(source: socket.socket, destination: socket.socket) -> None:
            try:
                while not finished.is_set():
                    data = source.recv(64 * 1024)
                    if not data:
                        break
                    destination.sendall(data)
            except OSError:
                pass
            finally:
                finished.set()
                try:
                    destination.shutdown(socket.SHUT_WR)
                except OSError:
                    pass

        threads = [
            threading.Thread(target=pump, args=(left, right), daemon=True),
            threading.Thread(target=pump, args=(right, left), daemon=True),
        ]
        for thread in threads:
            thread.start()
        finished.wait()


def recv_exact(sock: socket.socket, size: int) -> bytes | None:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_packet(sock: socket.socket) -> bytes | None:
    header = recv_exact(sock, 4)
    if header is None:
        return None
    length = struct.unpack("!I", header)[0]
    if length < 4 or length > 10 * 1024 * 1024:
        raise ValueError("invalid PostgreSQL startup packet length")
    body = recv_exact(sock, length - 4)
    return None if body is None else header + body


def rewrite_database(packet: bytes, database: str) -> bytes:
    if len(packet) < 12:
        raise ValueError("short PostgreSQL startup packet")
    protocol = packet[4:8]
    if protocol != b"\x00\x03\x00\x00":
        raise ValueError("unexpected PostgreSQL startup protocol")
    fields = packet[8:].split(b"\x00")
    rewritten: list[bytes] = []
    index = 0
    while index + 1 < len(fields) and fields[index]:
        key, value = fields[index], fields[index + 1]
        rewritten.extend((key, database.encode() if key == b"database" else value))
        index += 2
    payload = b"\x00".join(rewritten) + b"\x00\x00"
    length = 4 + len(protocol) + len(payload)
    return struct.pack("!I", length) + protocol + payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--live-port", type=int, required=True)
    parser.add_argument("--recovered-port", type=int, required=True)
    args = parser.parse_args()
    proxy = Proxy(
        args.listen_port,
        [
            Target("live", args.live_port, "live"),
            Target("logical-restore", args.live_port, "logical_restore"),
            Target("live", args.live_port, "live"),
            Target("point-in-time-recovery", args.recovered_port, "live"),
        ],
    )
    signal.signal(signal.SIGUSR1, lambda *_: proxy.switch(1))
    signal.signal(signal.SIGHUP, lambda *_: proxy.switch(2))
    signal.signal(signal.SIGUSR2, lambda *_: proxy.switch(3))
    signal.signal(signal.SIGTERM, lambda *_: proxy.close())
    print(f"listening on 127.0.0.1:{args.listen_port} in live mode", flush=True)
    try:
        proxy.serve()
    finally:
        proxy.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
