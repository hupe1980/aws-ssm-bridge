"""Connect to a forwarded local port and read whatever the far end greets with.

Used by live-smoke.sh: a forwarded SSH port answers with its banner, which is
proof that bytes crossed the tunnel in both directions.
"""
import socket
import sys

host, port = sys.argv[1].split(":")
sock = socket.create_connection((host, int(port)), timeout=20)
sock.settimeout(20)
banner = sock.recv(128)
sock.close()
print(f"  banner: {banner[:40]!r}")
sys.exit(0 if banner else 1)
