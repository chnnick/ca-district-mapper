"""Sidecar entrypoint used by the Tauri shell.

Binds an ephemeral port on the loopback interface, prints a JSON line
``{"port": N}`` to stdout so the host process can discover where to talk
to us, then hands the socket off to uvicorn. Keeping the bind in Python
(instead of letting uvicorn pick) avoids a race where uvicorn starts
listening before we've reported the port.
"""

import asyncio
import json
import socket
import sys

import uvicorn

from src.api.app import create_app


def main() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sys.stdout.write(json.dumps({"port": port}) + "\n")
    sys.stdout.flush()

    config = uvicorn.Config(create_app(), log_config=None, access_log=False)
    server = uvicorn.Server(config)
    asyncio.run(server.serve(sockets=[sock]))


if __name__ == "__main__":
    main()
