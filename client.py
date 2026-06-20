import os
import json
import socket
import struct
import hashlib

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 9000
DOWNLOAD_DIR = "downloads"
CHUNK_SIZE = 64 * 1024


def recv_exact(sock: socket.socket, n: int) -> bytes:
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Socket closed")
        data += chunk
    return data


def send_json(sock: socket.socket, obj: dict):
    payload = json.dumps(obj).encode("utf-8")
    sock.sendall(struct.pack("!I", len(payload)))
    sock.sendall(payload)


def recv_json(sock: socket.socket) -> dict:
    (length,) = struct.unpack("!I", recv_exact(sock, 4))
    payload = recv_exact(sock, length)
    return json.loads(payload.decode("utf-8"))


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def cmd_list(sock):
    try:
        send_json(sock, {"type": "REQUEST_LIST"})
        resp = recv_json(sock)

        if resp.get("type") != "LIST_RESPONSE":
            print("Unexpected:", resp)
            return

        files = resp.get("files", [])
        if not files:
            print("(empty)")
            return

        for i, it in enumerate(files):
            print(f"{i:2d}. {it['name']}  size={it['size']}  date={it['date']}")

    except Exception as e:
        print("List error:", e)


def cmd_upload(sock, path):
    try:
        path = path.strip()
        if not path:
            print("Usage: upload <path>")
            return

        if not os.path.isfile(path):
            print("File not found.")
            return

        name = os.path.basename(path)
        size = os.path.getsize(path)
        total_chunks = (size + CHUNK_SIZE - 1) // CHUNK_SIZE

        send_json(sock, {
            "type": "REQUEST_UPLOAD",
            "name": name,
            "size": size,
            "total_chunks": total_chunks
        })

        with open(path, "rb") as f:
            for idx in range(total_chunks):
                data = f.read(CHUNK_SIZE)
                header = struct.pack("!III", idx, total_chunks, len(data))
                sock.sendall(header)
                sock.sendall(data)

        resp = recv_json(sock)
        print(resp)

    except Exception as e:
        print("Upload error:", e)


def cmd_download(sock, name):
    try:
        name = name.strip()
        if not name:
            print("Usage: download <name>")
            return

        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        out_path = os.path.join(DOWNLOAD_DIR, os.path.basename(name))

        send_json(sock, {"type": "DOWNLOAD_REQUEST", "name": name})

        info = recv_json(sock)
        if info.get("type") == "ERROR":
            print(info)
            return

        if info.get("type") != "DOWNLOAD_INFO":
            print("Unexpected:", info)
            return

        total_chunks = int(info["total_chunks"])
        size = int(info["size"])
        print(f"Downloading {name} size={size} total_chunks={total_chunks}")

        with open(out_path, "wb") as f:
            for expected in range(total_chunks):
                header = recv_exact(sock, 12)
                chunk_index, total, chunk_size = struct.unpack("!III", header)

                if total != total_chunks or chunk_index != expected:
                    raise RuntimeError(
                        f"Chunk order mismatch. got idx={chunk_index} expected={expected}"
                    )

                data = recv_exact(sock, chunk_size)
                f.write(data)

        done = recv_json(sock)
        print(done)
        print("Saved to:", out_path)

    except Exception as e:
        print("Download error:", e)


def main():
    try:
        host = input(f"Server host [{SERVER_HOST}]: ").strip() or SERVER_HOST
        port_in = input(f"Server port [{SERVER_PORT}]: ").strip()
        port = int(port_in) if port_in else SERVER_PORT

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            print("Connected.")

            while True:
                print("\nCommands: list | upload <path> | download <name> | sha256 <path> | quit")
                cmd_line = input("> ").strip()

                if not cmd_line:
                    continue

                parts = cmd_line.split(" ", 1)
                command = parts[0].lower()
                args = parts[1].strip() if len(parts) > 1 else ""

                if command == "list":
                    cmd_list(sock)

                elif command == "upload":
                    cmd_upload(sock, args)

                elif command == "download":
                    cmd_download(sock, args)

                elif command == "sha256":
                    if not args:
                        print("Usage: sha256 <path>")
                    else:
                        try:
                            print(file_sha256(args))
                        except Exception as e:
                            print("SHA256 error:", e)

                elif command == "quit":
                    send_json(sock, {"type": "QUIT"})
                    print("Bye.")
                    break

                else:
                    print("Unknown command.")

    except ConnectionRefusedError:
        print(f"Cannot connect to {host}:{port} - connection refused.")
    except Exception as e:
        print("Client error:", e)


if __name__ == "__main__":
    main()