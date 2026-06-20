#!/usr/bin/env python3
import os
import json
import socket
import struct
import signal
import fcntl
from datetime import datetime

HOST = "0.0.0.0"
PORT = 9000
SHARED_DIR = "shared"
CHUNK_SIZE = 64 * 1024  # 64KB

# ---------- helpers ----------
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

def safe_name(filename: str) -> str:
    return os.path.basename(filename)

def safe_join(base: str, filename: str) -> str:
    return os.path.join(base, safe_name(filename))

def list_files():
    os.makedirs(SHARED_DIR, exist_ok=True)
    items = []
    for name in sorted(os.listdir(SHARED_DIR)):
        if name.endswith(".lock") or name.startswith(".tmp_"):
            continue
        path = os.path.join(SHARED_DIR, name)
        if os.path.isfile(path):
            st = os.stat(path)
            items.append({
                "name": name,
                "size": st.st_size,
                "date": datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds")
            })
    return items

def lock_for(filename: str):
    """Return opened fd for lock file. Caller must close."""
    os.makedirs(SHARED_DIR, exist_ok=True)
    lock_path = safe_join(SHARED_DIR, filename + ".lock")
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    return fd

# ---------- operations ----------
def handle_request_list(conn):
    send_json(conn, {"type": "LIST_RESPONSE", "files": list_files()})

def handle_upload(conn, msg):
    name = msg.get("name")
    total_chunks = int(msg.get("total_chunks", 0))
    if not name or total_chunks <= 0:
        send_json(conn, {"type": "ERROR", "code": 400, "message": "Invalid upload metadata"})
        return

    os.makedirs(SHARED_DIR, exist_ok=True)
    name = safe_name(name)
    final_path = safe_join(SHARED_DIR, name)

    # فایل موقت مخصوص این پردازه
    tmp_path = safe_join(SHARED_DIR, f".tmp_{os.getpid()}_{name}")

    received = 0
    try:
        with open(tmp_path, "wb") as f:
            for _ in range(total_chunks):
                header = recv_exact(conn, 12)
                chunk_index, total, chunk_size = struct.unpack("!III", header)
                if total != total_chunks:
                    send_json(conn, {"type": "ERROR", "code": 409, "message": "total_chunks mismatch"})
                    return

                data = recv_exact(conn, chunk_size)
                if chunk_index != received:
                    send_json(conn, {"type": "ERROR", "code": 409, "message": f"Unexpected chunk_index {chunk_index}, expected {received}"})
                    return

                f.write(data)
                received += 1

        # قفل روی نام فایل مقصد
        lock_fd = lock_for(name)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)

            # سیاست: اگر فایل وجود دارد، Conflict
            if os.path.exists(final_path):
                os.remove(tmp_path)
                send_json(conn, {"type": "ERROR", "code": 409, "message": "File already exists"})
                return

            # replace اتمیک
            os.replace(tmp_path, final_path)

        finally:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)

        send_json(conn, {"type": "OK", "message": f"Upload completed: {name}"})

    except Exception as e:
        # در صورت خطا فایل موقت را پاک کن
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except:
            pass
        raise e

def handle_download(conn, msg):
    name = msg.get("name")
    if not name:
        send_json(conn, {"type": "ERROR", "code": 400, "message": "Missing file name"})
        return

    name = safe_name(name)
    path = safe_join(SHARED_DIR, name)
    if not os.path.isfile(path):
        send_json(conn, {"type": "ERROR", "code": 404, "message": "File not found"})
        return

    size = os.path.getsize(path)
    total_chunks = (size + CHUNK_SIZE - 1) // CHUNK_SIZE

    send_json(conn, {
        "type": "DOWNLOAD_INFO",
        "name": name,
        "size": size,
        "total_chunks": total_chunks,
        "chunk_size": CHUNK_SIZE
    })

    with open(path, "rb") as f:
        for idx in range(total_chunks):
            data = f.read(CHUNK_SIZE)
            header = struct.pack("!III", idx, total_chunks, len(data))
            conn.sendall(header)
            conn.sendall(data)

    send_json(conn, {"type": "DONE_TRANSFER", "name": name})

# ---------- per-client handler ----------
def serve_client(conn, addr):
    print(f"[child {os.getpid()}] serving {addr}")
    try:
        while True:
            msg = recv_json(conn)
            mtype = msg.get("type")

            if mtype == "REQUEST_LIST":
                handle_request_list(conn)

            elif mtype == "REQUEST_UPLOAD":
                handle_upload(conn, msg)

            elif mtype == "DOWNLOAD_REQUEST":
                handle_download(conn, msg)

            elif mtype == "QUIT":
                break

            else:
                send_json(conn, {"type": "ERROR", "code": 400, "message": f"Unknown type: {mtype}"})

    except (ConnectionError, OSError) as e:
        print(f"[child {os.getpid()}] connection ended: {e}")
    finally:
        try:
            conn.close()
        except:
            pass
        print(f"[child {os.getpid()}] closed {addr}")

def main():
    os.makedirs(SHARED_DIR, exist_ok=True)

    # جلوگیری از zombie: childها را سیستم reap می‌کند
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen(50)
        print(f"[parent {os.getpid()}] listening on {HOST}:{PORT} (multi-process)")

        while True:
            conn, addr = s.accept()

            pid = os.fork()
            if pid == 0:
                # child
                try:
                    s.close()  # child به listening socket نیاز ندارد
                    serve_client(conn, addr)
                finally:
                    os._exit(0)
            else:
                # parent
                conn.close()  # parent این conn را استفاده نمی‌کند

if __name__ == "__main__":
    main()
