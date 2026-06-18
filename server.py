import os
import json
import time
import socket
import struct
from datetime import datetime

HOST = "0.0.0.0"
PORT = 9000
SHARED_DIR = "shared"
CHUNK_SIZE = 64 * 1024  # 64KB

# ---------- helpers ----------

# دریافت دقیقا n بایت
def recv_exact(sock: socket.socket, n: int) -> bytes:
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Socket closed")
        data += chunk
    return data

# اول طول پیام بعد خود پیام ارسال میشه
# گیرنده میفهمه چند بایت باید بخونه
def send_json(sock: socket.socket, obj: dict):
    payload = json.dumps(obj).encode("utf-8")
    sock.sendall(struct.pack("!I", len(payload)))
    sock.sendall(payload)

# خواندن طول پیام
# خواندن داده
# تبدیل JSON به Dictionary
def recv_json(sock: socket.socket) -> dict:
    (length,) = struct.unpack("!I", recv_exact(sock, 4))
    payload = recv_exact(sock, length)
    return json.loads(payload.decode("utf-8"))

# اسکن پوشه
# ساخت لیست
# خروجی JSON میدهد
def list_files():
    os.makedirs(SHARED_DIR, exist_ok=True)
    items = []
    for name in sorted(os.listdir(SHARED_DIR)):
        path = os.path.join(SHARED_DIR, name)
        if os.path.isfile(path):
            st = os.stat(path)
            items.append({
                "name": name,
                "size": st.st_size,
                "date": datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds")
            })
    return items

# برای امنیت
# جلوگیری از ../../etc/passwd
# فقط نام فایل رو نگه میدارد
def safe_join(base, filename):
    filename = os.path.basename(filename)
    return os.path.join(base, filename)

# ---------- operations ----------
def handle_request_list(conn):
    files = list_files()
    send_json(conn, {"type": "LIST_RESPONSE", "files": files})

def handle_upload(conn, msg):
    # msg: {type, name, size, total_chunks}
    name = msg.get("name")
    total_chunks = int(msg.get("total_chunks", 0))
    if not name or total_chunks <= 0:
        send_json(conn, {"type": "ERROR", "code": 400, "message": "Invalid upload metadata"})
        return

    os.makedirs(SHARED_DIR, exist_ok=True)
    out_path = safe_join(SHARED_DIR, name)

    # اگر فایل وجود دارد، overwrite می‌کنیم
    received = 0
    with open(out_path, "wb") as f:
        for _ in range(total_chunks):
            header = recv_exact(conn, 12)
            chunk_index, total, chunk_size = struct.unpack("!III", header)

            if total != total_chunks:
                send_json(conn, {"type": "ERROR", "code": 409, "message": "total_chunks mismatch"})
                return

            data = recv_exact(conn, chunk_size)
            # چون سرور ترتیبی است، انتظار داریم chunk_index به ترتیب بیاید
            if chunk_index != received:
                send_json(conn, {"type": "ERROR", "code": 409, "message": f"Unexpected chunk_index {chunk_index}, expected {received}"})
                return

            f.write(data)
            received += 1

    # پایان آپلود
    send_json(conn, {"type": "OK", "message": f"Upload completed: {name}"})

def handle_download(conn, msg):
    # msg: {type, name}
    name = msg.get("name")
    if not name:
        send_json(conn, {"type": "ERROR", "code": 400, "message": "Missing file name"})
        return

    path = safe_join(SHARED_DIR, name)
    if not os.path.isfile(path):
        send_json(conn, {"type": "ERROR", "code": 404, "message": "File not found"})
        return

    size = os.path.getsize(path)
    total_chunks = (size + CHUNK_SIZE - 1) // CHUNK_SIZE

    # ابتدا یک پیام JSON برای شروع دانلود
    send_json(conn, {
        "type": "DOWNLOAD_INFO",
        "name": name,
        "size": size,
        "total_chunks": total_chunks,
        "chunk_size": CHUNK_SIZE
    })

    # سپس chunkها را باینری ارسال می‌کنیم
    with open(path, "rb") as f:
        for idx in range(total_chunks):
            data = f.read(CHUNK_SIZE)
            header = struct.pack("!III", idx, total_chunks, len(data))
            conn.sendall(header)
            conn.sendall(data)

    # پیام پایان
    send_json(conn, {"type": "DONE_TRANSFER", "name": name})

# ---------- main server loop (single-thread, sequential) ----------
def main():
    os.makedirs(SHARED_DIR, exist_ok=True)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen(5)
        print(f"[server] listening on {HOST}:{PORT} (single-thread sequential)")
        print(f"[server] shared dir: {os.path.abspath(SHARED_DIR)}")

        while True:
            conn, addr = s.accept()
            print(f"\n[server] client connected: {addr}")
            # نکته: چون تک‌ریسه‌ای هستیم، تا پایان این بلوک، کلاینت بعدی در accept منتظر می‌ماند
            try:
                while True:
                    msg = recv_json(conn)
                    mtype = msg.get("type")

                    if mtype == "REQUEST_LIST":
                        print("[server] REQUEST_LIST")
                        handle_request_list(conn)

                    elif mtype == "REQUEST_UPLOAD":
                        print(f"[server] REQUEST_UPLOAD name={msg.get('name')} total_chunks={msg.get('total_chunks')}")
                        handle_upload(conn, msg)

                    elif mtype == "DOWNLOAD_REQUEST":
                        print(f"[server] DOWNLOAD_REQUEST name={msg.get('name')}")
                        # برای نمایش ترتیبی بودن، می‌تونی این خط را فعال کنی تا عمدی کند شود:
                        # time.sleep(5)
                        handle_download(conn, msg)

                    elif mtype == "QUIT":
                        print("[server] QUIT")
                        break

                    else:
                        send_json(conn, {"type": "ERROR", "code": 400, "message": f"Unknown type: {mtype}"})

            except (ConnectionError, OSError) as e:
                print(f"[server] connection ended: {e}")
            finally:
                try:
                    conn.close()
                except:
                    pass
                print(f"[server] client disconnected: {addr}")

if __name__ == "__main__":
    main()