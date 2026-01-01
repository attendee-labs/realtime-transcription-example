"""
Flask demo that
  • POST /launch  – creates a bot in Attendee and returns its id
  • POST /webhook – receives real‑time transcript.update webhooks
  • GET  /stream  – server‑sent‑events (SSE) feed the browser listens to
Environment variables
  ATTENDEE_API_KEY   – your Attendee API key
  WEBHOOK_SECRET     – your base‑64 secret from Attendee's dashboard
  ATTENDEE_API_BASE  – default https://app.attendee.dev
Run with:  python app.py
"""
import os, json, hmac, hashlib, base64, queue, threading, time
from typing import Dict, List

import requests
from flask import Flask, Response, request, stream_with_context, jsonify, abort, send_from_directory

ATTENDEE_API_KEY  = os.getenv("ATTENDEE_API_KEY")
WEBHOOK_SECRET    = os.getenv("WEBHOOK_SECRET")
ATTENDEE_API_BASE = os.getenv("ATTENDEE_API_BASE", "https://app.attendee.dev")

if not ATTENDEE_API_KEY or not WEBHOOK_SECRET:
    raise RuntimeError("Set ATTENDEE_API_KEY and WEBHOOK_SECRET env vars first")

app = Flask(
    __name__,
    static_url_path="",      # serve "index.html", "some.png" directly
    static_folder="."        # treat the current dir as the static folder
)
subscribers: List[queue.Queue] = []          # live SSE listeners

# ----------------------------------------------------------------------
# Helper: forward JSON objects to every connected SSE client
# ----------------------------------------------------------------------
def broadcast(obj: Dict):
    for q in list(subscribers):              # copy → tolerate removals
        try:
            q.put_nowait(obj)
        except queue.Full:
            pass

# ----------------------------------------------------------------------
# Endpoint: Launch a bot
# ----------------------------------------------------------------------
@app.post("/launch")
def launch_bot():
    data = request.get_json(force=True)
    meeting_url = data.get("meeting_url")
    if not meeting_url:
        return jsonify({"error": "meeting_url is required"}), 400

    payload = {
        "meeting_url": meeting_url,
        "bot_name":   "Transcription‑Demo"
    }
    resp = requests.post(
        f"{ATTENDEE_API_BASE}/api/v1/bots",
        headers={
            "Authorization": f"Token {ATTENDEE_API_KEY}",
            "Content-Type":  "application/json",
        },
        json=payload,
        timeout=30,
    )
    if resp.status_code >= 300:
        return jsonify({"error": resp.text}), resp.status_code

    bot = resp.json()
    return jsonify({"bot_id": bot["id"]}), 201

# ----------------------------------------------------------------------
# Endpoint: Make bot leave meeting
# ----------------------------------------------------------------------
@app.post("/leave/<bot_id>")
def leave_bot(bot_id):
    resp = requests.post(
        f"{ATTENDEE_API_BASE}/api/v1/bots/{bot_id}/leave",
        headers={
            "Authorization": f"Token {ATTENDEE_API_KEY}",
            "Content-Type":  "application/json",
        },
        json={},
        timeout=30,
    )
    if resp.status_code >= 300:
        return jsonify({"error": resp.text}), resp.status_code

    return jsonify({"success": True}), 200

# ----------------------------------------------------------------------
# Endpoint: Webhook receiver  (transcript.update, etc.)
# ----------------------------------------------------------------------
def _sign_payload(payload: Dict) -> str:
    """
    Return base‑64 HMAC‑SHA256 of canonical JSON payload.
    """
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                              separators=(",", ":"))
    secret = base64.b64decode(WEBHOOK_SECRET)
    digest = hmac.new(secret, payload_json.encode(), hashlib.sha256).digest()
    return base64.b64encode(digest).decode()

@app.post("/webhook")
def webhook():
    try:
        payload = request.get_json(force=True)
    except Exception:
        abort(400, "invalid JSON")

    sig_header = request.headers.get("X-Webhook-Signature", "")
    if sig_header != _sign_payload(payload):
        abort(400, "invalid signature")

    # Forward transcript lines
    if payload.get("trigger") == "transcript.update":
        broadcast({
            "type": "transcript",
            "data": payload["data"]
        })
    
    # Forward bot status changes
    elif payload.get("trigger") == "bot.state_change":
        broadcast({
            "type": "status",
            "data": payload["data"]
        })

    return "", 200

# ----------------------------------------------------------------------
# Endpoint: Server‑Sent Events stream
# ----------------------------------------------------------------------
@app.get("/stream")
def stream():
    client_q: queue.Queue = queue.Queue()
    subscribers.append(client_q)

    @stream_with_context
    def gen():
        try:
            # Send a comment every 15 s so proxies keep the connection alive
            keepalive = time.time()
            while True:
                try:
                    msg = client_q.get(timeout=1)
                    yield f"data: {json.dumps(msg)}\n\n"
                except queue.Empty:
                    pass
                if time.time() - keepalive > 15:
                    yield ": keep‑alive\n\n"
                    keepalive = time.time()
        finally:
            subscribers.remove(client_q)

    return Response(gen(), mimetype="text/event-stream")

# ----------------------------------------------------------------------

@app.get("/")                # ← add this route
def index():
    # send the SPA entry‑point
    return send_from_directory(app.static_folder, "index.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5005, threaded=True, debug=True)

