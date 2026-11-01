import json
from websocket import create_connection

URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

ws = create_connection(URL, timeout=30)
print("Connecté. 5 premiers posts :\n")
n = 0
while n < 5:
    ev = json.loads(ws.recv())
    c = ev.get("commit", {})
    if ev.get("kind") == "commit" and c.get("operation") == "create":
        text = c["record"].get("text", "").replace("\n", " ")
        print("-", text[:120])
        n += 1
ws.close()