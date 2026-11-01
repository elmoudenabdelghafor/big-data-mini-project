import html
import json
import re
import time
import urllib.request

from kafka import KafkaProducer

API_URL = "https://www.arbeitnow.com/api/job-board-api"
TOPIC = "job-offers"
CAPTURE_FILE = "data/jobs_capture.txt"   # copy of what was sent, for replay
POLL_SECONDS = 60      # keep this gentle: do not poll faster than once a minute
SEND_DELAY = 1.0       # seconds between two messages (simulates a stream)
MAX_PER_POLL = 40      # cap per poll, so a demo does not flood the topic

# Keep only IT-related jobs (the model only knows Data / Dev / Security / Cloud)
TECH = re.compile(
    r"\bdata\b|developer|développeur|entwickler|engineer|ingenieur|ingénieur|"
    r"devops|\bcloud\b|\baws\b|azure|\bgcp\b|kubernetes|docker|security|"
    r"sicherheit|cyber|pentest|\bsoc\b|python|\bjava\b|javascript|typescript|"
    r"react|angular|machine learning|\bml\b|\bai\b|\bsql\b|spark|full.?stack|"
    r"back.?end|front.?end|software|\bdev\b|sysadmin|infrastructure|linux|\bapi\b|"
    r"\bit\b|informatik",
    re.I,
)


def fetch_jobs():
    req = urllib.request.Request(
        API_URL,
        headers={"User-Agent": "INPT-student-bigdata-project",
                 "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8")).get("data", [])


def clean(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def build_text(job: dict) -> str:
    """Short, clean text: title + tags. (Descriptions are long and often German.)"""
    title = clean(job.get("title", ""))
    tags = ", ".join(clean(t) for t in (job.get("tags") or []))
    text = f"{title} - {tags}" if tags else title
    return text[:280]


def main():
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    seen = set()
    sent_total = 0
    print("Polling Arbeitnow every", POLL_SECONDS, "s (Ctrl+C to stop)")

    while True:
        try:
            jobs = fetch_jobs()
        except Exception as e:
            print("API error:", e, "-> retry in 30 s")
            time.sleep(30)
            continue

        candidates = []
        for job in jobs:
            key = job.get("slug") or job.get("url")
            if not key or key in seen:
                continue
            text = build_text(job)
            if len(text) < 10 or not TECH.search(text):
                seen.add(key)          # not relevant: never look at it again
                continue
            candidates.append((key, text))

        print(f"Poll: {len(jobs)} jobs, {len(candidates)} new IT jobs")

        try:
            for key, text in candidates[:MAX_PER_POLL]:
                producer.send(TOPIC, value=text.encode("utf-8"))
                with open(CAPTURE_FILE, "a", encoding="utf-8") as f:
                    f.write(text + "\n")
                seen.add(key)
                sent_total += 1
                print(f"Sent ({sent_total}): {text}")
                time.sleep(SEND_DELAY)
            producer.flush()
            time.sleep(POLL_SECONDS)
        except KeyboardInterrupt:
            producer.flush()
            print(f"\nStopped. {sent_total} jobs sent.")
            return


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")