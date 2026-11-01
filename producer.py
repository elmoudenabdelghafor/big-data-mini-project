import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")

# utf-8-sig : tolère un éventuel BOM ajouté par Windows
with open("data/offres.txt", encoding="utf-8-sig") as f:
    for line in f:
        line = line.strip()
        if line:
            producer.send("job-offers", value=line.encode("utf-8"))
            print("Envoyé :", line)
            time.sleep(1)

producer.flush()
print("Terminé.")