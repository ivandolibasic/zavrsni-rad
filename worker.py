#!/usr/bin/env python3

import json
import socket
import sys
import time

import paho.mqtt.client as mqtt
import psutil


BROKER = "192.168.50.10"
PORT = 1883

NODE = socket.gethostname()
BATCH_SIZE = int(sys.argv[1]) if len(sys.argv) > 1 else 5

TOPIC_RAW = "iot/raw"
SHARED_GROUP = "project"
TOPIC_RESULT = "edge/result"
TOPIC_LOG = "edge/log"
TOPIC_METRICS = f"edge/metrics/{NODE}"

SHARED_TOPIC = f"$share/{SHARED_GROUP}/{TOPIC_RAW}"


if BATCH_SIZE < 1:
    print("Veličina batcha mora biti najmanje 1.")
    sys.exit(1)


# Svaki senzor ima vlastiti batch.
batches = {}
batch_start = {}

received = 0
outputs = 0
last_received = 0

latency_sum = 0.0
latency_count = 0


client = mqtt.Client(client_id=NODE)


def on_connect(client, userdata, flags, rc):
    client.subscribe(SHARED_TOPIC)

    client.publish(
        TOPIC_LOG,
        f"[{NODE}] worker spojen, batch={BATCH_SIZE}"
    )


def on_message(client, userdata, msg):
    global received, outputs
    global latency_sum, latency_count

    try:
        data = json.loads(msg.payload.decode())
    except (json.JSONDecodeError, UnicodeDecodeError):
        client.publish(
            TOPIC_LOG,
            f"[{NODE}] primljena neispravna JSON poruka"
        )
        return

    if "sensor" not in data:
        client.publish(
            TOPIC_LOG,
            f"[{NODE}] poruka nema polje sensor"
        )
        return

    if "temperature" not in data or "humidity" not in data:
        client.publish(
            TOPIC_LOG,
            f"[{NODE}] poruka nema temperaturu ili vlagu"
        )
        return

    sensor = data["sensor"]

    # Ako senzor još nema batch, stvara se novi.
    if sensor not in batches:
        batches[sensor] = []

    # Vrijeme se bilježi kada stigne prva poruka novog batcha.
    if not batches[sensor]:
        batch_start[sensor] = time.perf_counter()

    batches[sensor].append(data)
    received += 1

    # Obrada počinje tek kada ovaj senzor prikupi dovoljno mjerenja.
    if len(batches[sensor]) < BATCH_SIZE:
        return

    sensor_batch = batches[sensor]

    latency_ms = (
        time.perf_counter() - batch_start[sensor]
    ) * 1000

    temperature_sum = sum(
        item["temperature"] for item in sensor_batch
    )

    humidity_sum = sum(
        item["humidity"] for item in sensor_batch
    )

    average_temperature = temperature_sum / len(sensor_batch)
    average_humidity = humidity_sum / len(sensor_batch)

    result = {
        "node": NODE,
        "sensor": sensor,
        "ts": time.time(),
        "temperature": round(average_temperature, 2),
        "humidity": round(average_humidity, 2),
        "samples": len(sensor_batch),
        "latency_ms": round(latency_ms, 1)
    }

    client.publish(
        TOPIC_RESULT,
        json.dumps(result)
    )

    outputs += 1
    latency_sum += latency_ms
    latency_count += 1

    # Briše se samo batch obrađenog senzora.
    batches[sensor].clear()
    del batch_start[sensor]


client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_start()

print(
    f"Worker {NODE}: "
    f"batch={BATCH_SIZE}, "
    f"tema={SHARED_TOPIC}"
)


try:
    while True:
        time.sleep(2)

        throughput = (received - last_received) / 2
        last_received = received

        if latency_count > 0:
            average_latency_ms = latency_sum / latency_count
        else:
            average_latency_ms = 0.0

        latency_sum = 0.0
        latency_count = 0

        metrics = {
            "node": NODE,
            "cpu": psutil.cpu_percent(),
            "ram": psutil.virtual_memory().percent,
            "disk": psutil.disk_usage("/").percent,
            "latency_ms": round(average_latency_ms, 1),
            "throughput": round(throughput, 1),
            "received": received,
            "outputs": outputs
        }

        client.publish(
            TOPIC_METRICS,
            json.dumps(metrics)
        )

except KeyboardInterrupt:
    client.publish(
        TOPIC_LOG,
        f"[{NODE}] worker zaustavljen"
    )

    client.loop_stop()
    client.disconnect()

    print(f"\nWorker {NODE} zaustavljen.")
