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

SHARED_TOPIC = "$share/project/iot/raw"
TOPIC_RESULT = "edge/result"
TOPIC_METRICS = f"edge/metrics/{NODE}"
TOPIC_CONTROL = "edge/control"

METRIC_INTERVAL = 2.0


if BATCH_SIZE < 1:
    print("BATCH mora biti najmanje 1.")
    sys.exit(1)


# Svaki senzor ima vlastiti batch.
batches = {}
batch_start = {}

received = 0
outputs = 0
last_received = 0

latency_sum = 0.0
latency_count = 0

last_metric_time = time.perf_counter()


client = mqtt.Client(client_id=NODE)


def reset_metrics():
    global received, outputs, last_received
    global latency_sum, latency_count
    global last_metric_time

    batches.clear()
    batch_start.clear()

    received = 0
    outputs = 0
    last_received = 0

    latency_sum = 0.0
    latency_count = 0

    last_metric_time = time.perf_counter()


def on_connect(client, userdata, flags, rc):
    client.subscribe(SHARED_TOPIC)
    client.subscribe(TOPIC_CONTROL)


def on_message(client, userdata, msg):
    global received, outputs
    global latency_sum, latency_count

    if msg.topic == TOPIC_CONTROL:

        if msg.payload.decode().strip() == "reset":
            reset_metrics()

        return

    try:
        data = json.loads(msg.payload.decode())

    except (json.JSONDecodeError, UnicodeDecodeError):
        return

    if (
        "sensor" not in data
        or "temperature" not in data
        or "humidity" not in data
    ):
        return

    sensor = str(data["sensor"])

    if sensor not in batches:
        batches[sensor] = []

    if not batches[sensor]:
        batch_start[sensor] = time.perf_counter()

    batches[sensor].append(data)
    received += 1

    if len(batches[sensor]) < BATCH_SIZE:
        return

    sensor_batch = batches[sensor]

    latency_ms = (
        time.perf_counter() - batch_start[sensor]
    ) * 1000

    avg_temperature = sum(
        item["temperature"]
        for item in sensor_batch
    ) / len(sensor_batch)

    avg_humidity = sum(
        item["humidity"]
        for item in sensor_batch
    ) / len(sensor_batch)

    result = {
        "node": NODE,
        "sensor": sensor,
        "ts": time.time(),
        "temperature": round(avg_temperature, 2),
        "humidity": round(avg_humidity, 2),
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

    batches[sensor].clear()
    del batch_start[sensor]


client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_start()

print(
    f"Worker {NODE}: "
    f"batch={BATCH_SIZE}"
)


try:
    while True:

        time.sleep(METRIC_INTERVAL)

        now = time.perf_counter()
        elapsed = now - last_metric_time

        throughput = (
            received - last_received
        ) / elapsed

        last_received = received
        last_metric_time = now

        if latency_count > 0:
            avg_latency = (
                latency_sum / latency_count
            )
        else:
            avg_latency = 0.0

        latency_sum = 0.0
        latency_count = 0

        metrics = {
            "cpu_percent": psutil.cpu_percent(),
            "ram_percent": psutil.virtual_memory().percent,
            "disk_percent": psutil.disk_usage("/").percent,
            "latency_ms": round(avg_latency, 1),
            "throughput_msg_s": round(throughput, 2),
            "received_total": received,
            "outputs_total": outputs
        }

        client.publish(
            TOPIC_METRICS,
            json.dumps(metrics)
        )


except KeyboardInterrupt:
    pass


finally:
    client.loop_stop()
    client.disconnect()
