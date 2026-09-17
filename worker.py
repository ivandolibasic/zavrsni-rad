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
TOPIC_LOG = "edge/log"

METRIC_INTERVAL = 2.0


if BATCH_SIZE < 1:
    sys.exit(1)


batches = {}
batch_start = {}

received = 0
processed = 0
outputs = 0

last_received = 0
last_processed = 0
last_outputs = 0

latency_sum = 0.0
latency_count = 0

last_metric_time = time.perf_counter()


client = mqtt.Client(client_id=NODE)


def reset_metrics():
    global received, processed, outputs
    global last_received, last_processed, last_outputs
    global latency_sum, latency_count
    global last_metric_time

    batches.clear()
    batch_start.clear()

    received = 0
    processed = 0
    outputs = 0

    last_received = 0
    last_processed = 0
    last_outputs = 0

    latency_sum = 0.0
    latency_count = 0

    last_metric_time = time.perf_counter()


def on_connect(client, userdata, flags, rc):
    client.subscribe(SHARED_TOPIC)
    client.subscribe(TOPIC_CONTROL)

    client.publish(
        TOPIC_LOG,
        f"[{NODE}] worker aktivan, batch={BATCH_SIZE}"
    )


def on_message(client, userdata, msg):
    global received, processed, outputs
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

    avg_temperature = sum(
        item["temperature"]
        for item in sensor_batch
    ) / len(sensor_batch)

    avg_humidity = sum(
        item["humidity"]
        for item in sensor_batch
    ) / len(sensor_batch)

    # Latencija se mjeri nakon završetka lokalne agregacije.
    latency_ms = (
        time.perf_counter() - batch_start[sensor]
    ) * 1000

    result = {
        "node": NODE,
        "sensor": sensor,
        "ts": time.time(),
        "temperature": round(avg_temperature, 2),
        "humidity": round(avg_humidity, 2),
        "samples": len(sensor_batch),
        "latency_ms": round(latency_ms, 2)
    }

    client.publish(
        TOPIC_RESULT,
        json.dumps(result)
    )

    processed += len(sensor_batch)
    outputs += 1

    latency_sum += latency_ms
    latency_count += 1

    batches[sensor].clear()
    del batch_start[sensor]


client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_start()


try:
    while True:
        time.sleep(METRIC_INTERVAL)

        now = time.perf_counter()
        elapsed = now - last_metric_time

        received_rate = (
            received - last_received
        ) / elapsed

        processed_rate = (
            processed - last_processed
        ) / elapsed

        output_rate = (
            outputs - last_outputs
        ) / elapsed

        last_received = received
        last_processed = processed
        last_outputs = outputs
        last_metric_time = now

        if latency_count:
            avg_latency = latency_sum / latency_count
        else:
            avg_latency = 0.0

        latency_sum = 0.0
        latency_count = 0

        metrics = {
            "batch_size": BATCH_SIZE,
            "cpu_percent": psutil.cpu_percent(),
            "ram_percent": psutil.virtual_memory().percent,
            "disk_percent": psutil.disk_usage("/").percent,

            "latency_ms": round(avg_latency, 2),

            "input_throughput_msg_s": round(
                received_rate,
                2
            ),

            "processed_throughput_msg_s": round(
                processed_rate,
                2
            ),

            "output_rate_result_s": round(
                output_rate,
                2
            ),

            # Dashboard prikazuje upravo ovu vrijednost.
            "throughput_msg_s": round(
                processed_rate,
                2
            ),

            "received_total": received,
            "processed_total": processed,
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
