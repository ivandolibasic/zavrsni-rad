#!/usr/bin/env python3

import json
import socket
import sys
import time

import paho.mqtt.client as mqtt
import psutil


BROKER = "192.168.50.10"
NODE = socket.gethostname()
BATCH = int(sys.argv[1]) if len(sys.argv) > 1 else 5

batch = []
batch_start = 0.0

received = 0
outputs = 0
last_received = 0

latency_sum = 0.0
latency_count = 0


client = mqtt.Client(client_id=NODE)


def on_connect(client, userdata, flags, rc):
    client.subscribe("$share/monitorko/iot/raw")
    client.publish("edge/log", f"[{NODE}] worker spojen, batch={BATCH}")


def on_message(client, userdata, msg):
    global batch_start, received, outputs
    global latency_sum, latency_count

    data = json.loads(msg.payload.decode())

    if not batch:
        batch_start = time.perf_counter()

    batch.append(data)
    received += 1

    if len(batch) < BATCH:
        return

    latency = (time.perf_counter() - batch_start) * 1000

    result = {
        "node": NODE,
        "ts": time.time(),
        "temperature": round(sum(x["temperature"] for x in batch) / len(batch), 2),
        "humidity": round(sum(x["humidity"] for x in batch) / len(batch), 2),
        "samples": len(batch),
        "latency": round(latency, 1),
    }

    client.publish("edge/result", json.dumps(result))

    outputs += 1
    latency_sum += latency
    latency_count += 1
    batch.clear()


client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, 1883)
client.loop_start()

print(f"Worker {NODE}: batch={BATCH}")


try:
    while True:
        time.sleep(2)

        throughput = (received - last_received) / 2
        last_received = received

        latency = latency_sum / latency_count if latency_count else 0.0
        latency_sum = 0.0
        latency_count = 0

        metrics = {
            "cpu": psutil.cpu_percent(),
            "ram": psutil.virtual_memory().percent,
            "disk": psutil.disk_usage("/").percent,
            "latency": round(latency, 1),
            "throughput": round(throughput, 1),
            "received": received,
            "outputs": outputs,
        }

        client.publish(f"edge/metrics/{NODE}", json.dumps(metrics))

except KeyboardInterrupt:
    client.publish("edge/log", f"[{NODE}] worker zaustavljen")
    client.loop_stop()
    client.disconnect()
