#!/usr/bin/env python3

import asyncio
import csv
import json
import os
import time

import paho.mqtt.client as mqtt
import websockets


BROKER = "192.168.50.10"

iot = []
cluster = {}
logs = []
simulator_status = "unknown"


def save_metric(node, data):
    new_file = not os.path.exists("metrics.csv")

    with open("metrics.csv", "a", newline="") as f:
        w = csv.writer(f)

        if new_file:
            w.writerow([
                "time", "node", "cpu", "ram", "disk",
                "latency", "throughput", "received", "outputs"
            ])

        w.writerow([
            time.strftime("%H:%M:%S"),
            node,
            data["cpu"],
            data["ram"],
            data["disk"],
            data["latency"],
            data["throughput"],
            data["received"],
            data["outputs"],
        ])


def on_connect(client, userdata, flags, rc):
    client.subscribe("edge/result")
    client.subscribe("edge/metrics/+")
    client.subscribe("edge/log")
    client.subscribe("iot/status")


def on_message(client, userdata, msg):
    global simulator_status

    if msg.topic == "edge/result":
        iot.append(json.loads(msg.payload.decode()))
        del iot[:-30]

    elif msg.topic.startswith("edge/metrics/"):
        node = msg.topic.split("/")[-1]
        data = json.loads(msg.payload.decode())
        cluster[node] = data
        save_metric(node, data)

    elif msg.topic == "edge/log":
        logs.append(msg.payload.decode())
        del logs[:-50]

    elif msg.topic == "iot/status":
        simulator_status = msg.payload.decode()


mqtt_client = mqtt.Client(client_id="dashboard")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(BROKER, 1883)
mqtt_client.loop_start()


async def browser(ws):
    try:
        while True:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=1)

                command = json.loads(message).get("command")

                if command in ("pause", "resume"):
                    mqtt_client.publish("iot/control", command)

            except asyncio.TimeoutError:
                pass

            await ws.send(json.dumps({
                "iot": iot,
                "cluster": cluster,
                "logs": logs,
                "simulator_status": simulator_status,
            }))

    except websockets.exceptions.ConnectionClosed:
        pass


async def main():
    print("Dashboard server: ws://0.0.0.0:8765")

    async with websockets.serve(browser, "0.0.0.0", 8765):
        await asyncio.Future()


try:
    asyncio.run(main())

except KeyboardInterrupt:
    pass

finally:
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
