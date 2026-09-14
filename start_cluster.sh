#!/usr/bin/env bash

#
# Upotreba:
# ./start_cluster.sh RATE BATCH TRAJANJE WORKERI
#
# Primjer:
# ./start_cluster.sh 500 5 60 4
#

set -u


PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RESULTS="$PROJECT/results"
LOGS="$PROJECT/run_logs"

REMOTE_PROJECT="~/monitorko"

BROKER="192.168.50.10"

PI5_8="pi@192.168.50.11"
PI4_1="pi@192.168.50.12"
PI1_512="pi@192.168.50.13"

RATE="${1:-500}"
BATCH="${2:-5}"
DURATION="${3:-60}"
WORKERS="${4:-4}"

WARMUP=10
RUN_ID="$(date +%Y%m%d_%H%M%S)"


if [[ "$WORKERS" != "1" &&
      "$WORKERS" != "2" &&
      "$WORKERS" != "4" ]]; then

    echo "WORKERI moraju biti 1, 2 ili 4."
    exit 1
fi


mkdir -p "$RESULTS" "$LOGS"
cd "$PROJECT" || exit 1


WORKER_PID=""
SIMULATOR_PID=""
SERVER_PID=""


ALL_REMOTE=(
    "$PI5_8"
    "$PI4_1"
    "$PI1_512"
)


ACTIVE_REMOTE=()


if [[ "$WORKERS" == "2" ]]; then

    ACTIVE_REMOTE=(
        "$PI5_8"
    )

elif [[ "$WORKERS" == "4" ]]; then

    ACTIVE_REMOTE=(
        "$PI5_8"
        "$PI4_1"
        "$PI1_512"
    )
fi


mqtt_pub() {

    timeout 5 mosquitto_pub \
        -h "$BROKER" \
        -t "$1" \
        -m "$2"
}


stop_pid() {

    local pid="$1"

    if [[ -z "$pid" ]]; then
        return
    fi

    if ! kill -0 "$pid" 2>/dev/null; then
        return
    fi

    kill -TERM "$pid" 2>/dev/null || true

    sleep 0.2

    if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid" 2>/dev/null || true
    fi
}


stop_remote_workers() {

    local pids=()

    for host in "$@"; do

        (
            timeout 3 ssh -n \
                -o BatchMode=yes \
                -o ConnectTimeout=2 \
                "$host" \
                "pkill -TERM -f '[w]orker.py' 2>/dev/null || true" \
                >/dev/null 2>&1
        ) &

        pids+=("$!")
    done

    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
}


cleanup() {

    trap - INT TERM

    echo
    echo "Zaustavljam sustav..."

    stop_pid "$SIMULATOR_PID"
    stop_pid "$WORKER_PID"

    if [[ ${#ACTIVE_REMOTE[@]} -gt 0 ]]; then
        stop_remote_workers "${ACTIVE_REMOTE[@]}"
    fi

    stop_pid "$SERVER_PID"

    echo "Sustav zaustavljen."
}


fail() {

    echo
    echo "GREŠKA: $1"

    cleanup
    exit 1
}


trap 'cleanup; exit 130' INT TERM


echo "=================================="
echo " RATE:     $RATE poruka/s"
echo " BATCH:    $BATCH"
echo " WORKERI:  $WORKERS"
echo " TRAJANJE: $DURATION s"
echo "=================================="
echo


echo "[1/6] Čistim stare procese..."

pkill -TERM -f '[s]imulator.py' \
    2>/dev/null || true

pkill -TERM -f '[w]orker.py' \
    2>/dev/null || true

pkill -TERM -f '[d]ashboard_server.py' \
    2>/dev/null || true

stop_remote_workers "${ALL_REMOTE[@]}"

rm -f "$PROJECT/metrics.csv"

sleep 1


echo "[2/6] Pokrećem dashboard..."

python3 "$PROJECT/dashboard_server.py" \
    >"$LOGS/dashboard.log" 2>&1 &

SERVER_PID=$!

sleep 1

if ! kill -0 "$SERVER_PID" 2>/dev/null; then

    cat "$LOGS/dashboard.log"

    fail "Dashboard server nije pokrenut."
fi


echo "[3/6] Pokrećem workere..."

python3 "$PROJECT/worker.py" "$BATCH" \
    >"$LOGS/pi5-16_worker.log" 2>&1 &

WORKER_PID=$!


for host in "${ACTIVE_REMOTE[@]}"; do

    echo "  -> $host"

    scp -q \
        "$PROJECT/worker.py" \
        "$host:$REMOTE_PROJECT/worker.py" \
        || fail "Ne mogu kopirati worker.py na $host"

    ssh -n "$host" \
        "cd $REMOTE_PROJECT && setsid -f python3 worker.py '$BATCH' >/tmp/worker.log 2>&1 </dev/null" \
        || fail "Ne mogu pokrenuti worker na $host"

done


sleep 2


if ! kill -0 "$WORKER_PID" 2>/dev/null; then

    cat "$LOGS/pi5-16_worker.log"

    fail "Lokalni worker nije pokrenut."
fi


echo "[4/6] Pokrećem simulator..."

python3 "$PROJECT/simulator.py" "$RATE" \
    >"$LOGS/simulator.log" 2>&1 &

SIMULATOR_PID=$!

sleep 1


if ! kill -0 "$SIMULATOR_PID" 2>/dev/null; then

    cat "$LOGS/simulator.log"

    fail "Simulator nije pokrenut."
fi


echo "[5/6] Warm-up $WARMUP s..."

sleep "$WARMUP"


echo "Pripremam mjerenje..."

mqtt_pub "iot/control" "pause"

sleep 1

mqtt_pub "edge/control" "reset"
mqtt_pub "iot/control" "reset"

rm -f "$PROJECT/metrics.csv"

mqtt_pub "iot/control" "resume"


echo "[6/6] Mjerenje $DURATION s..."

sleep "$DURATION"


#
# Zaustavi generiranje novih poruka.
#
mqtt_pub "iot/control" "pause"


#
# Odmah zaustavi sve procese.
# Nema dodatnog čekanja i dodatnih worker metrika.
#
cleanup


RESULT_FILE="$RESULTS/test_${RUN_ID}_w${WORKERS}_r${RATE}_b${BATCH}.csv"


if [[ ! -f "$PROJECT/metrics.csv" ]]; then

    echo
    echo "GREŠKA: metrics.csv nije pronađen."
    exit 1
fi


mv \
    "$PROJECT/metrics.csv" \
    "$RESULT_FILE"


echo
echo "Gotovo."
echo
echo "Rezultat:"
echo "  $RESULT_FILE"
