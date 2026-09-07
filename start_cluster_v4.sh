#!/usr/bin/env bash
#
# Upotreba:
#   ./start_cluster_v4.sh RATE BATCH TRAJANJE BROJ_WORKERA
#
# BROJ_WORKERA:
#   1 = pi5-16
#   2 = pi5-16 + pi5-8
#   4 = sva cetiri noda
#
# Primjeri:
#   ./start_cluster_v4.sh 500 5 60 4
#   ./start_cluster_v4.sh 1000 5 60 2
#   ./start_cluster_v4.sh 500 20 60 4

set -u

PROJECT="$HOME/monitorko"
RESULTS="$PROJECT/results"
LOGS="$PROJECT/run_logs"

PI5_8="pi@192.168.50.11"
PI4_1="pi@192.168.50.12"
PI1_512="pi@192.168.50.13"

RATE="${1:-500}"
BATCH="${2:-5}"
DURATION="${3:-60}"
WORKERS="${4:-4}"
WARMUP=10

if [[ "$WORKERS" != "1" && "$WORKERS" != "2" && "$WORKERS" != "4" ]]; then
    echo "BROJ_WORKERA mora biti 1, 2 ili 4."
    exit 1
fi

mkdir -p "$RESULTS" "$LOGS"
cd "$PROJECT" || exit 1

LOCAL_WORKER_PID=""
DASH_PID=""
SIM_PID=""

ALL_REMOTE=("$PI5_8" "$PI4_1" "$PI1_512")
ACTIVE_REMOTE=()

if [[ "$WORKERS" == "2" ]]; then
    ACTIVE_REMOTE=("$PI5_8")
elif [[ "$WORKERS" == "4" ]]; then
    ACTIVE_REMOTE=("$PI5_8" "$PI4_1" "$PI1_512")
fi

stop_remote_workers() {
    for host in "${ALL_REMOTE[@]}"; do
        timeout 8 ssh -n \
            -o BatchMode=yes \
            -o ConnectTimeout=5 \
            "$host" \
            "pkill -f '[w]orker.py' 2>/dev/null || true" \
            >/dev/null 2>&1 || true
    done
}

cleanup() {
    trap - INT TERM

    echo
    echo "Zaustavljam procese..."

    [[ -n "$SIM_PID" ]] && kill "$SIM_PID" 2>/dev/null || true
    [[ -n "$LOCAL_WORKER_PID" ]] && kill "$LOCAL_WORKER_PID" 2>/dev/null || true
    [[ -n "$DASH_PID" ]] && kill "$DASH_PID" 2>/dev/null || true

    stop_remote_workers
    wait 2>/dev/null || true
}

fail() {
    echo
    echo "GRESKA: $1"
    cleanup
    exit 1
}

trap 'cleanup; exit 130' INT TERM

echo "=========================================="
echo " Monitorko test"
echo " RATE:       $RATE msg/s"
echo " BATCH:      $BATCH"
echo " WORKERI:    $WORKERS"
echo " WARM-UP:    $WARMUP s"
echo " MJERENJE:   $DURATION s"
echo "=========================================="
echo

echo "[1/8] Provjeravam potrebne SSH veze..."
for host in "${ACTIVE_REMOTE[@]}"; do
    if ! timeout 8 ssh -n \
        -o BatchMode=yes \
        -o ConnectTimeout=5 \
        "$host" "hostname" >/dev/null 2>&1; then
        fail "SSH bez lozinke ne radi za $host"
    fi
done

echo "[2/8] Provjeravam Mosquitto..."
if ! systemctl is-active --quiet mosquitto; then
    sudo systemctl start mosquitto || fail "Mosquitto se nije pokrenuo."
fi

echo "[3/8] Cistim prethodni test..."
pkill -f '[s]imulator.py' 2>/dev/null || true
pkill -f '[d]ashboard_server.py' 2>/dev/null || true
pkill -f '[w]orker.py' 2>/dev/null || true
stop_remote_workers
rm -f "$PROJECT/metrics.csv"
sleep 2

echo "[4/8] Pokrecem dashboard_server.py..."
python3 "$PROJECT/dashboard_server.py" \
    >"$LOGS/dashboard.log" 2>&1 &
DASH_PID=$!
sleep 2
kill -0 "$DASH_PID" 2>/dev/null || fail "dashboard_server.py nije aktivan."

echo "[5/8] Pokrecem workere..."
python3 "$PROJECT/worker.py" "$BATCH" \
    >"$LOGS/pi5-16_worker.log" 2>&1 &
LOCAL_WORKER_PID=$!

for host in "${ACTIVE_REMOTE[@]}"; do
    echo "  -> $host"
    timeout 8 ssh -n \
        -o BatchMode=yes \
        -o ConnectTimeout=5 \
        "$host" \
        "cd ~/monitorko && setsid -f python3 worker.py '$BATCH' >/tmp/monitorko_worker.log 2>&1 </dev/null" \
        >/dev/null 2>&1 || true
done

sleep 4

kill -0 "$LOCAL_WORKER_PID" 2>/dev/null || fail "worker na pi5-16 nije aktivan."

for host in "${ACTIVE_REMOTE[@]}"; do
    if ! timeout 8 ssh -n \
        -o BatchMode=yes \
        -o ConnectTimeout=5 \
        "$host" "pgrep -f '[w]orker.py' >/dev/null"; then
        echo "Log s $host:"
        timeout 8 ssh -n \
            -o BatchMode=yes \
            -o ConnectTimeout=5 \
            "$host" "tail -20 /tmp/monitorko_worker.log 2>/dev/null || true"
        fail "worker na $host nije aktivan."
    fi
done

echo "[6/8] Pokrecem simulator..."
python3 "$PROJECT/simulator.py" "$RATE" \
    >"$LOGS/simulator.log" 2>&1 &
SIM_PID=$!
sleep 2
kill -0 "$SIM_PID" 2>/dev/null || fail "simulator.py nije aktivan."

echo "[7/8] Zagrijavanje $WARMUP s..."
sleep "$WARMUP"

# Brisanje CSV-a nakon zagrijavanja. dashboard_server.py svako
# zapisivanje otvara datoteku iznova, pa ce sljedeci zapis napraviti novi CSV.
rm -f "$PROJECT/metrics.csv"

echo "[8/8] Mjerenje $DURATION s..."
sleep "$DURATION"

cleanup

RESULT_FILE="$RESULTS/metrics_${WORKERS}node_${BATCH}batch_${RATE}mps.csv"

if [[ -f "$PROJECT/metrics.csv" ]]; then
    mv "$PROJECT/metrics.csv" "$RESULT_FILE"
    echo
    echo "Gotovo."
    echo "CSV:"
    echo "  $RESULT_FILE"
    echo
    echo "Simulator log:"
    echo "  $LOGS/simulator.log"
else
    echo "GRESKA: metrics.csv nije pronaden."
    exit 1
fi
