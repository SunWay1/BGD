#!/usr/bin/env bash

set -e  # exit on any error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
DB_NAME="bgd_taxidb"

echo "=================================================="
echo "  BGD ETL Task 1 — Full Pipeline"
echo "  Database: $DB_NAME"
echo "  Root: $ROOT_DIR"
echo "=================================================="

# ─── Sprawdź wymagania ───────────────────────────────────────────────────────
echo ""
echo "Sprawdzam wymagania..."

MONGOSH=$(command -v mongosh 2>/dev/null || command -v mongosh.exe 2>/dev/null || where mongosh 2>/dev/null | head -1)
if [ -z "$MONGOSH" ]; then
    echo "BŁĄD: mongosh nie znaleziony"
    exit 1
fi

if ! "$MONGOSH" --quiet --eval "db.adminCommand('ping')" &>/dev/null; then
    echo "BŁĄD: Brak połączenia z MongoDB na localhost:27017"
    exit 1
fi

VENV_DIR="$(dirname "$ROOT_DIR")/.venv"

# Szukaj .venv dwa poziomy powyżej run_all.sh, fallback na systemowego Pythona
if [ -f "$VENV_DIR/Scripts/python.exe" ]; then
    PYTHON="$VENV_DIR/Scripts/python.exe"
elif [ -f "$VENV_DIR/bin/python" ]; then
    PYTHON="$VENV_DIR/bin/python"
elif command -v python3 &>/dev/null; then
    PYTHON=$(command -v python3)
elif command -v python &>/dev/null; then
    PYTHON=$(command -v python)
else
    echo "BŁĄD: Python nie znaleziony."
    exit 1
fi
echo "  mongosh: OK"
echo "  Python: $PYTHON"
echo "  MongoDB: connected"

# ─── Sprawdź pliki danych ────────────────────────────────────────────────────
PARQUET_COUNT=$(ls "$ROOT_DIR/data/raw/yellow_tripdata_"*.parquet 2>/dev/null | wc -l)
if [ "$PARQUET_COUNT" -eq 0 ]; then
    echo ""
    echo "BŁĄD: Brak plików parquet w data/raw/"
    echo "Pobierz je ręcznie ze strony:"
    echo "  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    echo "Przykład (jeden miesiąc):"
    echo "  curl -L -o data/raw/yellow_tripdata_2023-01.parquet \\"
    echo "    https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    exit 1
fi

if [ ! -f "$ROOT_DIR/data/raw/taxi_zone_lookup.csv" ]; then
    echo "Pobieranie taxi_zone_lookup.csv..."
    curl -L -o "$ROOT_DIR/data/raw/taxi_zone_lookup.csv" \
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
fi

echo "  Parquet files found: $PARQUET_COUNT"
echo "  taxi_zone_lookup.csv: OK"

# ─── Krok 0: Sprawdź czy warstwa raw już istnieje ────────────────────────────
echo ""
RAW_COUNT=$("$MONGOSH" --quiet "$DB_NAME" --eval "db.raw_trips.countDocuments()" 2>/dev/null | tail -1)

if [ "$RAW_COUNT" -gt 0 ] 2>/dev/null; then
    echo "[Krok 0] Warstwa raw istnieje (raw_trips: $RAW_COUNT dokumentów) — pomijam ładowanie."
else
    echo "[Krok 0] Warstwa raw pusta — ładowanie danych..."
    cd "$ROOT_DIR"
    "$PYTHON" scripts/load_data.py
    echo "  Dane załadowane."
fi

# ─── Wyczyść warstwy clean i gold (raw zostaje) ──────────────────────────────
echo ""
echo "[Krok 0b] Usuwam warstwy clean + gold"
"$MONGOSH" --quiet "$DB_NAME" --eval "
['clean_trips','clean_zones','gold_zone_revenue','gold_hourly_demand','gold_top_routes']
    .forEach(c => { db[c].drop(); print('  dropped: ' + c); });
"

# ─── Krok 3: Weryfikacja warstwy raw ─────────────────────────────────────────
echo ""
echo "[Krok 3] Weryfikacja warstwy raw..."
"$MONGOSH" --quiet "$DB_NAME" scripts/01_raw_layer.js

# ─── Krok 4: Budowanie warstwy clean ─────────────────────────────────────────
echo ""
echo "[Krok 4] Budowanie warstwy clean (transformacje)..."
"$MONGOSH" --quiet "$DB_NAME" scripts/02_clean_layer.js

# ─── Krok 5: Budowanie warstwy gold ──────────────────────────────────────────
echo ""
echo "[Krok 5] Budowanie warstwy gold (agregacje + JOIN-y)..."
"$MONGOSH" --quiet "$DB_NAME" scripts/03_gold_layer.js

echo ""
echo "=================================================="
echo "  Pipeline zakończony! Kolekcje w $DB_NAME:"
echo "=================================================="
"$MONGOSH" --quiet "$DB_NAME" --eval "
db.getCollectionNames().forEach(name => {
    const count = db[name].countDocuments();
    print('  ' + name + ': ' + count.toLocaleString() + ' documents');
});
"
