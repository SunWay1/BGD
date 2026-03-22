#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
DB_NAME="bgd_taxidb"
CLEAN=false

for arg in "$@"; do
    case $arg in
        --clean) CLEAN=true ;;
    esac
done

echo "=================================================="
echo "  BGD ETL Task 1"
echo "  baza: $DB_NAME"
echo "  katalog: $ROOT_DIR"
echo "=================================================="

# sprawdz czy mongosh i python sa dostepne
echo ""
echo "Sprawdzam wymagania..."

MONGOSH=$(command -v mongosh 2>/dev/null || command -v mongosh.exe 2>/dev/null || where mongosh 2>/dev/null | head -1)
if [ -z "$MONGOSH" ]; then
    echo "brak mongosh w PATH"
    exit 1
fi

if ! "$MONGOSH" --quiet --eval "db.adminCommand('ping')" &>/dev/null; then
    echo "nie mozna polaczyc sie z MongoDB (localhost:27017)"
    exit 1
fi

# szukaj .venv dwa poziomy wyzej, fallback na systemowego pythona
VENV_DIR="$(dirname "$ROOT_DIR")/.venv"
if [ -f "$VENV_DIR/Scripts/python.exe" ]; then
    PYTHON="$VENV_DIR/Scripts/python.exe"
elif [ -f "$VENV_DIR/bin/python" ]; then
    PYTHON="$VENV_DIR/bin/python"
elif command -v python3 &>/dev/null; then
    PYTHON=$(command -v python3)
elif command -v python &>/dev/null; then
    PYTHON=$(command -v python)
else
    echo "nie znaleziono pythona"
    exit 1
fi
echo "  mongosh: ok"
echo "  python:  $PYTHON"
echo "  mongodb: ok"

# sprawdz czy sa pliki danych
PARQUET_COUNT=$(ls "$ROOT_DIR/data/raw/yellow_tripdata_"*.parquet 2>/dev/null | wc -l)
if [ "$PARQUET_COUNT" -eq 0 ]; then
    echo ""
    echo "brak plikow parquet w data/raw/"
    echo "pobierz je recznie:"
    echo "  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    echo ""
    echo "przyklad (jeden miesiac):"
    echo "  curl -L -o data/raw/yellow_tripdata_2023-01.parquet \\"
    echo "    https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    exit 1
fi

if [ ! -f "$ROOT_DIR/data/raw/taxi_zone_lookup.csv" ]; then
    echo "pobieram taxi_zone_lookup.csv..."
    curl -L -o "$ROOT_DIR/data/raw/taxi_zone_lookup.csv" \
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
fi

echo "  pliki parquet: $PARQUET_COUNT"
echo "  taxi_zone_lookup.csv: ok"

# krok 0 - zaladuj dane raw jesli baza jest pusta
echo ""
RAW_COUNT=$("$MONGOSH" --quiet "$DB_NAME" --eval "db.raw_trips.countDocuments()" 2>/dev/null | tail -1)

if [ "$RAW_COUNT" -gt 0 ] 2>/dev/null; then
    echo "[0] raw_trips ma juz $RAW_COUNT dokumentow, pomijam ladowanie"
else
    echo "[0] laduje dane do warstwy raw..."
    cd "$ROOT_DIR"
    "$PYTHON" scripts/load_data.py
    echo "  gotowe"
fi

# krok 0b - wyczysc clean i gold jesli --clean
if [ "$CLEAN" = true ]; then
    echo ""
    echo "[0b] czyszcze warstwy clean + gold..."
    "$MONGOSH" --quiet "$DB_NAME" --eval "
['clean_trips','clean_zones','gold_zone_revenue','gold_hourly_demand','gold_top_routes']
    .forEach(c => { db[c].drop(); print('  dropped: ' + c); });
"
fi

# krok 1 - weryfikacja raw
echo ""
echo "[1] weryfikacja warstwy raw..."
"$MONGOSH" --quiet "$DB_NAME" scripts/01_raw_layer.js

# krok 2 - warstwa clean
echo ""
echo "[2] buduje warstwe clean..."
"$MONGOSH" --quiet "$DB_NAME" scripts/02_clean_layer.js

# krok 3 - warstwa gold
echo ""
echo "[3] buduje warstwe gold..."
"$MONGOSH" --quiet "$DB_NAME" scripts/03_gold_layer.js

echo ""
echo "=================================================="
echo "  gotowe. kolekcje w $DB_NAME:"
echo "=================================================="
"$MONGOSH" --quiet "$DB_NAME" --eval "
db.getCollectionNames().forEach(name => {
    const count = db[name].countDocuments();
    print('  ' + name + ': ' + count.toLocaleString() + ' docs');
});
"
