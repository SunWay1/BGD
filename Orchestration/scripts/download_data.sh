#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAW_DIR="$SCRIPT_DIR/data/raw"
BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

mkdir -p "$RAW_DIR"

echo "pobieranie NYC Yellow Taxi 2022-2023 do $RAW_DIR"
echo ""

for year in 2022 2023; do
    for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
        filename="yellow_tripdata_${year}-${month}.parquet"
        dest="$RAW_DIR/$filename"

        if [ -f "$dest" ]; then
            echo "  [pomiń] $filename"
            continue
        fi

        echo "  [pobierz] $filename..."
        curl -f -L -o "$dest" "$BASE_URL/$filename" || {
            echo "  [blad] $filename — pomijam"
            rm -f "$dest"
        }
    done
done

ZONES="$RAW_DIR/taxi_zone_lookup.csv"
if [ ! -f "$ZONES" ]; then
    echo ""
    echo "  [pobierz] taxi_zone_lookup.csv..."
    curl -f -L -o "$ZONES" "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
else
    echo "  [pomiń] taxi_zone_lookup.csv"
fi

echo ""
echo "pliki w $RAW_DIR:"
ls -lh "$RAW_DIR"/*.parquet "$RAW_DIR"/*.csv 2>/dev/null | awk '{print "  " $5 "\t" $9}'
