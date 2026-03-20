# BGD — ETL Zadanie 1: Analiza przejazdów NYC Yellow Taxi

## Problem Statement

Celem projektu jest analiza wzorców przejazdów taksówkowych w Nowym Jorku na podstawie datasetu **NYC TLC Yellow Taxi Trip Records** (~10 GB). Główne pytanie analityczne:

> **Które strefy geograficzne generują największe przychody i jak zmienia się popyt na przejazdy w zależności od godziny dnia?**

Projekt implementuje pełny pipeline ETL oparty na architekturze 3-warstwowej (Medallion):

1. **Warstwa Raw** — ingestion surowych danych do MongoDB bez modyfikacji
2. **Warstwa Clean** — filtrowanie błędnych rekordów, normalizacja pól, dodanie atrybutów pochodnych
3. **Warstwa Gold** — agregacje analityczne z użyciem JOIN (`$lookup`) i GROUP BY (`$group`)

---

## Dataset

| Właściwość | Wartość |
| --- | --- |
| Źródło | NYC TLC Yellow Taxi Trip Records |
| URL | <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page> |
| Zakres | 01.2022 – 12.2023 (miesięczne pliki Parquet) |
| Uzupełnienie | taxi_zone_lookup.csv (265 stref geograficznych) |

**Kluczowe pola:** `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `PULocationID`, `DOLocationID`, `fare_amount`, `tip_amount`, `total_amount`

---

### Kolekcje

| Warstwa | Kolekcja | Opis |
| --- | --- | --- |
| Raw | `raw_trips` | Wszystkie przejazdy załadowane z plików Parquet bez zmian |
| Raw | `raw_zones` | 265 stref taksówkowych z pliku CSV |
| Clean | `clean_trips` | Filtrowane (poprawne opłaty/dystanse), wzbogacone o `pickup_hour`, `trip_duration_min`, `tip_pct` |
| Clean | `clean_zones` | Znormalizowane nazwy dzielnic, flaga `is_airport` |
| Gold | `gold_zone_revenue` | Przychód i średnia opłata per strefa (JOIN + GROUP BY) |
| Gold | `gold_hourly_demand` | Liczba przejazdów per godzina doby (GROUP BY) |
| Gold | `gold_top_routes` | Top 50 tras odbiór→docel (GROUP BY + podwójny JOIN) |

---

## Ryzyka jakości danych

### Ryzyko 1 — Błędne wartości fare_amount i trip_distance (Ingestion)

**Opis:** `fare_amount` zawiera wartości ujemne i zerowe; `trip_distance` zawiera 0.0 oraz fizycznie niemożliwe wartości (>200 mil dla taksówki w NYC).

**Wpływ:** Agregacje przychodów w warstwie gold są zaburzone. Obliczenia średniej opłaty są bez sensu bez wcześniejszego odfiltrowania.

**Mitygacja:** W `02_clean_layer.js`:

```js
{ $match: { fare_amount: { $gt: 0, $lte: 500 }, trip_distance: { $gt: 0, $lte: 200 } } }
```

---

### Ryzyko 2 — Niezidentyfikowane LocationID / osierocone klucze obce (Transformation)

**Opis:** Rekordy przejazdów zawierają `PULocationID` 264 i 265, które reprezentują strefy "Unknown" — nie mają odpowiednika w `taxi_zone_lookup.csv`. Po `$lookup` te rekordy zwracają pustą tablicę `zone_info`.

**Wpływ:** Przychody przypisane nieznanym strefom są wykluczone z `gold_zone_revenue`, zaniżając sumy przychodów.

**Mitygacja:** W `03_gold_layer.js`:

```js
{ $match: { zone_info: { $ne: [] } } }
```

---

### Ryzyko 3 — Schema drift między plikami miesięcznymi (Scale)

**Opis:** NYC TLC dodawało nowe kolumny w kolejnych latach: `congestion_surcharge` (od 02.2019) i `airport_fee` (od 01.2022). Pliki sprzed tych dat nie zawierają tych kolumn.

**Wpływ:** Agregacja `total_amount` przez wiele lat jest niespójna — starsze rekordy nie uwzględniają dopłat zawartych w nowszych. Dla zakresu 2022–2023 problem nie wystąpił, ale jest ryzykiem przy rozszerzeniu datasetu na wcześniejsze lata.

**Mitygacja:** W `02_clean_layer.js`, `$ifNull` uzupełnia brakujące wartości zerami:

```js
congestion_surcharge: { $ifNull: ["$congestion_surcharge", 0] },
airport_fee:          { $ifNull: ["$airport_fee", 0] }
```
