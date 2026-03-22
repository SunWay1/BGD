# BGD — ETL Zadanie 1: Analiza przejazdów NYC Yellow Taxi

## Problem Statement

Celem projektu jest analiza wzorców przejazdów taksówkowych w Nowym Jorku na podstawie datasetu **NYC TLC Yellow Taxi Trip Records**. Główne pytanie analityczne:

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
