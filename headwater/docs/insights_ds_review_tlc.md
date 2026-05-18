# Data Scientist Review: NYC TLC Insights (Jan-Feb 2026)

Reviewer: Data Scientist
Dataset: NYC TLC trip data, January 1 - February 28, 2026 (local Parquet)
Scope: Assessment of Headwater's current insight output against ground-truth signals in the TLC dataset.

## Data Reality

The dataset is NYC TLC trip data for January 1, 2026 through February 28, 2026. Locally it includes yellow, green, FHV, and high-volume FHV Parquet files: about 51.9M raw rows in period, with 51.45M valid trips after basic duration/distance filters.

Key observed facts:

- HVFHV dominates volume: about 40.8M valid trips, roughly 79% of valid trips.
- Yellow taxis: 6.72M valid trips.
- FHV: 3.87M valid trips, but 88.4% of pickup locations are null, so location insights must handle that explicitly.
- Green taxis: only 73.6K valid trips.
- Peak total volume is around 6 PM, then 5 PM, 7 PM, 8 PM.
- Weekday trips take longer than weekend trips: 19.87 min vs 17.62 min, practically meaningful at scale.
- PM peak trips, 3 PM to 6 PM, take 20.89 min vs 18.71 min for other hours.
- JFK and LaGuardia are the obvious long-duration hotspots:
  - JFK pickup avg 42.1 min, p90 68.2 min
  - LaGuardia pickup avg 32.3 min, p90 51.3 min
- Slow/high-volume corridors are mostly airport corridors, especially JFK <-> Manhattan and JFK/LaGuardia -> Outside NYC.

## Problems With Current Generated Insights

The current statistical insight engine is too generic for this dataset.

- `headwater/headwater/explorer/statistical.py:40` scans temporal and metric columns generically. It does not first infer "this is taxi trip lifecycle data."
- `headwater/headwater/explorer/statistical.py:274` loads full tables into Polars with `SELECT *`. That is risky for 50M+ taxi rows.
- `headwater/headwater/explorer/statistical.py:343` groups by raw temporal column. For taxi trips, raw pickup timestamps are near-row-level, so this misses the right buckets: hour, day, day-of-week, pickup zone, route.
- `headwater/headwater/explorer/suggestions.py:1` explicitly avoids domain knowledge. That is good for generality, but weak here: "trip_distance vs total_amount correlation" is less useful than "which pickup zones have long p90 trip time during PM peak?"
- `headwater/headwater/api/routes/explore.py:123` returns `"insights": []` from the suggestions endpoint, so the user sees suggested questions before actual statistical insights.

## Recommended Improvements

Implement a domain-aware insight layer before the generic statistical layer.

For taxi-like datasets, detect columns such as pickup/dropoff datetime, pickup/dropoff location ID, trip distance, fare, passenger count, trip time, request time, tips, driver pay. Then create canonical derived fields:

```
trip_date
pickup_hour
day_of_week
is_weekend
duration_min
speed_mph
wait_min
pickup_zone
dropoff_zone
pickup_borough
dropoff_borough
route_pair
service_type
```

Then generate insight families:

- **Coverage and period**: "Data covers Jan-Feb 2026 only; avoid long-term trend claims."
- **Volume**: trips per day, per hour, by service, by zone.
- **Peak time**: busiest hours and slowest hours, not just highest count.
- **Travel time**: median, p90, p95 by service, hour, pickup zone, route.
- **Congestion proxies**: low speed, high p90 duration, airport corridors.
- **Data quality**: bad duration, bad distance, null pickup/dropoff locations, impossible speeds.

Statistical guidance:

- Use quantiles, especially p90 and p95, because trip duration is skewed.
- Use multiple-testing control per insight family, not across unrelated tests only.
- Rank insights by practical impact: `effect_size x trip_count x confidence x actionability`.

## Example Better Insights

The system should produce insights like:

- "6 PM is the busiest pickup hour with 3.08M valid trips; PM peak trips are about 2.18 minutes longer than other hours."
- "Weekday trips are 2.25 minutes longer on average than weekend trips, with p90 38.4 vs 32.9 minutes."
- "JFK Airport pickups have the longest high-volume travel times: avg 42.1 min, p90 68.2 min."
- "HVFHV wait time is highest around 4 AM and 7 AM, with p90 wait near 12 minutes."
- "FHV location analysis is unreliable unless null pickup locations are handled, because 88.4% of FHV pickup zones are missing."

## Sources

- NYC TLC trip record page: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- HVFHS data dictionary: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf

The official notes confirm Jan/Feb 2026 files are monthly Parquet trip records and that TLC data includes pickup/dropoff times, locations, fares, distances, and HVFHV lifecycle fields.
