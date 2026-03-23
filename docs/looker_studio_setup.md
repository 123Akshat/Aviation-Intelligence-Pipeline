# Looker Studio Dashboard Setup Guide

## Step 1 — Connect to BigQuery

1. Go to https://lookerstudio.google.com
2. Click "Create" → "Report" → "Add Data"
3. Select BigQuery connector
4. Choose: Project = skylens-india | Dataset = skylens_dbt_marts
5. Add these tables: mart_airline_performance, mart_route_fares, mart_price_analysis

## Step 2 — Recommended Pages & Charts

### Page 1: Airline Overview
- Scorecard: Total Flights (COUNT from mart_airline_performance)
- Scorecard: Average Price INR (AVG price_inr)
- Bar chart: avg_price_inr by airline_clean (sorted descending)
- Bar chart: nonstop_pct by airline_clean
- Table: airline_clean | total_flights | avg_price_inr | nonstop_pct | avg_duration_min

### Page 2: Route Analysis
- Bar chart: Top 10 routes by flight_count (mart_route_fares)
- Line chart: avg_price_inr by journey_month, broken down by route_label
- Geo map: source_city to destination_city, sized by avg_price_inr

### Page 3: Price Intelligence
- Bar chart: avg_price_inr by stop_category (mart_price_analysis)
- Bar chart: avg_price_inr by departure_slot
- Scorecard: avg nonstop_premium_pct (how much extra you pay for non-stop)
- Table: route_label | stop_category | avg_price_inr | nonstop_premium_pct

### Page 4: Monthly Trends
- Line chart: flight_count by journey_month, coloured by airline_clean
- Bar chart: avg_price_inr by journey_month
- Heatmap: journey_month vs airline_clean, metric = avg_price_inr

## Step 3 — Add Filters (top of every page)
- Airline dropdown: filter on airline_clean
- Source city: filter on source_city
- Stop category: filter on stop_category
- Month: filter on journey_month
