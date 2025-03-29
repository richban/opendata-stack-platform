# NYC Taxi Data Analytics Marts

This directory contains analytical data marts built from the NYC Taxi & FHV data warehouse. These marts are designed to address specific business questions and provide insights into various aspects of the taxi service operations.

## Overview

The marts are organized into business domains to facilitate targeted analytics:

```
marts/
├── financial/          # Revenue, payment, and financial metrics
├── operational/        # Trip performance and operational efficiency
├── geographic/         # Zone and location-based analysis
├── temporal/           # Time patterns and seasonality
└── service/            # Service quality and customer experience
```

## Business Metrics & Use Cases

### Financial Analysis

**Key Mart: `mart_revenue_analysis`**

**Business Questions Addressed:**
- What are the total revenue trends over time?
- How does revenue break down by vendor, borough, payment type?
- What's the contribution of different fee types to overall revenue?
- How do surge pricing and peak hours affect revenue?

**Key Metrics:**
- Total fare revenue
- Average fare per trip
- Percentage contribution of surcharges and fees
- Payment type distribution (cash vs. credit)
- Tip percentage analysis

**Business Value:**
- Identify revenue growth opportunities
- Understand fee structure impact
- Optimize pricing strategies
- Monitor payment method trends

### Operational Performance

**Key Mart: `mart_trip_performance`**

**Business Questions Addressed:**
- What are average trip distances and durations by various dimensions?
- How efficient are trips in different boroughs and time periods?
- Which areas have the highest/lowest utilization rates?
- What are the busiest pickup/dropoff combinations?

**Key Metrics:**
- Trip distance and duration
- Speed metrics (miles per hour)
- Time efficiency (revenue per hour)
- Utilization rates by location and time

**Business Value:**
- Identify operational inefficiencies
- Optimize driver deployment
- Improve service coverage
- Enhance fleet utilization

### Geographic Analysis

> Given that taxi patterns typically follow weekly cycles with time-of-day variations, this granularity seems reasonable, but adding the specific date as an additional dimension would provide more analytical flexibility without significantly increasing the mart size.

**Key Mart: `mart_zone_analysis` granularity** 

- `year_number` (2023)
- `month_name` (January)
- `day_name` (Monday)
- `period_of_da`y` (Morning Rush)

This granularity represents a **pattern-focused approach** rather than a specific date approach. The data is aggregated to show recurring patterns like "Monday mornings in January 2023" rather than specific dates like "January 16, 2023." This **cyclical nature** means that data aggregated by day of week and time of day effectively captures the recurring transportation patterns that transportation planners, taxi companies, and ride-sharing services need to understand.

**Business Questions Addressed:**
- Which zones generate the most trips and revenue?
- How do trip patterns differ across boroughs?
- What are the most common pickup-dropoff zone pairs?
- Which areas are underserved or overserved?

**Key Metrics:**
- Trip counts and revenue by zone
- Zone-to-zone trip patterns
- Zone performance metrics (revenue per trip, avg distance)

**Business Value:**
- Target high-demand areas
- Identify service gaps
- Optimize coverage strategy
- Plan infrastructure improvements

#### Example Row from the Zone Analysis Mart

```py
# Zone Identifiers
pickup_location_id: 43
pickup_zone: "Upper East Side"
pickup_borough: "Manhattan"
dropoff_location_id: 236
dropoff_zone: "Midtown"
dropoff_borough: "Manhattan"

# Time Dimensions
year_number: 2023
month_name: "January"
day_name: "Monday"
period_of_day: "Morning Rush"
taxi_type: "Yellow"

# Pickup Zone Metrics (Upper East Side overall metrics)
total_pickups: 3,500           # Total pickups from Upper East Side during this time period
total_pickup_revenue: $52,500   # Total revenue from all pickups in Upper East Side
avg_pickup_distance: 2.8 miles  # Average distance of all trips starting in Upper East Side
avg_pickup_duration: 16 min     # Average duration of all trips starting in Upper East Side
total_pickup_passengers: 4,200   # Total passengers picked up from Upper East Side

# Dropoff Zone Metrics (Midtown overall metrics)
total_dropoffs: 5,600           # Total dropoffs to Midtown during this time period
total_dropoff_revenue: $78,400   # Total revenue from all dropoffs to Midtown
avg_dropoff_distance: 3.1 miles  # Average distance of all trips ending in Midtown
avg_dropoff_duration: 19 min     # Average duration of all trips ending in Midtown
total_dropoff_passengers: 6,720   # Total passengers dropped off in Midtown

# Zone Pair Metrics (Upper East Side to Midtown specifically)
pair_trip_count: 780            # Number of trips from Upper East Side to Midtown
pair_revenue: $9,360            # Revenue from Upper East Side to Midtown trips
pair_avg_distance: 2.2 miles    # Average distance for this specific route
pair_avg_duration: 14 min       # Average duration for this specific route
pair_total_passengers: 936      # Total passengers for this specific route
percentage_of_all_trips: 1.8%   # This route represents 1.8% of all taxi trips

# Calculated Metrics
pickup_to_dropoff_ratio: 0.63   # Upper East Side has fewer pickups than Midtown has dropoffs
revenue_per_pickup: $15         # Average revenue per pickup in Upper East Side
avg_pickup_speed_mph: 10.5      # Average speed of trips from Upper East Side
```

##### Insights from This Single Row:

1. **Commuting Pattern**: This shows a clear commuting pattern - many people are taking taxis from Upper East Side (residential) to Midtown (business district) during morning rush hour.
2. **Supply/Demand Imbalance**: The `pickup_to_dropoff_ratio` of 0.63 indicates Midtown is receiving more dropoffs than Upper East Side has pickups, suggesting taxis may need to relocate after dropping off in Midtown.
3. **Route Efficiency**: The specific Upper East Side to Midtown route (2.2 miles) is shorter than the average trip from Upper East Side (2.8 miles), suggesting this is a direct, efficient route.
4. **Route Popularity**: With 780 trips in this time period representing 1.8% of all trips, this is clearly a popular route.
5. **Revenue Opportunity**: This single route generates over $9,300 in revenue during this time period.
6. **Traffic Conditions**: Average speed of 10.5 mph indicates moderate traffic congestion.
7. **Occupancy Rate**: The average occupancy is 1.2 passengers per trip (936/780), slightly below the zone average of 1.2 passengers (4200/3500).


### Temporal Patterns

**Key Mart: `mart_time_patterns`**

**Business Questions Addressed:**
- How does trip volume vary by time of day, day of week, and season?
- What are the peak demand hours and how do they vary by borough?
- How do fare amounts and trip patterns change during different time periods?
- What seasonal trends exist in taxi usage?

**Key Metrics:**
- Hourly, daily, and monthly trip patterns
- Peak period analysis
- Time-based performance metrics
- Seasonal trend indicators

**Business Value:**
- Optimize driver scheduling
- Plan for peak demand
- Implement time-based pricing
- Anticipate seasonal fluctuations

### Service Quality

**Key Mart: `mart_service_quality`**

**Business Questions Addressed:**
- How effectively are vendors serving accessibility (WAV) requests?
- What is the distribution of shared rides and their efficiency?
- How does service quality vary by borough, time period, and vendor?
- What are the key indicators of customer satisfaction?

**Key Metrics:**
- WAV request fulfillment rates
- Shared ride percentages and efficiency
- Tip percentages as a proxy for customer satisfaction
- Service response efficiency

**Business Value:**
- Improve accessibility compliance
- Enhance customer satisfaction
- Optimize shared ride offerings
- Benchmark vendor performance

## Usage Examples

These marts can be used to build dashboards, reports, and ad-hoc analyses that provide insights into the NYC taxi operations. Example use cases include:

1. **Executive Dashboard:** High-level KPIs showing revenue trends, trip volumes, and operational efficiency metrics.

2. **Operational Reports:** Daily/weekly reports on service performance, zone coverage, and vendor metrics.

3. **Financial Analysis:** Revenue breakdowns, payment trends, and pricing optimization opportunities.

4. **Service Planning:** Identifying peak demand periods and locations to optimize driver deployment.

5. **Quality Monitoring:** Tracking service quality metrics and accessibility compliance.

## Data Sources

These marts are built from the `fact_taxi_trip` fact table and related dimension tables. The underlying data includes:
- Yellow Taxi trip data
- Green Taxi trip data 
- High-Volume For-Hire Vehicle (HVFHV) trip data

## Future Enhancements

Possible future enhancements to these marts include:
- Driver-specific performance metrics
- Weather impact analysis
- Surge pricing effectiveness
- Customer segmentation
- Predictive demand modeling
