# Datasets & Data Dictionary

### NYC Taxi Data

The project integrates the following datasets, sourced from NYC’s Open Data portal:

- **Yellow Taxi Trips**:
  - Covers trips made by medallion taxis in all five boroughs.
  - Includes fields like pick-up/drop-off locations, timestamps, fares, surcharges, and passenger counts.

- **Green Taxi Trips**:
  - Covers trips made by green taxis, which operate primarily outside Manhattan’s central business district.
  - Includes similar fields as Yellow Taxi data.

- **HVFHV Trips**:
  - Covers trips from high-volume for-hire vehicle services like Uber, Lyft, and Via.
  - Includes shared ride information, driver payouts, and congestion surcharges.

## Yellow Taxi Trip Records

Yellow Trip Data represents the trip records of New York City's iconic Yellow Taxis, regulated by the New York City Taxi and Limousine Commission (TLC). These taxis are the only vehicles allowed to respond to street hails across all five boroughs of NYC.

Source: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

| Field Name              | Description                                                                                                                                                                           |
|-------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| VendorID               | A code indicating the TPEP provider that provided the record. <br> 1 = Creative Mobile Technologies, LLC <br> 2 = VeriFone Inc.                                                      |
| tpep_pickup_datetime   | The date and time when the meter was engaged.                                                                                                                                        |
| tpep_dropoff_datetime  | The date and time when the meter was disengaged.                                                                                                                                     |
| Passenger_count        | The number of passengers in the vehicle. This is a driver-entered value.                                                                                                            |
| Trip_distance          | The elapsed trip distance in miles reported by the taximeter.                                                                                                                       |
| PULocationID           | TLC Taxi Zone in which the taximeter was engaged.                                                                                                                                   |
| DOLocationID           | TLC Taxi Zone in which the taximeter was disengaged.                                                                                                                                |
| RateCodeID             | The final rate code in effect at the end of the trip. <br> 1 = Standard rate <br> 2 = JFK <br> 3 = Newark <br> 4 = Nassau or Westchester <br> 5 = Negotiated fare <br> 6 = Group ride |
| Store_and_fwd_flag     | This flag indicates whether the trip record was held in vehicle memory before sending to the vendor ("store and forward") because the vehicle did not have a connection to the server. <br> Y = Store and forward trip <br> N = Not a store and forward trip |
| Payment_type           | A numeric code signifying how the passenger paid for the trip. <br> 1 = Credit card <br> 2 = Cash <br> 3 = No charge <br> 4 = Dispute <br> 5 = Unknown <br> 6 = Voided trip          |
| Fare_amount            | The time-and-distance fare calculated by the meter.                                                                                                                                 |
| Extra                  | Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges.                                                                 |
| MTA_tax                | $0.50 MTA tax that is automatically triggered based on the metered rate in use.                                                                                                     |
| Improvement_surcharge  | $0.30 improvement surcharge assessed trips at the flag drop. The improvement surcharge began being levied in 2015.                                                                  |
| Tip_amount             | Tip amount – This field is automatically populated for credit card tips. Cash tips are not included.                                                                                |
| Tolls_amount           | Total amount of all tolls paid in trip.                                                                                                                                             |
| Total_amount           | The total amount charged to passengers. Does not include cash tips.                                                                                                                |
| Congestion_Surcharge   | Total amount collected in trip for NYS congestion surcharge.                                                                                                                       |
| Airport_fee            | $1.25 for pick up only at LaGuardia and John F. Kennedy Airports.                                                                                                                  |

## Green Taxi Trip Records

Green Trip Data refers to the trip records of New York City's green taxis, officially known as Street Hail Liveries (SHLs). Introduced in August 2013, these taxis were established to enhance transportation services in areas less frequented by the traditional yellow cabs. Green taxis are authorized to pick up passengers through street hails and prearranged rides in the boroughs outside of Manhattan's central business district and above West 110th Street and East 96th Street in Manhattan. 

Source: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf


| **Field Name**            | **Description**                                                                                                                                                     |
|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **VendorID**              | A code indicating the LPEP provider that provided the record. <br> 1 = Creative Mobile Technologies, LLC <br> 2 = VeriFone Inc.                                     |
| **lpep_pickup_datetime**  | The date and time when the meter was engaged.                                                                                                                       |
| **lpep_dropoff_datetime** | The date and time when the meter was disengaged.                                                                                                                    |
| **Passenger_count**       | The number of passengers in the vehicle. This is a driver-entered value.                                                                                            |
| **Trip_distance**         | The elapsed trip distance in miles reported by the taximeter.                                                                                                       |
| **PULocationID**          | TLC Taxi Zone in which the taximeter was engaged.                                                                                                                   |
| **DOLocationID**          | TLC Taxi Zone in which the taximeter was disengaged.                                                                                                                |
| **RateCodeID**            | The final rate code in effect at the end of the trip. <br> 1 = Standard rate <br> 2 = JFK <br> 3 = Newark <br> 4 = Nassau or Westchester <br> 5 = Negotiated fare <br> 6 = Group ride |
| **Store_and_fwd_flag**    | Indicates whether the trip record was held in vehicle memory before sending to the vendor ("store and forward") due to a lack of server connection. <br> Y = Store and forward trip <br> N = Not a store and forward trip |
| **Payment_type**          | A numeric code signifying how the passenger paid for the trip. <br> 1 = Credit card <br> 2 = Cash <br> 3 = No charge <br> 4 = Dispute <br> 5 = Unknown <br> 6 = Voided trip |
| **Fare_amount**           | The time-and-distance fare calculated by the meter.                                                                                                                 |
| **Extra**                 | Miscellaneous extras and surcharges. Currently, this includes the $0.50 and $1 rush hour and overnight charges.                                                     |
| **MTA_tax**               | $0.50 MTA tax that is automatically triggered based on the metered rate in use.                                                                                     |
| **Improvement_surcharge** | $0.30 improvement surcharge assessed on hailed trips at the flag drop. The improvement surcharge began being levied in 2015.                                        |
| **Tip_amount**            | Tip amount – This field is automatically populated for credit card tips. Cash tips are not included.                                                                |
| **Tolls_amount**          | Total amount of all tolls paid in trip.                                                                                                                             |
| **Total_amount**          | The total amount charged to passengers. Does not include cash tips.                                                                                                 |
| **Trip_type**             | A code indicating whether the trip was a street-hail or a dispatch, automatically assigned based on the metered rate in use but can be altered by the driver. <br> 1 = Street-hail <br> 2 = Dispatch |

## High Volume FHV Trip Records

This data dictionary describes High Volume FHV (For-Hire Vehicle) trip data. Each row represents a single trip in an FHV dispatched by one of NYC’s licensed High Volume FHV bases. On August 14, 2018, Mayor de Blasio signed Local Law 149 of 2018, creating a new license category for TLC-licensed FHV businesses that dispatch more than 10,000 FHV trips daily under a single brand, referred to as High-Volume For-Hire Services (HVFHS). This law went into effect on February 1, 2019.

Source: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf


| **Field Name**           | **Description**                                                                                                                                                             |
|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Hvfhs_license_num**    | The TLC license number of the HVFHS base or business. <br> As of September 2019: <br> HV0002: Juno <br> HV0003: Uber <br> HV0004: Via <br> HV0005: Lyft                     |
| **Dispatching_base_num** | The TLC Base License Number of the base that dispatched the trip.                                                                                                          |
| **Pickup_datetime**      | The date and time of the trip pick-up.                                                                                                                                    |
| **DropOff_datetime**     | The date and time of the trip drop-off.                                                                                                                                   |
| **PULocationID**         | TLC Taxi Zone in which the trip began.                                                                                                                                    |
| **DOLocationID**         | TLC Taxi Zone in which the trip ended.                                                                                                                                    |
| **originating_base_num** | The base number of the base that received the original trip request.                                                                                                      |
| **request_datetime**     | The date and time when the passenger requested to be picked up.                                                                                                           |
| **on_scene_datetime**    | The date and time when the driver arrived at the pick-up location (Accessible Vehicles-only).                                                                             |
| **trip_miles**           | Total miles for the passenger trip.                                                                                                                                       |
| **trip_time**            | Total time in seconds for the passenger trip.                                                                                                                             |
| **base_passenger_fare**  | Base passenger fare before tolls, tips, taxes, and fees.                                                                                                                  |
| **tolls**                | Total amount of all tolls paid in the trip.                                                                                                                               |
| **bcf**                  | Total amount collected in the trip for the Black Car Fund.                                                                                                               |
| **sales_tax**            | Total amount collected in the trip for NYS sales tax.                                                                                                                     |
| **congestion_surcharge** | Total amount collected in the trip for NYS congestion surcharge.                                                                                                          |
| **airport_fee**          | $2.50 for both drop-off and pick-up at LaGuardia, Newark, and John F. Kennedy airports.                                                                                   |
| **tips**                 | Total amount of tips received from the passenger.                                                                                                                         |
| **driver_pay**           | Total driver pay (not including tolls or tips and net of commission, surcharges, or taxes).                                                                               |
| **shared_request_flag**  | Indicates if the passenger agreed to a shared/pooled ride, regardless of whether they were matched. (Y/N)                                                                 |
| **shared_match_flag**    | Indicates if the passenger shared the vehicle with another passenger who booked separately at any point during the trip. (Y/N)                                            |
| **access_a_ride_flag**   | Indicates if the trip was administered on behalf of the Metropolitan Transportation Authority (MTA). (Y/N)                                                                |
| **wav_request_flag**     | Indicates if the passenger requested a wheelchair-accessible vehicle (WAV). (Y/N)                                                                                        |
| **wav_match_flag**       | Indicates if the trip occurred in a wheelchair-accessible vehicle (WAV). (Y/N)                                                                                           |


