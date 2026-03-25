
from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp

from pyspark.sql.functions import (
    col,
    explode_outer,
    upper,
    trim,
    current_timestamp,
    regexp_like,
    current_timestamp,
    to_timestamp,
    expr,
    concat_ws,
)
import sys
import os


if "__file__" in globals():
    current_dir = os.path.dirname(os.path.abspath(__file__))
else:
    current_dir = os.getcwd()

src_root = os.path.abspath(os.path.join(current_dir, '..'))

if src_root not in sys.path:
    sys.path.append(src_root)


from extract_data.config import config



#corrected
#not tested
@dp.table(
    name=config.silver_table_cities,
    comment="Cleaned city master data",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_city_code", "city_code IS NOT NULL AND trim(city_code) <> ''")
@dp.expect_or_drop("valid_country_code", "country_code IS NOT NULL")
@dp.expect("valid_time_zone_id", "time_zone_id IS NOT NULL")
@dp.expect("valid_utc_offset_format", "utc_offset RLIKE '^[+-][0-9]{2}:[0-9]{2}$'")
def cities_silver():
    return (
        spark.readStream.table("bronze_table_cities")
        .select(
            explode_outer(col("CityResource.Cities.City")).alias("city"),
            col("_source_file"),
            col("_ingested_at"),
        )
        .select(
            upper(trim(col("city.CityCode"))).alias("city_code"),
            upper(trim(col("city.CountryCode"))).alias("country_code"),
            trim(col("city.Names.Name")).alias("city_name"),
            trim(col("city.TimeZoneId")).alias("time_zone_id"),
            trim(col("city.UtcOffset")).alias("utc_offset"),
            col("city.Airports").alias("airports"),
            col("_source_file"),
            col("_ingested_at"),
            current_timestamp().alias("silver_processed_at"),
        )
        .dropDuplicates(["city_code"])
    )

#    read bronze
#→ explode nested records
#→ standardize keys with upper(trim(...))
#→ rename columns clearly
#→ apply expectations
#→ deduplicate

@dp.table(
    name=config.silver_table_airlines,
    comment="Cleaned airline master data",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_airline_id", "airline_id IS NOT NULL AND trim(airline_id) <> ''")
@dp.expect("valid_airline_id_icao", "airline_id_icao IS NOT NULL")
def airlines_silver():
    return (
        spark.readStream.table("bronze_table_airlines")
        .select(
            explode_outer(col("AirlineResource.Airlines.Airline")).alias("airline"),
            col("_source_file"),
            col("_ingested_at"),
        )
        .select(
            upper(trim(col("airline.AirlineID"))).alias("airline_id"),
            upper(trim(col("airline.AirlineID_ICAO"))).alias("airline_id_icao"),
            col("airline.Names.Name").alias("raw_name"),
            col("_source_file"),
            col("_ingested_at"),
            current_timestamp().alias("silver_processed_at"),
        )
        .dropDuplicates(["airline_id"])
    )


#for flights and delys FlightStatusResource: 
#use UTC time stemps


@dp.table(
    name=config.silver_table_dep_a,
    comment="Cleaned FRA departure flight status data for downstream delay analysis",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop(
    "valid_departure_airport_code",
    "departure_airport_code IS NOT NULL AND trim(departure_airport_code) <> ''"
)
@dp.expect_or_drop(
    "valid_arrival_airport_code",
    "arrival_airport_code IS NOT NULL AND trim(arrival_airport_code) <> ''"
)
@dp.expect_or_drop(
    "valid_marketing_airline_id",
    "marketing_airline_id IS NOT NULL AND trim(marketing_airline_id) <> ''"
)
@dp.expect_or_drop(
    "valid_marketing_flight_number",
    "marketing_flight_number IS NOT NULL AND trim(marketing_flight_number) <> ''"
)
@dp.expect_or_drop(
    "valid_dep_sched_utc_ts",
    "dep_sched_utc_ts IS NOT NULL"
)
def departures_fra_silver():
    flights = (
        spark.readStream.table(config.bronze_table_dep_a)
        .select(
            explode_outer(col("FlightStatusResource.Flights.Flight")).alias("flight"),
            col("_source_file"),
            col("_ingested_at"),
        )
    )

    silver_df = (
        flights
        .select(
            upper(trim(col("flight.Departure.AirportCode"))).alias("departure_airport_code"),
            upper(trim(col("flight.Arrival.AirportCode"))).alias("arrival_airport_code"),

            upper(trim(col("flight.MarketingCarrier.AirlineID"))).alias("marketing_airline_id"),
            trim(col("flight.MarketingCarrier.FlightNumber")).alias("marketing_flight_number"),

            upper(trim(col("flight.OperatingCarrier.AirlineID"))).alias("operating_airline_id"),
            trim(col("flight.OperatingCarrier.FlightNumber")).alias("operating_flight_number"),

            upper(trim(col("flight.ServiceType"))).alias("service_type"),

            upper(trim(col("flight.Equipment.AircraftCode"))).alias("aircraft_code"),
            upper(trim(col("flight.Equipment.AircraftRegistration"))).alias("aircraft_registration"),

            trim(col("flight.Departure.Terminal.Name")).alias("dep_terminal_name"),
            trim(col("flight.Departure.Terminal.Gate")).alias("dep_gate"),
            trim(col("flight.Arrival.Terminal.Name")).alias("arr_terminal_name"),
            trim(col("flight.Arrival.Terminal.Gate")).alias("arr_gate"),

            trim(col("flight.Departure.TimeStatus.Code")).alias("dep_time_status_code"),
            trim(col("flight.Departure.TimeStatus.Definition")).alias("dep_time_status_definition"),
            trim(col("flight.Arrival.TimeStatus.Code")).alias("arr_time_status_code"),
            trim(col("flight.Arrival.TimeStatus.Definition")).alias("arr_time_status_definition"),

            trim(col("flight.FlightStatus.Code")).alias("flight_status_code"),
            trim(col("flight.FlightStatus.Definition")).alias("flight_status_definition"),

            to_timestamp(col("flight.Departure.ScheduledTimeLocal.DateTime")).alias("dep_sched_local_ts"),
            to_timestamp(col("flight.Departure.ScheduledTimeUTC.DateTime")).alias("dep_sched_utc_ts"),
            to_timestamp(col("flight.Departure.EstimatedTimeLocal.DateTime")).alias("dep_est_local_ts"),
            to_timestamp(col("flight.Departure.EstimatedTimeUTC.DateTime")).alias("dep_est_utc_ts"),
            to_timestamp(col("flight.Departure.ActualTimeLocal.DateTime")).alias("dep_actual_local_ts"),
            to_timestamp(col("flight.Departure.ActualTimeUTC.DateTime")).alias("dep_actual_utc_ts"),

            to_timestamp(col("flight.Arrival.ScheduledTimeLocal.DateTime")).alias("arr_sched_local_ts"),
            to_timestamp(col("flight.Arrival.ScheduledTimeUTC.DateTime")).alias("arr_sched_utc_ts"),
            to_timestamp(col("flight.Arrival.EstimatedTimeLocal.DateTime")).alias("arr_est_local_ts"),
            to_timestamp(col("flight.Arrival.EstimatedTimeUTC.DateTime")).alias("arr_est_utc_ts"),
            to_timestamp(col("flight.Arrival.ActualTimeLocal.DateTime")).alias("arr_actual_local_ts"),
            to_timestamp(col("flight.Arrival.ActualTimeUTC.DateTime")).alias("arr_actual_utc_ts"),

            col("_source_file"),
            col("_ingested_at"),
            current_timestamp().alias("silver_processed_at"),
        )
        .withColumn(
            "flight_occurrence_key",
            concat_ws(
                "_",
                col("departure_airport_code"),
                col("marketing_airline_id"),
                col("marketing_flight_number"),
                col("dep_sched_utc_ts").cast("string"),
            )
        )
        .withColumn(
            "dep_delay_minutes_actual",
            expr("timestampdiff(MINUTE, dep_sched_utc_ts, dep_actual_utc_ts)")
        )
        .withColumn(
            "dep_delay_minutes_estimated",
            expr("timestampdiff(MINUTE, dep_sched_utc_ts, dep_est_utc_ts)")
        )
        .withColumn(
            "arr_delay_minutes_actual",
            expr("timestampdiff(MINUTE, arr_sched_utc_ts, arr_actual_utc_ts)")
        )
        .withColumn(
            "arr_delay_minutes_estimated",
            expr("timestampdiff(MINUTE, arr_sched_utc_ts, arr_est_utc_ts)")
        )
        .dropDuplicates(["flight_occurrence_key"])
    )

    return silver_df