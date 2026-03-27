from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    map_values,
    explode,
    explode_outer,
    upper,
    trim,
    current_timestamp,
    to_timestamp,
    expr,
    concat_ws,
    when,
    lit,
    coalesce
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

extract_data_root = os.path.join(src_root, 'extract_data')
if extract_data_root not in sys.path:
    sys.path.append(extract_data_root)

from extract_data.config import config



#loop different airports
@dp.table(
    name=config.silver_table_dep_a,
    comment=f"Cleaned departure flight status data for delay analysis",
    table_properties={"quality": "silver"},
)
def departures_fra_silver():
    df = spark.readStream.table(f"{config.catalog_name}.{config.schema_name}.{config.bronze_table_dep_a}")
    
    df.printSchema()

    df = df.select(
        explode_outer(col("FlightStatusResource.Flights.Flight")).alias("flight"),
        col("_source_file"),
        col("_ingested_at"),
    )
    
    df = df.withColumn(
        "departure_airport_code",
        upper(trim(col("flight.Departure.AirportCode")))
    ).withColumn(
        "arrival_airport_code",
        upper(trim(col("flight.Arrival.AirportCode")))
    ).withColumn(
        "marketing_airline_id",
        upper(trim(col("flight.MarketingCarrier.AirlineID")))
    ).withColumn(
        "marketing_flight_number",
        trim(col("flight.MarketingCarrier.FlightNumber"))
    ).withColumn(
        "operating_airline_id",
        upper(trim(col("flight.OperatingCarrier.AirlineID")))
    ).withColumn(
        "operating_flight_number",
        trim(col("flight.OperatingCarrier.FlightNumber"))
    ).withColumn(
        "service_type",
        upper(trim(col("flight.ServiceType")))
    ).withColumn(
        "aircraft_code",
        upper(trim(col("flight.Equipment.AircraftCode")))
    ).withColumn(
        "aircraft_registration",
        upper(trim(col("flight.Equipment.AircraftRegistration")))
    )
    
    df = df.withColumn(
        "dep_terminal_name",
        trim(col("flight.Departure.Terminal.Name"))
    ).withColumn(
        "dep_gate",
        trim(col("flight.Departure.Terminal.Gate"))
    ).withColumn(
        "arr_terminal_name",
        trim(col("flight.Arrival.Terminal.Name"))
    ).withColumn(
        "arr_gate",
        trim(col("flight.Arrival.Terminal.Gate"))
    )
    
    df = df.withColumn(
        "dep_time_status_code",
        trim(col("flight.Departure.TimeStatus.Code"))
    ).withColumn(
        "dep_time_status_definition",
        trim(col("flight.Departure.TimeStatus.Definition"))
    ).withColumn(
        "arr_time_status_code",
        trim(col("flight.Arrival.TimeStatus.Code"))
    ).withColumn(
        "arr_time_status_definition",
        trim(col("flight.Arrival.TimeStatus.Definition"))
    ).withColumn(
        "flight_status_code",
        trim(col("flight.FlightStatus.Code"))
    ).withColumn(
        "flight_status_definition",
        trim(col("flight.FlightStatus.Definition"))
    )
    
    df = df.withColumn(
        "dep_sched_local_ts",
        to_timestamp(col("flight.Departure.ScheduledTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm")
    ).withColumn(
        "dep_sched_utc_ts",
        to_timestamp(col("flight.Departure.ScheduledTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mmX")
    ).withColumn(
        "dep_est_local_ts",
        to_timestamp(col("flight.Departure.EstimatedTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm")
    ).withColumn(
        "dep_est_utc_ts",
        to_timestamp(col("flight.Departure.EstimatedTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mmX")
    ).withColumn(
        "dep_actual_local_ts",
        to_timestamp(col("flight.Departure.ActualTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm")
    ).withColumn(
        "dep_actual_utc_ts",
        to_timestamp(col("flight.Departure.ActualTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mmX")
    ).withColumn(
        "arr_sched_local_ts",
        to_timestamp(col("flight.Arrival.ScheduledTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm")
    ).withColumn(
        "arr_sched_utc_ts",
        to_timestamp(col("flight.Arrival.ScheduledTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mmX")
    ).withColumn(
        "arr_est_local_ts",
        to_timestamp(col("flight.Arrival.EstimatedTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm")
    ).withColumn(
        "arr_est_utc_ts",
        to_timestamp(col("flight.Arrival.EstimatedTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mmX")
    ).withColumn(
        "arr_actual_local_ts",
        to_timestamp(col("flight.Arrival.ActualTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm")
    ).withColumn(
        "arr_actual_utc_ts",
        to_timestamp(col("flight.Arrival.ActualTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mmX")
    )
    
    df = df.withColumn(
        "silver_processed_at",
        current_timestamp()
    ).withColumn(
        "flight_occurrence_key",
        concat_ws(
            "_",
            col("departure_airport_code"),
            col("marketing_airline_id"),
            col("marketing_flight_number"),
            col("dep_sched_utc_ts").cast("string"),
        )
    ).withColumn(
        "dep_delay_minutes_actual",
        expr("timestampdiff(MINUTE, dep_sched_utc_ts, dep_actual_utc_ts)")
    ).withColumn(
        "dep_delay_minutes_estimated",
        expr("timestampdiff(MINUTE, dep_sched_utc_ts, dep_est_utc_ts)")
    ).withColumn(
        "arr_delay_minutes_actual",
        expr("timestampdiff(MINUTE, arr_sched_utc_ts, arr_actual_utc_ts)")
    ).withColumn(
        "arr_delay_minutes_estimated",
        expr("timestampdiff(MINUTE, arr_sched_utc_ts, arr_est_utc_ts)")
    )
    
    # Filter for valid records only - RELAXED filter
    # df = df.filter(
    #     (col("departure_airport_code").isNotNull()) & 
    #     (trim(col("departure_airport_code")) != "") &
    #     (col("arrival_airport_code").isNotNull()) & 
    #     (trim(col("arrival_airport_code")) != "") &
    #     (col("marketing_airline_id").isNotNull()) & 
    #     (trim(col("marketing_airline_id")) != "") &
    #     (col("marketing_flight_number").isNotNull()) & 
    #     (trim(col("marketing_flight_number")) != "") &
    #     (col("dep_sched_utc_ts").isNotNull())
    # )
    
    final_columns = [
        "flight_occurrence_key",
        "departure_airport_code",
        "arrival_airport_code",
        "marketing_airline_id",
        "marketing_flight_number",
        "operating_airline_id",
        "operating_flight_number",
        "service_type",
        "aircraft_code",
        "aircraft_registration",
        "dep_terminal_name",
        "dep_gate",
        "arr_terminal_name",
        "arr_gate",
        "dep_time_status_code",
        "dep_time_status_definition",
        "arr_time_status_code",
        "arr_time_status_definition",
        "flight_status_code",
        "flight_status_definition",
        "dep_sched_local_ts",
        "dep_sched_utc_ts",
        "dep_est_local_ts",
        "dep_est_utc_ts",
        "dep_actual_local_ts",
        "dep_actual_utc_ts",
        "arr_sched_local_ts",
        "arr_sched_utc_ts",
        "arr_est_local_ts",
        "arr_est_utc_ts",
        "arr_actual_local_ts",
        "arr_actual_utc_ts",
        "dep_delay_minutes_actual",
        "dep_delay_minutes_estimated",
        "arr_delay_minutes_actual",
        "arr_delay_minutes_estimated",
        "_source_file",
        "_ingested_at",
        "silver_processed_at",
    ]
    
    return df.select(*final_columns)


@dp.table(
    name="quarantine_departures_fra",
    comment="Quarantine table for FRA departure records failing data quality checks",
    table_properties={"quality": "quarantine"},
)
def departures_fra_quarantine():
    """Captures departure records that fail validation checks"""
    df = spark.readStream.table(f"{config.catalog_name}.{config.schema_name}.{config.bronze_table_dep_a}")
    
    # df = df.withColumn(
    #     "flight",
    #     explode_outer(col("FlightStatusResource.Flights.Flight"))
    # )
    df = df.select(
        explode_outer(col("FlightStatusResource.Flights.Flight")).alias("flight"),
        col("_source_file"),
        col("_ingested_at"),
    )


    
    df = df.withColumn(
        "departure_airport_code",
        upper(trim(col("flight.Departure.AirportCode")))
    ).withColumn(
        "arrival_airport_code",
        upper(trim(col("flight.Arrival.AirportCode")))
    ).withColumn(
        "marketing_airline_id",
        upper(trim(col("flight.MarketingCarrier.AirlineID")))
    ).withColumn(
        "marketing_flight_number",
        trim(col("flight.MarketingCarrier.FlightNumber"))
    ).withColumn(
        "dep_sched_utc_ts",
        to_timestamp(col("flight.Departure.ScheduledTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mmX")
    ).withColumn(
        "dep_sched_local_ts",
        to_timestamp(col("flight.Departure.ScheduledTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm")
    ).withColumn(
        "dep_est_utc_ts",
        to_timestamp(col("flight.Departure.EstimatedTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mmX")
    ).withColumn(
        "dep_est_local_ts",
        to_timestamp(col("flight.Departure.EstimatedTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm")
    ).withColumn(
        "arr_sched_utc_ts",
        to_timestamp(col("flight.Arrival.ScheduledTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mmX")
    ).withColumn(
        "arr_sched_local_ts",
        to_timestamp(col("flight.Arrival.ScheduledTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm")
    ).withColumn(
        "arr_est_utc_ts",
        to_timestamp(col("flight.Arrival.EstimatedTimeUTC.DateTime"), "yyyy-MM-dd'T'HH:mmX")
    ).withColumn(
        "arr_est_local_ts",
        to_timestamp(col("flight.Arrival.EstimatedTimeLocal.DateTime"), "yyyy-MM-dd'T'HH:mm")
    )
    
    # df.cache()
    # record_count = df.count()
    # print(f"Records before filter: {record_count}")
    # Filter for invalid records
    # df = df.filter(
    #     (col("departure_airport_code").isNull() | (trim(col("departure_airport_code")) == "")) |
    #     (col("arrival_airport_code").isNull() | (trim(col("arrival_airport_code")) == "")) |
    #     (col("marketing_airline_id").isNull() | (trim(col("marketing_airline_id")) == "") |
    #     (col("marketing_flight_number").isNull() | (trim(col("marketing_flight_number")) == "") |
    #     (col("dep_sched_utc_ts").isNull()))
    # )
    
    df = df.withColumn(
        "quarantine_reason",
        when(col("departure_airport_code").isNull() | (trim(col("departure_airport_code")) == ""), "Invalid departure_airport_code")
        .when(col("arrival_airport_code").isNull() | (trim(col("arrival_airport_code")) == ""), "Invalid arrival_airport_code")
        .when(col("marketing_airline_id").isNull() | (trim(col("marketing_airline_id")) == ""), "Invalid marketing_airline_id")
        .when(col("marketing_flight_number").isNull() | (trim(col("marketing_flight_number")) == ""), "Invalid marketing_flight_number")
        .when(col("dep_sched_utc_ts").isNull(), "Invalid dep_sched_utc_ts")
        .otherwise("Unknown validation error")
    ).withColumn(
        "quarantine_timestamp",
        current_timestamp()
    )
    
    final_columns = [
        "departure_airport_code",
        "arrival_airport_code",
        "marketing_airline_id",
        "marketing_flight_number",
        "dep_sched_utc_ts",
        "quarantine_reason",
        "_source_file",
        "_ingested_at",
        "quarantine_timestamp",
    ]
    
    return df.select(*final_columns)



# df.cache()
# record_count = df.count()
# print(f"Records before filter: {record_count}")

# df = df.filter((col("flight").isNotNull()))  # First ensure flight object exists

# df_test = df.select("dep_sched_utc_ts").show()  # Check if parsing worked