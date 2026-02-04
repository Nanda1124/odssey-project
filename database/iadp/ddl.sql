CREATE SCHEMA IF NOT EXISTS iadp;

CREATE TABLE IF NOT EXISTS iadp.dim_routes (
    dim_route_id STRING,
    route_id INT,
    route_number INT,
    route_name STRING,
    route_type INT,
    route_color STRING,
    route_color_code STRING,
    route_sort_order INT,
    agency_name STRING,
    agency_url STRING,
    agency_timezone STRING,
    agency_lang STRING,
    agency_phone STRING,
    agency_id INT,
    loaded_date DATETIME
);

CREATE TABLE IF NOT EXISTS iadp.fact_trips (
    fact_trips_id STRING,
    trip_id INTEGER,
    route_id INTEGER,
    service_id INTEGER,
    direction_id INTEGER,
    shape_id INTEGER,
    trip_headsign STRING,
    block_id STRING,
    loaded_date DATETIME
);

CREATE TABLE IF NOT EXISTS iadp.dim_calendar (
    dim_calendar_id STRING,
    service_id INT,
    monday INT,
    tuesday INT,
    wednesday INT,
    thursday INT,
    friday INT,
    saturday INT,
    sunday INT,
    start_date DATE,
    end_date DATE,
    exception_date DATE,
    exception_type INT,
    loaded_date DATETIME
);

CREATE TABLE IF NOT EXISTS iadp.dim_stops (
    dim_stops_id STRING,
    stop_id INT,
    stop_name STRING,
    stop_lat STRING,
    stop_lon STRING,
    trip_id INT,
    arrival_time STRING,
    departure_time STRING,
    stop_sequence INT,
    time_point INT,
    loaded_date DATETIME
);
