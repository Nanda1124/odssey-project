-- Creating schema and sadp tables

-- Create schema
CREATE SCHEMA IF NOT EXISTS sadp;

-- creating table 'trips', 'routes', 'agency', 'calendar', 'calendar_dates', 'stops', 'stop_times'
CREATE TABLE IF NOT EXISTS sadp.trips (
    id INTEGER PRIMARY KEY,
    route_id INTEGER NOT NULL,
    service_id INTEGER NOT NULL,
    trip_id INTEGER NOT NULL,
    direction_id INTEGER,
    shape_id INTEGER
)


CREATE TABLE IF NOT EXISTS sadp.routes (
    id INTEGER PRIMARY KEY,
    route_id INTEGER NOT NULL,
    route_number INTEGER,
    route_name VARCHAR NOT NULL,
    route_type INTEGER,
    route_color VARCHAR,
    route_color_code VARCHAR,
    route_sort_order INTEGER,
    agency_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sadp.agency (
    id INTEGER PRIMARY KEY,
    agency_name VARCHAR NOT NULL,
    agency_url VARCHAR NOT NULL,
    agency_timezone VARCHAR,
    agency_lang VARCHAR,
    agency_phone VARCHAR,
    agency_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sadp.calendar (
    id INTEGER PRIMARY KEY,
    service_id INTEGER NOT NULL,
    monday INTEGER NOT NULL,
    tuesday INTEGER NOT NULL,
    wednesday INTEGER NOT NULL,
    thursday INTEGER NOT NULL,
    friday INTEGER NOT NULL,
    saturday INTEGER NOT NULL,
    sunday INTEGER NOT NULL,
    start_date VARCHAR NOT NULL,
    end_date VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS sadp.calendar_dates (
    id INTEGER PRIMARY KEY,
    service_id INTEGER NOT NULL,
    exception_date VARCHAR NOT NULL,
    exception_type INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sadp.stops (
    id INTEGER PRIMARY KEY,
    stop_id INTEGER NOT NULL,
    stop_name VARCHAR NOT NULL,
    stop_lat VARCHAR NOT NULL,
    stop_lon VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS sadp.stop_times (
    id INTEGER PRIMARY KEY,
    trip_id INTEGER NOT NULL,
    arrival_time VARCHAR NOT NULL,
    departure_time VARCHAR NOT NULL,
    stop_id INTEGER NOT NULL,
    stop_sequence INTEGER NOT NULL,
    time_point INTEGER
);

