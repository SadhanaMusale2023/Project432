CREATE TABLE taxitrips (
    trip_id SERIAL PRIMARY KEY,
    trip_start_timestamp TIMESTAMP,
    trip_end_timestamp TIMESTAMP,
    pickup_community_area INT,
    dropoff_community_area INT,
    pickup_centroid_latitude NUMERIC(9, 6),
    pickup_centroid_longitude NUMERIC(9, 6),
    dropoff_centroid_latitude NUMERIC(9, 6),
    dropoff_centroid_longitude NUMERIC(9, 6)
);

