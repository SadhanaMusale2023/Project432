CREATE TABLE Building_Permits_Fact (
    id SERIAL PRIMARY KEY,
    permit VARCHAR(255),
    permit_type VARCHAR(255),
    application_start_date TIMESTAMP,
    latitude NUMERIC(9, 6),
    longitude NUMERIC(9, 6),
    xcoordinate INT,
    ycoordinate INT
);