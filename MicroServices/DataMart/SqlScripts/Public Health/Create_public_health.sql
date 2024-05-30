
CREATE TABLE public_health_statistics (
    community_area INT PRIMARY KEY,
    below_poverty_level NUMERIC(5, 2),
    per_capita_income INT,
    unemployment NUMERIC(5, 2)
);
