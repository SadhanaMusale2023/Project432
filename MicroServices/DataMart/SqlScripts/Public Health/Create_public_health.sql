CREATE TABLE public_health (
    zip_code varchar(10),
    cases_cumulative INT,
    cases_weekly INT,
    week_number INT,
    week_start DATE,
    week_end DATE,
    case_rate_weekly NUMERIC(10, 2)
);