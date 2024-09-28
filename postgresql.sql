https://www.postgresql.org/docs/current/sql.html

-- --- PostgreSQL SQL Snippet ---

-- --- Important Data Types ---
-- Numeric types
CREATE TABLE numeric_types (
    id SERIAL PRIMARY KEY,
    int_value INT,
    float_value FLOAT,
    decimal_value DECIMAL(10, 2)
);

-- String types
CREATE TABLE string_types (
    id SERIAL PRIMARY KEY,
    char_value CHAR(10),
    varchar_value VARCHAR(255),
    text_value TEXT
);

-- Date/Time types
CREATE TABLE datetime_types (
    id SERIAL PRIMARY KEY,
    date_value DATE,
    timestamp_value TIMESTAMP,
    interval_value INTERVAL
);

-- JSONB for storing JSON data
CREATE TABLE json_data (
    id SERIAL PRIMARY KEY,
    data JSONB
);

-- --- Create tables ---
CREATE TABLE cities (
    name       VARCHAR(80) PRIMARY KEY,
    location   POINT DEFAULT '(0, 0)',
    no         INT DEFAULT nextval('no')
);

CREATE TABLE weather (
    city      VARCHAR(80) REFERENCES cities(name),
    temp_lo   INT,
    temp_hi   INT,
    prcp      REAL,
    date      DATE
);

-- --- Create table Inheritance ---
CREATE TABLE capitals (
    state CHAR(2) UNIQUE NOT NULL
) INHERITS (cities);

-- --- Drop table ---
DROP TABLE tablename;

-- --- Insert ---
INSERT INTO weather (date, city, temp_hi, temp_lo)
VALUES ('1994-11-29', 'Hayward', 54, 37);

-- --- Update ---
UPDATE weather
SET temp_hi = temp_hi - 2, temp_lo = temp_lo - 2
WHERE date > '1994-11-28';

-- --- Delete ---
DELETE FROM weather WHERE city = 'Hayward';

-- --- Select ---
SELECT DISTINCT city FROM weather;

SELECT city, (temp_hi + temp_lo) / 2 AS temp_avg, date FROM weather;

SELECT * FROM weather;

-- --- Join ---
SELECT * FROM weather JOIN cities ON city = name;

SELECT weather.city, weather.temp_lo, weather.temp_hi, 
       weather.prcp, weather.date, cities.location
FROM weather JOIN cities ON weather.city = cities.name;

SELECT *
FROM weather, cities
WHERE city = name;

-- --- Aggregate Functions ---
SELECT max(temp_lo) FROM weather;

SELECT city FROM weather
WHERE temp_lo = (SELECT max(temp_lo) FROM weather);

SELECT city, count(*), max(temp_lo)
FROM weather
GROUP BY city;

SELECT city, count(*), max(temp_lo)
FROM weather
GROUP BY city
HAVING max(temp_lo) < 40;

SELECT city, count(*) FILTER (WHERE temp_lo < 45), max(temp_lo)
FROM weather
GROUP BY city;

-- --- Views ---
CREATE VIEW myview AS
SELECT name, temp_lo, temp_hi, prcp, date, location
FROM weather, cities
WHERE city = name;

SELECT * FROM myview; 

-- --- UNION ---
SELECT 1 AS Column1, 2 AS Column2
UNION ALL
SELECT 3 AS Column1, 4 AS Column2;

-- --- Advanced Features ---
-- --- CTEs (Common Table Expressions) and Recursive Queries ---
WITH recent_weather AS (
    SELECT city, temp_hi, temp_lo, date 
    FROM weather
    WHERE date > '1994-11-28'
)
SELECT * FROM recent_weather;

WITH RECURSIVE organization_chart AS (
    SELECT employee_id, employee_name, manager_id
    FROM employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.employee_id, e.employee_name, e.manager_id
    FROM employees e
    JOIN organization_chart o ON e.manager_id = o.employee_id
)
SELECT * FROM organization_chart;

-- --- Foreign Data Wrapper (FDW) ---
CREATE EXTENSION postgres_fdw;

CREATE SERVER foreign_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'remote_host', dbname 'foreign_db', port '5432');

CREATE USER MAPPING FOR current_user
SERVER foreign_server
OPTIONS (user 'remote_user', password 'remote_password');

IMPORT FOREIGN SCHEMA public
FROM SERVER foreign_server
INTO local_schema;

SELECT * FROM local_schema.foreign_table;

-- --- Materialized Views ---
CREATE MATERIALIZED VIEW weather_summary AS
SELECT city, AVG(temp_hi) AS avg_temp_hi, AVG(temp_lo) AS avg_temp_lo, COUNT(*) AS record_count
FROM weather
GROUP BY city;

REFRESH MATERIALIZED VIEW weather_summary;

SELECT * FROM weather_summary;

-- --- Logical Replication ---
CREATE PUBLICATION my_publication FOR ALL TABLES;

CREATE SUBSCRIPTION my_subscription
CONNECTION 'host=primary_host dbname=primary_db user=replication_user password=replication_password'
PUBLICATION my_publication;

-- --- Parallel Query Processing ---
SET max_parallel_workers_per_gather = 4;

SELECT SUM(temp_hi)
FROM weather
WHERE temp_lo > 40;

-- --- Lateral Joins ---
SELECT cities.name, weather.date, w.temp_hi, w.temp_lo
FROM cities
JOIN LATERAL (
    SELECT temp_hi, temp_lo, date
    FROM weather
    WHERE weather.city = cities.name
    ORDER BY date DESC
    LIMIT 1
) w ON TRUE;

-- --- Custom Aggregates ---
CREATE AGGREGATE string_agg_custom(TEXT, TEXT) (
    SFUNC = textcat,
    STYPE = TEXT,
    INITCOND = ''
);

SELECT string_agg_custom(name, ', ')
FROM cities;

-- --- Event Triggers ---
CREATE EVENT TRIGGER prevent_table_drop
ON ddl_command_start
WHEN TAG IN ('DROP TABLE')
EXECUTE FUNCTION prevent_table_drop_function();

CREATE FUNCTION prevent_table_drop_function() RETURNS event_trigger AS $$
BEGIN
    RAISE EXCEPTION 'Dropping tables is prohibited!';
END;
$$ LANGUAGE plpgsql;

-- --- Advanced Partitioning (Hash Partitioning) ---
CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    sale_date DATE,
    amount NUMERIC
) PARTITION BY HASH (id);

CREATE TABLE sales_partition_1 PARTITION OF sales
    FOR VALUES WITH (MODULUS 4, REMAINDER 0);

CREATE TABLE sales_partition_2 PARTITION OF sales
    FOR VALUES WITH (MODULUS 4, REMAINDER 1);

CREATE TABLE sales_partition_3 PARTITION OF sales
    FOR VALUES WITH (MODULUS 4, REMAINDER 2);

CREATE TABLE sales_partition_4 PARTITION OF sales
    FOR VALUES WITH (MODULUS 4, REMAINDER 3);

INSERT INTO sales (sale_date, amount) VALUES ('2024-09-28', 100.00);

SELECT * FROM sales WHERE amount > 50;
