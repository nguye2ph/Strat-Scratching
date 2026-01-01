--Challenge 2
--Author: Peter Nguyen
--Purpose: Generate a per-minute, time-weighted average for each tag_id using sensor data.
--PostGreSQL Version: PostgreSQL 13 or later

-- Drop the table if it already exists to avoid conflicts
DROP TABLE IF EXISTS record_data;
CREATE TABLE record_data (
    tagid INTEGER,
    float_value FLOAT,
    timestamp TIMESTAMP
);

INSERT INTO record_data (tagid, float_value, timestamp) VALUES
(61808, 4.518282890319824, '2023-06-01 00:04:00.000'),
(61808, 4.959143161773682, '2023-06-01 00:36:50.000'),
(61808, 4.817047595977783, '2023-06-01 00:42:50.000'),
(61808, 4.674476623535156, '2023-06-01 00:43:00.000'),
(61808, 4.634105682373047, '2023-06-01 00:08:00.000'),
(61808, 4.737510681152344, '2023-06-01 00:14:50.000'),
(61808, 4.85707950592041,  '2023-06-01 00:24:30.000'),
(61808, 4.567147254943848, '2023-06-01 00:43:40.000')
-- generate the rest of the data in Excel =CONCAT("INSERT INTO EMPLOYEE VALUES (", A2, ", '", B2, "', '", C2, "');")
;

WITH mutated AS (
    SELECT
        tagid, 
        DATE_TRUNC('minute', timestamp) AS timestamp_minute,
        float_value, 
        COALESCE(
            -- Calculate the duration in seconds between the current and previous timestamp for the same tag
            DATE_PART('minute', timestamp - LAG(timestamp) OVER (PARTITION BY tagid ORDER BY timestamp)) * 60 +
            DATE_PART('second', timestamp - LAG(timestamp) OVER (PARTITION BY tagid ORDER BY timestamp)),
            -- fall back if there is no previous timestamp
            DATE_PART('minute', timestamp)*60 + DATE_PART('second', timestamp)
        ) AS duration_seconds
    FROM record_data 
)
-- Calculate the time-weighted average for each tag and minute
SELECT
    tagid, 
    timestamp_minute, 
    -- Compute the time-weighted average by summing the product of float_value and duration_seconds,
    -- then dividing by 60 (weighted by minute)
    SUM(float_value * duration_seconds) / 60 AS time_weighted_average
FROM mutated 
GROUP BY tagid, timestamp_minute
ORDER BY tagid, timestamp_minute;