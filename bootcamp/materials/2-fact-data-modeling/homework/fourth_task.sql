-- A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column 

WITH starter AS (
    SELECT uc.dates_active @> ARRAY [DATE(d.valid_date)]   AS is_active,
           EXTRACT(
               DAY FROM DATE('2023-01-31') - d.valid_date) AS days_since,
           uc.user_id,
		   uc.browser_type
    FROM user_devices_cumulated uc
             CROSS JOIN
         (SELECT generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS valid_date) as d
    WHERE date = DATE('2023-01-31')
), bits AS (
         SELECT user_id,
				browser_type,
                SUM(CASE
                        WHEN is_active THEN POW(2, 32 - days_since)
                        ELSE 0 END)::bigint::bit(32) AS datelist_int,
                DATE('2023-01-31') as date
         FROM starter
         GROUP BY user_id,browser_type
     )

     --INSERT INTO user_datelist_int
     SELECT * FROM bits