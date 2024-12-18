WITH yesterday AS (
    SELECT *
    FROM host_activity_reduced
    WHERE month_start = '2023-01-01'
),
     today AS (
         SELECT host,
                DATE_TRUNC('day', CAST(event_time AS TIMESTAMP)) AS today_date,
                COUNT(1) as num_hits,
		 		COUNT(DISTINCT user_id) as unique_visitors
		 
         FROM events
         WHERE DATE_TRUNC('day', CAST(event_time AS TIMESTAMP)) = DATE('2023-01-03')
         AND user_id IS NOT NULL
         GROUP BY host, DATE_TRUNC('day', CAST(event_time AS TIMESTAMP))
     )
INSERT INTO host_activity_reduced
SELECT 
    COALESCE(t.host, y.host) AS host,
	CASE WHEN y.hit_array IS NOT NULL THEN y.hit_array || ARRAY[COALESCE(t.num_hits,0)]
		 WHEN y.hit_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(DATE(t.today_date)- DATE(DATE_TRUNC('month',t.today_date)),0)]) || ARRAY[COALESCE(t.num_hits,0)]
		END AS hit_array,
	COALESCE (y.month_start,DATE_trunc('month',t.today_date)) AS month_start,
	CASE WHEN y.unique_visitors_array IS NOT NULL THEN y.unique_visitors_array || ARRAY[COALESCE(t.unique_visitors,0)]
		 WHEN y.unique_visitors_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(DATE(t.today_date)- DATE(DATE_TRUNC('month',t.today_date)),0)]) || ARRAY[COALESCE(t.unique_visitors,0)]
		END AS unique_visitors_array
    FROM yesterday y
    FULL OUTER JOIN today t
        ON y.host = t.host
	ON CONFLICT (host, month_start)
	DO
		UPDATE SET hit_array=EXCLUDED.hit_array, unique_visitors_array=EXCLUDED.unique_visitors_array
