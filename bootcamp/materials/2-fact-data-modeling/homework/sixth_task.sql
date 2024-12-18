-- The incremental query to generate `host_activity_datelist`


WITH yesterday as (
	select * from hosts_cumulated
	where date=DATE('2023-01-02')
), today as (
	SELECT
		host,
		'Host_activity' as metric_name,
		DATE(CAST(event_time AS TIMESTAMP)) as date_active
	FROM events
	WHERE DATE(CAST(event_time AS TIMESTAMP))=DATE('2023-01-03')
	group by host,DATE(CAST(event_time AS TIMESTAMP))
	
)
	INSERT INTO hosts_cumulated
	SELECT 
		COALESCE(t.host,y.host) as host,
		COALESCE(t.metric_name,y.metric_name) AS metric_name,
		CASE WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date_active]
			 WHEN t.date_active IS NULL THEN y.host_activity_datelist
			 ELSE ARRAY[t.date_active] || y.host_activity_datelist
			 END AS host_activity_datelist,
		COALESCE(t.date_active,y.date + INTERVAL '1 day') AS date
	FROM today t FULL OUTER JOIN yesterday y
	ON t.host=y.host