do $$
declare
	counter date := DATE('2023-01-01');
begin
	while counter <= DATE('2023-01-30') loop
		WITH yesterday as (
		SELECT * FROM 
		user_devices_cumulated
		WHERE date=counter

	), today as (


		SELECT  
			CAST(user_id AS TEXT) AS user_id,
			browser_type,
			DATE(CAST(event_time as TIMESTAMP)) as date_active
		FROM events inner join devices 
		on events.device_id=devices.device_id
		WHERE DATE(CAST(event_time as TIMESTAMP))=(counter + INTERVAL '1 day') AND user_id IS NOT NULL
		GROUP BY user_id, browser_type,DATE(CAST(event_time as TIMESTAMP))
	)
		INSERT INTO user_devices_cumulated
		SELECT 
			COALESCE(t.user_id,y.user_id) as user_id,
			COALESCE(t.browser_type,y.browser_type) as browser_type,
			CASE WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]           
				  WHEN t.date_active IS NULL THEN y.dates_active
				  ELSE ARRAY[t.date_active] || y.dates_active END as dates_active,
			COALESCE(t.date_active,y.date + INTERVAL'1 day') as date
		FROM
		today t FULL OUTER JOIN yesterday y
		ON y.user_id=t.user_id and t.browser_type=y.browser_type;
	
		
		
		raise notice 'Counter %', counter;
		counter := counter + INTERVAL '1 day';
	end loop;
end;
$$;
