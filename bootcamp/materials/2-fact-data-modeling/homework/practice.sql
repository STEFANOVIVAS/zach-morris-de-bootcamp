do $$
declare
	counter DATE := DATE('2023-01-03');
begin
	while counter < '2023-02-01' loop
	
		with YESTERDAY AS (
		SELECT * FROM users_cumulated
		where date=DATE(counter)


		),TODAY AS(
		SELECT
			CAST(user_id AS TEXT) as user_id,
			DATE(CAST(event_time AS TIMESTAMP)) as date_active
		FROM events
		WHERE DATE(CAST(event_time AS TIMESTAMP))=DATE(counter + INTERVAL '1 day') AND user_id IS NOT NULL
		GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
		)

		INSERT INTO users_cumulated
		SELECT 
			COALESCE(Y.user_id,T.user_id),
			CASE WHEN Y.dates_active IS NULL THEN ARRAY[T.date_active]
				 WHEN T.date_active is NULL THEN Y.dates_active
				 ELSE ARRAY[T.date_active] || Y.dates_active END as dates_active,
			COALESCE (T.date_active, Y.date +1)



		FROM TODAY AS T FULL OUTER JOIN YESTERDAY AS Y
		ON T.user_id=Y.user_id;
		raise notice 'Counter %', counter;
		counter := counter + INTERVAL '1 day';
	end loop;
end;
$$;




with YESTERDAY AS (
	SELECT * FROM users_cumulated
	where date=DATE('2023-01-01')


),TODAY AS(
	SELECT
		CAST(user_id AS TEXT) as user_id,
		DATE(CAST(event_time AS TIMESTAMP)) as date_active
	FROM events
	WHERE DATE(CAST(event_time AS TIMESTAMP))=DATE('2023-01-02') AND user_id IS NOT NULL
	GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
)

INSERT INTO users_cumulated
SELECT 
	COALESCE(Y.user_id,T.user_id),
	CASE WHEN Y.dates_active IS NULL THEN ARRAY[T.date_active]
		 WHEN T.date_active is NULL THEN Y.dates_active
		 ELSE ARRAY[T.date_active] || Y.dates_active END as dates_active,
	COALESCE (T.date_active, Y.date +1)



FROM TODAY AS T FULL OUTER JOIN YESTERDAY AS Y
ON T.user_id=Y.user_id;

SELECT * FROM users_cumulated
WHERE date=DATE('2023-01-31')

do $$
declare
	counter DATE := DATE('2023-01-03');
begin
	while counter < '2023-02-01' loop
	
		with YESTERDAY AS (
		SELECT * FROM users_cumulated
		where date=DATE(counter)


		),TODAY AS(
		SELECT
			CAST(user_id AS TEXT) as user_id,
			DATE(CAST(event_time AS TIMESTAMP)) as date_active
		FROM events
		WHERE DATE(CAST(event_time AS TIMESTAMP))=DATE(counter + INTERVAL '1 day') AND user_id IS NOT NULL
		GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
		)

		INSERT INTO users_cumulated
		SELECT 
			COALESCE(Y.user_id,T.user_id),
			CASE WHEN Y.dates_active IS NULL THEN ARRAY[T.date_active]
				 WHEN T.date_active is NULL THEN Y.dates_active
				 ELSE ARRAY[T.date_active] || Y.dates_active END as dates_active,
			COALESCE (T.date_active, Y.date +1)



		FROM TODAY AS T FULL OUTER JOIN YESTERDAY AS Y
		ON T.user_id=Y.user_id;
		raise notice 'Counter %', counter;
		counter := counter + INTERVAL '1 day';
	end loop;
end;
$$;




with YESTERDAY AS (
	SELECT * FROM users_cumulated
	where date=DATE('2023-01-01')


),TODAY AS(
	SELECT
		CAST(user_id AS TEXT) as user_id,
		DATE(CAST(event_time AS TIMESTAMP)) as date_active
	FROM events
	WHERE DATE(CAST(event_time AS TIMESTAMP))=DATE('2023-01-02') AND user_id IS NOT NULL
	GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
)

INSERT INTO users_cumulated
SELECT 
	COALESCE(Y.user_id,T.user_id),
	CASE WHEN Y.dates_active IS NULL THEN ARRAY[T.date_active]
		 WHEN T.date_active is NULL THEN Y.dates_active
		 ELSE ARRAY[T.date_active] || Y.dates_active END as dates_active,
	COALESCE (T.date_active, Y.date +1)



FROM TODAY AS T FULL OUTER JOIN YESTERDAY AS Y
ON T.user_id=Y.user_id;

SELECT * FROM users_cumulated
WHERE date=DATE('2023-01-31')









-- CREATE TABLE users_cumulated (
-- 	user_id TEXT,
-- 	dates_active DATE[],
-- 	date DATE,
-- 	PRIMARY KEY(user_id,date)


-- )







-- CREATE TABLE users_cumulated (
-- 	user_id TEXT,
-- 	dates_active DATE[],
-- 	date DATE,
-- 	PRIMARY KEY(user_id,date)


-- )