-- - A DDL for `hosts_cumulated` table 
--   - a `host_activity_datelist` which logs to see which dates each host is experiencing any activity

CREATE TABLE hosts_cumulated (
	host TEXT,
	metric_name TEXT,
	host_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY (host,date,metric_name)


)