
   CREATE TABLE host_activity_reduced
(
    host          	 		TEXT,
	 hit_array        		INT[],
    month_start      		DATE,
    unique_visitors_array 	INT[],
    
    PRIMARY KEY (host, month_start)
);