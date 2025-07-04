-- Создаем таблицы
CREATE TABLE user_events (
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;


CREATE TABLE user_event_counts_agg (
	event_date Date,
	event_type String,
	uniq_users_state AggregateFunction(uniq, UInt32),
	points_spent_sum_state AggregateFunction(sum, UInt32),
	event_count_state AggregateFunction(count, UInt8)
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;

-- Создаем MV
CREATE MATERIALIZED VIEW mv_daily_user_events 
TO user_event_counts_agg
AS
SELECT 
    toDate(event_time) AS event_date,
    event_type,
    uniqState(user_id) AS uniq_users_state,
    sumState(points_spent) AS points_spent_sum_state,
    countState() AS event_count_state
FROM user_events
GROUP BY (event_date, event_type);
    

-- Вставляем данные
 INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),


(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),


(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),


(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),


(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),


(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());   

-- Создаем Merge запросы
SELECT 
    event_date,
    event_type,
    uniqMerge(uniq_users_state) AS uniq_users,
    sumMerge(points_spent_sum_state) AS total_spent,
    countMerge(event_count_state) AS total_actions
FROM user_event_counts_agg
GROUP BY event_date, event_type 
ORDER BY event_date, event_type 


-- Считаем Retention
WITH start_date AS (
    SELECT 
        user_id,
        min(toDate(event_time)) AS start_date
    FROM user_events
    GROUP BY user_id
),
returning_users AS (
	SELECT 
	    start_date.start_date,
	    start_date.user_id
	FROM start_date
	INNER JOIN user_events 
	ON start_date.user_id = user_events.user_id 
	    AND toDate(user_events.event_time) BETWEEN start_date.start_date + INTERVAL 1 DAY AND start_date.start_date + INTERVAL 7 DAY
 )
 
 SELECT 
     start_date.start_date AS date,
     COUNT(DISTINCT start_date.user_id) AS total_users_day_0,
     COUNT(DISTINCT returning_users.user_id) AS returned_in_7_days,
     round(COUNT(DISTINCT returning_users.user_id) / NULLIF(COUNT(DISTINCT start_date.user_id), 0) * 100 , 2) AS retention_7d_percent
 FROM start_date
 LEFT JOIN returning_users ON start_date.start_date = returning_users.start_date
 GROUP BY start_date.start_date
 ORDER BY start_date.start_date;
 
