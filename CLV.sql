--CTE for extracting each user's first week
WITH cohort AS 
(
SELECT 
  DISTINCT user_pseudo_id,
  DATE_TRUNC(MIN(PARSE_DATE('%Y%m%d', event_date)), week) AS start_week
FROM `turing_data_analytics.raw_events`
GROUP BY 1
),
--CTE for extracting purchases and putting into weekly basis
purchases AS
(
  SELECT
    DATE_TRUNC(PARSE_DATE('%Y%m%d', event_date), week) AS purchase_week,
    user_pseudo_id,
    purchase_revenue_in_usd AS revenue
  FROM `turing_data_analytics.raw_events`
  WHERE event_name = 'purchase'
),
--CTE for main data that will be used to divide into weeks
main_table AS
(
SELECT
  c.user_pseudo_id,
  c.start_week,
  p.purchase_week,
  p.revenue
FROM cohort c
LEFT JOIN purchases p
  ON c.user_pseudo_id = p.user_pseudo_id
WHERE c.start_week <= '2021-01-24'
)
SELECT
  start_week,
  SUM(CASE WHEN purchase_week = start_week THEN revenue END)/COUNT(DISTINCT(user_pseudo_id)) AS week_0,
  SUM(CASE WHEN purchase_week = DATE_ADD(start_week, INTERVAL 1 WEEK) THEN revenue END)/COUNT(DISTINCT(user_pseudo_id)) week_1,
  SUM(CASE WHEN purchase_week = DATE_ADD(start_week, INTERVAL 2 WEEK) THEN revenue END)/COUNT(DISTINCT(user_pseudo_id)) week_2,
  SUM(CASE WHEN purchase_week = DATE_ADD(start_week, INTERVAL 3 WEEK) THEN revenue END)/COUNT(DISTINCT(user_pseudo_id)) week_3,
  SUM(CASE WHEN purchase_week = DATE_ADD(start_week, INTERVAL 4 WEEK) THEN revenue END)/COUNT(DISTINCT(user_pseudo_id)) week_4,
  SUM(CASE WHEN purchase_week = DATE_ADD(start_week, INTERVAL 5 WEEK) THEN revenue END)/COUNT(DISTINCT(user_pseudo_id)) week_5,
  SUM(CASE WHEN purchase_week = DATE_ADD(start_week, INTERVAL 6 WEEK) THEN revenue END)/COUNT(DISTINCT(user_pseudo_id)) week_6,
  SUM(CASE WHEN purchase_week = DATE_ADD(start_week, INTERVAL 7 WEEK) THEN revenue END)/COUNT(DISTINCT(user_pseudo_id)) week_7,
  SUM(CASE WHEN purchase_week = DATE_ADD(start_week, INTERVAL 8 WEEK) THEN revenue END)/COUNT(DISTINCT(user_pseudo_id)) week_8,
  SUM(CASE WHEN purchase_week = DATE_ADD(start_week, INTERVAL 9 WEEK) THEN revenue END)/COUNT(DISTINCT(user_pseudo_id)) week_9,
  SUM(CASE WHEN purchase_week = DATE_ADD(start_week, INTERVAL 10 WEEK) THEN revenue END)/COUNT(DISTINCT(user_pseudo_id)) week_10,
  SUM(CASE WHEN purchase_week = DATE_ADD(start_week, INTERVAL 11 WEEK) THEN revenue END)/COUNT(DISTINCT(user_pseudo_id)) week_11,
  SUM(CASE WHEN purchase_week = DATE_ADD(start_week, INTERVAL 12 WEEK) THEN revenue END)/COUNT(DISTINCT(user_pseudo_id)) week_12
FROM main_table
GROUP BY start_week
ORDER BY start_week
