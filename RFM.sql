--CTE for frequency and monetary values
WITH t1 AS
(
  SELECT 
  CustomerID,
  MAX(DATE_TRUNC(InvoiceDate,day)) AS last_purchase_date,
  COUNT(DISTINCT InvoiceNo) AS frequency,
  ROUND(SUM(Quantity*UnitPrice),2) AS monetary
  FROM `turing_data_analytics.rfm`
  WHERE DATE_TRUNC(InvoiceDate, day) BETWEEN '2010-12-01' AND '2011-12-01'
    AND CustomerID IS NOT NULL
    AND Quantity > 0 AND UnitPrice > 0 
  GROUP BY CustomerID
),

--CTE for recency and latest purchase date in whole chosen data set
t2 AS (
  SELECT 
  CustomerID,
  frequency,    
  monetary,
  DATE_DIFF(reference_date, last_purchase_date, DAY) AS recency
  FROM (
      SELECT  *,
      MAX(last_purchase_date) OVER () AS reference_date --this returns the latest purchase date in whole chosen data set
      FROM t1
    )  
),
--CTE for assigning quartiles
t3 AS
(
  SELECT
  a.*,
  b.percentiles[OFFSET(25)] AS r25,
  b.percentiles[OFFSET(50)] AS r50,
  b.percentiles[OFFSET(75)] AS r75,
  b.percentiles[OFFSET(100)] AS r100,
  c.percentiles[OFFSET(25)] AS f25,
  c.percentiles[OFFSET(50)] AS f50,
  c.percentiles[OFFSET(75)] AS f75,
  c.percentiles[OFFSET(100)] AS f100,
  d.percentiles[OFFSET(25)] AS m25,
  d.percentiles[OFFSET(50)] AS m50,
  d.percentiles[OFFSET(75)] AS m75,
  d.percentiles[OFFSET(100)] AS m100,
  FROM
    t2 a,
    (SELECT APPROX_QUANTILES(recency, 100) percentiles FROM t2) b,
    (SELECT APPROX_QUANTILES(frequency, 100) percentiles FROM t2) c,
    (SELECT APPROX_QUANTILES(monetary, 100) percentiles FROM t2) d
),
--CTE for setting R, F and M scores from what we calulated in last t3
t4 AS
(
  SELECT *
  FROM (
      SELECT *,
      CASE WHEN recency <= r25 THEN 4
          WHEN recency <= r50 AND recency > r25 THEN 3
          WHEN recency <= r75 AND recency > r50 THEN 2
          WHEN recency <= r100 AND recency > r75 THEN 1
      END AS r_score,
      CASE WHEN frequency <= f25 THEN 1
          WHEN frequency <= f50 AND frequency > f25 THEN 2
          WHEN frequency <= f75 AND frequency > f50 THEN 3
          WHEN frequency <= f100 AND frequency > f75 THEN 4
      END AS f_score,
      CASE WHEN monetary <= m25 THEN 1
          WHEN monetary <= m50 AND monetary > m25 THEN 2
          WHEN monetary <= m75 AND monetary > m50 THEN 3
          WHEN monetary <= m100 AND monetary > m75 THEN 4
      END AS m_score
      FROM t3
  )
),
--CTE for concatenating R, F and M scores so as I would be able to then segment customers in next CTE
t5 AS
(
  SELECT
    CustomerID,
    recency,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score, 
    CONCAT(r_score, f_score, m_score) AS RFM_score
  FROM t4
),
--CTE for segmenting customers based on RFM_score
t6 AS
(
  SELECT *,
    CASE
      WHEN RFM_score = '444' THEN 'Best Customers'
      WHEN RFM_score IN ('443', '442', '441', '344', '343', '342', '334', '341') THEN 'Loyal Customers'
      WHEN RFM_score IN ('414', '314', '214', '114', '224', '324') THEN 'Big Spenders'
      WHEN RFM_score = '111' THEN 'Lost Customers'
      WHEN RFM_score IN ('333', '434', '433', '332', '424', '431', '432') THEN 'Potential Loyalists'
      WHEN RFM_score IN ('412', '411', '311', '312') THEN 'New Customers'
      WHEN RFM_score IN ('422', '421', '413', '313', '321', '322', '331', '423', '323') THEN 'Promising'
      WHEN RFM_score IN ('233', '232', '213', '231', '212', '221', '242') THEN 'Need Attention'
      WHEN RFM_score IN ('144', '143', '134', '133', '124', '123') THEN 'Cant Lose Them'
      WHEN RFM_score IN ('222', '223', '243', '244', '234', '241') THEN 'At Risk'
      WHEN RFM_score IN ('112', '113', '121', '122', '131', '132', '141', '142', '211') THEN 'About to Sleep'
      ELSE 'Other'
    END AS Segment
  FROM t5
)
SELECT *
FROM t6
ORDER BY 1--CTE for frequency and monetary values
WITH t1 AS
(
  SELECT 
  CustomerID,
  MAX(DATE_TRUNC(InvoiceDate,day)) AS last_purchase_date,
  COUNT(DISTINCT InvoiceNo) AS frequency,
  ROUND(SUM(Quantity*UnitPrice),2) AS monetary
  FROM `turing_data_analytics.rfm`
  WHERE DATE_TRUNC(InvoiceDate, day) BETWEEN '2010-12-01' AND '2011-12-01'
    AND CustomerID IS NOT NULL
    AND Quantity > 0 AND UnitPrice > 0 
  GROUP BY CustomerID
),

--CTE for recency and latest purchase date in whole chosen data set
t2 AS (
  SELECT 
  CustomerID,
  frequency,    
  monetary,
  DATE_DIFF(reference_date, last_purchase_date, DAY) AS recency
  FROM (
      SELECT  *,
      MAX(last_purchase_date) OVER () AS reference_date --this returns the latest purchase date in whole chosen data set
      FROM t1
    )  
),
--CTE for assigning quartiles
t3 AS
(
  SELECT
  a.*,
  b.percentiles[OFFSET(25)] AS r25,
  b.percentiles[OFFSET(50)] AS r50,
  b.percentiles[OFFSET(75)] AS r75,
  b.percentiles[OFFSET(100)] AS r100,
  c.percentiles[OFFSET(25)] AS f25,
  c.percentiles[OFFSET(50)] AS f50,
  c.percentiles[OFFSET(75)] AS f75,
  c.percentiles[OFFSET(100)] AS f100,
  d.percentiles[OFFSET(25)] AS m25,
  d.percentiles[OFFSET(50)] AS m50,
  d.percentiles[OFFSET(75)] AS m75,
  d.percentiles[OFFSET(100)] AS m100,
  FROM
    t2 a,
    (SELECT APPROX_QUANTILES(recency, 100) percentiles FROM t2) b,
    (SELECT APPROX_QUANTILES(frequency, 100) percentiles FROM t2) c,
    (SELECT APPROX_QUANTILES(monetary, 100) percentiles FROM t2) d
),
--CTE for setting R, F and M scores from what we calulated in last t3
t4 AS
(
  SELECT *
  FROM (
      SELECT *,
      CASE WHEN recency <= r25 THEN 4
          WHEN recency <= r50 AND recency > r25 THEN 3
          WHEN recency <= r75 AND recency > r50 THEN 2
          WHEN recency <= r100 AND recency > r75 THEN 1
      END AS r_score,
      CASE WHEN frequency <= f25 THEN 1
          WHEN frequency <= f50 AND frequency > f25 THEN 2
          WHEN frequency <= f75 AND frequency > f50 THEN 3
          WHEN frequency <= f100 AND frequency > f75 THEN 4
      END AS f_score,
      CASE WHEN monetary <= m25 THEN 1
          WHEN monetary <= m50 AND monetary > m25 THEN 2
          WHEN monetary <= m75 AND monetary > m50 THEN 3
          WHEN monetary <= m100 AND monetary > m75 THEN 4
      END AS m_score
      FROM t3
  )
),
--CTE for concatenating R, F and M scores so as I would be able to then segment customers in next CTE
t5 AS
(
  SELECT
    CustomerID,
    recency,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score, 
    CONCAT(r_score, f_score, m_score) AS RFM_score
  FROM t4
),
--CTE for segmenting customers based on RFM_score
t6 AS
(
  SELECT *,
    CASE
      WHEN RFM_score = '444' THEN 'Best Customers'
      WHEN RFM_score IN ('443', '442', '441', '344', '343', '342', '334', '341') THEN 'Loyal Customers'
      WHEN RFM_score IN ('414', '314', '214', '114', '224', '324') THEN 'Big Spenders'
      WHEN RFM_score = '111' THEN 'Lost Customers'
      WHEN RFM_score IN ('333', '434', '433', '332', '424', '431', '432') THEN 'Potential Loyalists'
      WHEN RFM_score IN ('412', '411', '311', '312') THEN 'New Customers'
      WHEN RFM_score IN ('422', '421', '413', '313', '321', '322', '331', '423', '323') THEN 'Promising'
      WHEN RFM_score IN ('233', '232', '213', '231', '212', '221', '242') THEN 'Need Attention'
      WHEN RFM_score IN ('144', '143', '134', '133', '124', '123') THEN 'Cant Lose Them'
      WHEN RFM_score IN ('222', '223', '243', '244', '234', '241') THEN 'At Risk'
      WHEN RFM_score IN ('112', '113', '121', '122', '131', '132', '141', '142', '211') THEN 'About to Sleep'
      ELSE 'Other'
    END AS Segment
  FROM t5
)
SELECT *
FROM t6
ORDER BY 1  
