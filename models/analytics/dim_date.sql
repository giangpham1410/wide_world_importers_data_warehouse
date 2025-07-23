SELECT
  FORMAT_DATE('%Y%m%d', d) as date_key
  , d AS date
  , EXTRACT(YEAR FROM d) AS year
  , EXTRACT(WEEK FROM d) AS year_week
  --, EXTRACT(DAY FROM d) AS year_day
  --, EXTRACT(YEAR FROM d) AS fiscal_year
  --, FORMAT_DATE('%Q', d) as fiscal_qtr
  , EXTRACT(MONTH FROM d) AS month
  , FORMAT_DATE('%B', d) as month_name
  , FORMAT_DATE('%b', d) as month_name_short
  , FORMAT_DATE('%w', d) AS week_day
  , FORMAT_DATE('%a', d) AS day_name
  , (CASE WHEN FORMAT_DATE('%A', d) IN ('Sunday', 'Saturday') THEN 'Weekend' ELSE 'Weekday' END) AS weekday_type,
FROM (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2010-01-01', '2030-12-31', INTERVAL 1 DAY)) AS d )