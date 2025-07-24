WITH
  dim_date__generate AS (
    SELECT
        *
      FROM
        UNNEST(GENERATE_DATE_ARRAY('2010-01-01', '2030-12-31', INTERVAL 1 DAY)) AS date
)

, dim_date__enrich AS (
  SELECT
    FORMAT_DATE('%Y%m%d', date) as date_key
    , date
    , EXTRACT(YEAR FROM date) AS year_number
    , DATE_TRUNC(date, YEAR) AS year
    , EXTRACT(WEEK FROM date) AS year_week
    , DATE_TRUNC(date, MONTH) AS year_month
    --, EXTRACT(DAY FROM date) AS year_day
    --, EXTRACT(YEAR FROM date) AS fiscal_year
    --, FORMAT_DATE('%Q', date) as fiscal_qtr
    , EXTRACT(MONTH FROM date) AS month_num
    , FORMAT_DATE('%B', date) as month
    , FORMAT_DATE('%b', date) as month_name_short
    , FORMAT_DATE('%w', date) AS week_day
    , FORMAT_DATE('%A', date) AS day_of_week
    , FORMAT_DATE('%a', date) AS day_of_week_short
  FROM dim_date__generate
)


SELECT
  *
  , CASE
      WHEN day_of_week_short IN ('Sun', 'Sat') THEN 'Weekend'
      WHEN day_of_week_short IN ('Mon', 'Tue', 'Wed', 'Thu', 'Fri') THEN 'Weekday'
      ELSE 'Invalid'
      END AS is_weekday_or_weekend
FROM dim_date__enrich

