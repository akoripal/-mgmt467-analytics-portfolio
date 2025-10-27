-- =====================================================
-- MGMT 467 Data Quality (DQ) Queries - Netflix Dataset
-- Author: Anurag Koripalli
-- =====================================================

-- Step 1: Verify row counts for all Netflix tables
SELECT 'users' AS table_name, COUNT(*) AS row_count FROM `mgmt467project.netflix.users`
UNION ALL
SELECT 'movies', COUNT(*) FROM `mgmt467project.netflix.movies`
UNION ALL
SELECT 'watch_history', COUNT(*) FROM `mgmt467project.netflix.watch_history`
UNION ALL
SELECT 'recommendation_logs', COUNT(*) FROM `mgmt467project.netflix.recommendation_logs`
UNION ALL
SELECT 'search_logs', COUNT(*) FROM `mgmt467project.netflix.search_logs`
UNION ALL
SELECT 'reviews', COUNT(*) FROM `mgmt467project.netflix.reviews`;

-- Step 2: Missingness percentages for key user fields
SELECT
  COUNT(*) AS total_rows,
  ROUND(SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pct_country_missing,
  ROUND(SUM(CASE WHEN subscription_plan IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pct_subscription_plan_missing,
  ROUND(SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pct_age_missing
FROM `mgmt467project.netflix.users`;

-- Step 3: Missingness by country (for subscription_plan)
SELECT
  country,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN subscription_plan IS NULL THEN 1 ELSE 0 END) AS missing_plan_tier_count,
  ROUND(SUM(CASE WHEN subscription_plan IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pct_subscription_plan_missing
FROM `mgmt467project.netflix.users`
GROUP BY country
ORDER BY pct_subscription_plan_missing DESC;

-- Step 4: Duplicate groups in watch_history
SELECT
  user_id,
  movie_id,
  watch_date,
  device_type,
  COUNT(*) AS duplicate_count
FROM `mgmt467project.netflix.watch_history`
GROUP BY user_id, movie_id, watch_date, device_type
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20;

-- Step 5: Create deduplicated watch_history table
CREATE OR REPLACE TABLE `mgmt467project.netflix.watch_history_dedup` AS
SELECT *
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY user_id, movie_id, watch_date, device_type
      ORDER BY progress_percentage DESC, watch_duration_minutes DESC
    ) AS row_rank
  FROM `mgmt467project.netflix.watch_history`
)
WHERE row_rank = 1;

-- Step 6: Verify before vs after deduplication
SELECT 'Before Deduplication' AS stage, COUNT(*) AS total_rows
FROM `mgmt467project.netflix.watch_history`
UNION ALL
SELECT 'After Deduplication' AS stage, COUNT(*) AS total_rows
FROM `mgmt467project.netflix.watch_history_dedup`;

-- Step 7: Compute IQR bounds and % outliers for watch_duration_minutes
WITH stats AS (
  SELECT
    APPROX_QUANTILES(watch_duration_minutes, 4)[OFFSET(1)] AS Q1,
    APPROX_QUANTILES(watch_duration_minutes, 4)[OFFSET(3)] AS Q3
  FROM `mgmt467project.netflix.watch_history_dedup`
),
bounds AS (
  SELECT
    Q1,
    Q3,
    (Q3 - Q1) AS IQR,
    Q1 - 1.5 * (Q3 - Q1) AS lower_bound,
    Q3 + 1.5 * (Q3 - Q1) AS upper_bound
  FROM stats
),
outlier_counts AS (
  SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN watch_duration_minutes < (SELECT lower_bound FROM bounds)
             OR watch_duration_minutes > (SELECT upper_bound FROM bounds)
        THEN 1 ELSE 0 END) AS outlier_count
  FROM `mgmt467project.netflix.watch_history_dedup`
)
SELECT
  (SELECT Q1 FROM bounds) AS Q1,
  (SELECT Q3 FROM bounds) AS Q3,
  (SELECT IQR FROM bounds) AS IQR,
  (SELECT lower_bound FROM bounds) AS lower_bound,
  (SELECT upper_bound FROM bounds) AS upper_bound,
  total_rows,
  outlier_count,
  ROUND(outlier_count / total_rows * 100, 2) AS pct_outliers
FROM outlier_counts;

-- Step 8: Create winsorized (robust) table
CREATE OR REPLACE TABLE `mgmt467project.netflix.watch_history_robust` AS
WITH pct_bounds AS (
  SELECT
    APPROX_QUANTILES(watch_duration_minutes, 100)[OFFSET(1)] AS p01,
    APPROX_QUANTILES(watch_duration_minutes, 100)[OFFSET(99)] AS p99
  FROM `mgmt467project.netflix.watch_history_dedup`
)
SELECT
  w.*,
  CASE
    WHEN w.watch_duration_minutes < p.p01 THEN p.p01
    WHEN w.watch_duration_minutes > p.p99 THEN p.p99
    ELSE w.watch_duration_minutes
  END AS minutes_watched_capped
FROM `mgmt467project.netflix.watch_history_dedup` w
CROSS JOIN pct_bounds p;

-- Step 9: Compare min/median/max before vs after winsorization
SELECT
  'Before Winsorization' AS stage,
  MIN(watch_duration_minutes) AS min_value,
  APPROX_QUANTILES(watch_duration_minutes, 2)[OFFSET(1)] AS median_value,
  MAX(watch_duration_minutes) AS max_value
FROM `mgmt467project.netflix.watch_history_dedup`
UNION ALL
SELECT
  'After Winsorization' AS stage,
  MIN(minutes_watched_capped) AS min_value,
  APPROX_QUANTILES(minutes_watched_capped, 2)[OFFSET(1)] AS median_value,
  MAX(minutes_watched_capped) AS max_value
FROM `mgmt467project.netflix.watch_history_robust`;

-- Step 10: Flag binge sessions (> 8 hours)
SELECT
  COUNT(*) AS total_sessions,
  SUM(CASE WHEN watch_duration_minutes > 480 THEN 1 ELSE 0 END) AS binge_count,
  ROUND(SUM(CASE WHEN watch_duration_minutes > 480 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pct_binge
FROM `mgmt467project.netflix.watch_history_robust`;

-- Step 11: Flag unrealistic ages (< 10 or > 100)
SELECT
  COUNT(*) AS total_users,
  SUM(CASE WHEN age < 10 OR age > 100 THEN 1 ELSE 0 END) AS extreme_age_count,
  ROUND(SUM(CASE WHEN age < 10 OR age > 100 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pct_extreme_age
FROM `mgmt467project.netflix.users`;

-- Step 12: Flag anomalous movie durations (< 15 min or > 480 min)
SELECT
  COUNT(*) AS total_movies,
  SUM(CASE WHEN duration_minutes < 15 OR duration_minutes > 480 THEN 1 ELSE 0 END) AS duration_anomaly_count,
  ROUND(SUM(CASE WHEN duration_minutes < 15 OR duration_minutes > 480 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pct_duration_anomaly
FROM `mgmt467project.netflix.movies`;

-- Step 13: Summary of all anomaly flags
SELECT 'flag_binge' AS flag_name,
       ROUND(SUM(CASE WHEN watch_duration_minutes > 480 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS pct_of_rows
FROM `mgmt467project.netflix.watch_history_robust`
UNION ALL
SELECT 'flag_age_extreme',
       ROUND(SUM(CASE WHEN age < 10 OR age > 100 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)
FROM `mgmt467project.netflix.users`
UNION ALL
SELECT 'flag_duration_anomaly',
       ROUND(SUM(CASE WHEN duration_minutes < 15 OR duration_minutes > 480 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2)
FROM `mgmt467project.netflix.movies`;
