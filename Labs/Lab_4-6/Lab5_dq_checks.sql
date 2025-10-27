-- =====================================================
-- MGMT 467 Data Quality (DQ) Queries - Netflix Dataset
-- Author: Anurag Koripalli
-- =====================================================
-- Task 1: Prepare Churn Features Table
CREATE OR REPLACE TABLE `mgmt467project.netflix.churn_features` AS
SELECT
  user_id,
  age,
  gender,
  country,
  subscription_plan,
  monthly_spend,
  household_size,
  primary_device,
  churn_label
FROM `mgmt467project.netflix.cleaned_features`;

-- Task 2: Train Logistic Regression Model
CREATE OR REPLACE MODEL `mgmt467project.netflix.churn_model`
OPTIONS(
  model_type = 'logistic_reg',
  input_label_cols = ['churn_label']
) AS
SELECT
  age,
  gender,
  country,
  subscription_plan,
  monthly_spend,
  household_size,
  primary_device,
  churn_label
FROM `mgmt467project.netflix.churn_features`;

-- Task 3: Evaluate Model
SELECT * 
FROM ML.EVALUATE(MODEL `mgmt467project.netflix.churn_model`);

-- Task 4: Predict Churn
SELECT
  user_id,
  predicted_churn_label,
  predicted_churn_label_probs[OFFSET(1)] AS churn_probability
FROM ML.PREDICT(
  MODEL `mgmt467project.netflix.churn_model`,
  (
    SELECT
      user_id,
      age,
      gender,
      country,
      subscription_plan,
      monthly_spend,
      household_size,
      primary_device,
      churn_label
    FROM `mgmt467project.netflix.cleaned_features`
  )
);

-- Task 5.0: Bucket a Continuous Feature
CREATE OR REPLACE TABLE `mgmt467project.netflix.watch_time_buckets` AS
SELECT
  wh.user_id,
  SUM(wh.watch_duration_minutes) AS total_minutes,
  cf.churn_label,
  CASE
    WHEN SUM(wh.watch_duration_minutes) < 100 THEN 'Low'
    WHEN SUM(wh.watch_duration_minutes) BETWEEN 100 AND 300 THEN 'Medium'
    WHEN SUM(wh.watch_duration_minutes) > 300 THEN 'High'
    ELSE 'Unknown'
  END AS watch_time_bucket
FROM `mgmt467project.netflix.watch_history_dedup` AS wh
JOIN `mgmt467project.netflix.cleaned_features` AS cf
USING (user_id)
GROUP BY wh.user_id, cf.churn_label;

-- Task 5.1: Create Binary Flag Feature
CREATE OR REPLACE TABLE `mgmt467project.netflix.binge_flag_features` AS
SELECT
  user_id,
  total_minutes,
  churn_label,
  IF(total_minutes > 500, 1, 0) AS flag_binge
FROM `mgmt467project.netflix.watch_time_buckets`;

-- Task 5.2: Create Interaction Term
CREATE OR REPLACE TABLE `mgmt467project.netflix.plan_region_combo_features` AS
SELECT
  user_id,
  subscription_plan AS plan_tier,
  country AS region,
  churn_label,
  CONCAT(subscription_plan, '_', country) AS plan_region_combo
FROM `mgmt467project.netflix.cleaned_features`
WHERE churn_label IS NOT NULL;

-- Task 5.3: Add Missingness Flags
CREATE OR REPLACE TABLE `mgmt467project.netflix.missingness_flags` AS
SELECT
  u.user_id,
  u.age AS avg_age,
  wh.user_rating AS avg_rating,
  cf.churn_label,
  IF(u.age IS NULL, 1, 0) AS is_missing_age_band,
  IF(wh.user_rating IS NULL, 1, 0) AS is_missing_avg_rating
FROM `mgmt467project.netflix.users` AS u
LEFT JOIN `mgmt467project.netflix.watch_history_dedup` AS wh
ON u.user_id = wh.user_id
LEFT JOIN `mgmt467project.netflix.cleaned_features` AS cf
ON u.user_id = cf.user_id;

-- Task 5.4: Create Time-Based Feature
CREATE OR REPLACE TABLE `mgmt467project.netflix.time_features` AS
SELECT
  wh.user_id,
  MAX(wh.watch_date) AS last_watch_date,
  cf.churn_label,
  DATE_DIFF(CURRENT_DATE(), MAX(wh.watch_date), DAY) AS days_since_last_watch
FROM `mgmt467project.netflix.watch_history_dedup` AS wh
JOIN `mgmt467project.netflix.cleaned_features` AS cf
USING (user_id)
GROUP BY user_id, churn_label;

-- Task 5.5: Assemble Enhanced Feature Table
CREATE OR REPLACE TABLE `mgmt467project.netflix.churn_features_enhanced` AS
SELECT
  cf.user_id,
  cf.age,
  cf.gender,
  cf.country,
  cf.subscription_plan,
  cf.monthly_spend,
  cf.household_size,
  cf.primary_device,
  cf.churn_label,
  CONCAT(cf.subscription_plan, '_', cf.country) AS plan_region_combo,
  IF(whb.total_minutes > 500, 1, 0) AS flag_binge,
  whb.watch_time_bucket,
  tf.days_since_last_watch,
  mf.is_missing_age_band,
  mf.is_missing_avg_rating
FROM `mgmt467project.netflix.cleaned_features` AS cf
LEFT JOIN `mgmt467project.netflix.watch_time_buckets` AS whb
ON cf.user_id = whb.user_id
LEFT JOIN `mgmt467project.netflix.missingness_flags` AS mf
ON cf.user_id = mf.user_id
LEFT JOIN `mgmt467project.netflix.time_features` AS tf
ON cf.user_id = tf.user_id;

-- Task 6: Retrain Model on Enhanced Features
CREATE OR REPLACE MODEL `mgmt467project.netflix.churn_model_enhanced`
OPTIONS(
  model_type = 'logistic_reg',
  input_label_cols = ['churn_label']
) AS
SELECT
  age,
  gender,
  country,
  subscription_plan,
  monthly_spend,
  household_size,
  primary_device,
  plan_region_combo,
  flag_binge,
  watch_time_bucket,
  days_since_last_watch,
  is_missing_age_band,
  is_missing_avg_rating,
  churn_label
FROM `mgmt467project.netflix.churn_features_enhanced`;

-- Task 7: Compare Model Performance
WITH base_eval AS (
  SELECT 'Base Model' AS model_name, *
  FROM ML.EVALUATE(MODEL `mgmt467project.netflix.churn_model`)
),
enhanced_eval AS (
  SELECT 'Enhanced Model' AS model_name, *
  FROM ML.EVALUATE(MODEL `mgmt467project.netflix.churn_model_enhanced`)
)
SELECT * FROM base_eval
UNION ALL
SELECT * FROM enhanced_eval;
