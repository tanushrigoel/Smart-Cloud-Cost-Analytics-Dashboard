-- queries/cost_optimization.sql
-- Advanced cost optimization queries for the Cost Analytics Dashboard

-- 1. Identify unused or idle resources
WITH resource_usage AS (
  SELECT 
    project.id as project_id,
    service.description as service_name,
    sku.description as resource_type,
    location.location,
    DATE(usage_start_time) as usage_date,
    SUM(usage.amount) as usage_amount,
    SUM(cost) as daily_cost,
    COUNT(*) as usage_records
  FROM `{project_id}.{dataset_id}.{table_id}`
  WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND service.description IN ('Compute Engine', 'Cloud Storage', 'Cloud SQL')
  GROUP BY project_id, service_name, resource_type, location.location, usage_date
),
idle_resources AS (
  SELECT 
    project_id,
    service_name,
    resource_type,
    location,
    COUNT(DISTINCT usage_date) as active_days,
    AVG(usage_amount) as avg_usage,
    SUM(daily_cost) as total_cost,
    CASE 
      WHEN AVG(usage_amount) < 0.1 THEN 'Potential Waste'
      WHEN COUNT(DISTINCT usage_date) < 15 THEN 'Underutilized'
      ELSE 'Active'
    END as usage_status
  FROM resource_usage
  GROUP BY project_id, service_name, resource_type, location
)
SELECT 
  project_id,
  service_name,
  resource_type,
  location,
  usage_status,
  ROUND(total_cost, 2) as monthly_cost,
  ROUND(avg_usage, 4) as avg_daily_usage,
  active_days,
  CASE 
    WHEN usage_status = 'Potential Waste' THEN ROUND(total_cost * 0.9, 2)
    WHEN usage_status = 'Underutilized' THEN ROUND(total_cost * 0.5, 2)
    ELSE 0
  END as potential_savings
FROM idle_resources
WHERE usage_status IN ('Potential Waste', 'Underutilized')
ORDER BY potential_savings DESC;

-- 2. Cost trend analysis with seasonality detection
WITH daily_costs AS (
  SELECT 
    DATE(usage_start_time) as cost_date,
    EXTRACT(DAYOFWEEK FROM usage_start_time) as day_of_week,
    EXTRACT(HOUR FROM usage_start_time) as hour_of_day,
    project.id as project_id,
    service.description as service_name,
    SUM(cost + IFNULL((SELECT SUM(amount) FROM UNNEST(credits)), 0)) as net_cost
  FROM `{project_id}.{dataset_id}.{table_id}`
  WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  GROUP BY cost_date, day_of_week, hour_of_day, project_id, service_name
),
trend_analysis AS (
  SELECT 
    cost_date,
    project_id,
    service_name,
    net_cost,
    -- 7-day moving average
    AVG(net_cost) OVER (
      PARTITION BY project_id, service_name 
      ORDER BY cost_date 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7d,
    -- Week-over-week growth
    LAG(net_cost, 7) OVER (
      PARTITION BY project_id, service_name 
      ORDER BY cost_date
    ) as cost_7d_ago,
    -- Standard deviation for volatility
    STDDEV(net_cost) OVER (
      PARTITION BY project_id, service_name 
      ORDER BY cost_date 
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) as cost_volatility
  FROM daily_costs
)
SELECT 
  cost_date,
  project_id,
  service_name,
  ROUND(net_cost, 2) as daily_cost,
  ROUND(moving_avg_7d, 2) as weekly_avg,
  ROUND(((net_cost - cost_7d_ago) / NULLIF(cost_7d_ago, 0)) * 100, 2) as wow_growth_pct,
  ROUND(cost_volatility, 2) as volatility,
  CASE 
    WHEN ABS(net_cost - moving_avg_7d) > 2 * cost_volatility THEN 'Anomaly'
    WHEN ((net_cost - cost_7d_ago) / NULLIF(cost_7d_ago, 0)) > 0.2 THEN 'High Growth'
    WHEN ((net_cost - cost_7d_ago) / NULLIF(cost_7d_ago, 0)) < -0.2 THEN 'Declining'
    ELSE 'Stable'
  END as trend_status
FROM trend_analysis
WHERE cost_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY cost_date DESC, net_cost DESC;

-- 3. Service-level cost breakdown with recommendations
WITH service_costs AS (
  SELECT 
    project.id as project_id,
    service.description as service_name,
    sku.description as sku_name,
    location.location,
    DATE_TRUNC(DATE(usage_start_time), MONTH) as cost_month,
    SUM(cost) as gross_cost,
    SUM(IFNULL((SELECT SUM(amount) FROM UNNEST(credits)), 0)) as total_credits,
    SUM(cost + IFNULL((SELECT SUM(amount) FROM UNNEST(credits)), 0)) as net_cost,
    SUM(usage.amount) as total_usage
  FROM `{project_id}.{dataset_id}.{table_id}`
  WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
  GROUP BY project_id, service_name, sku_name, location.location, cost_month
),
service_summary AS (
  SELECT 
    project_id,
    service_name,
    COUNT(DISTINCT sku_name) as sku_count,
    COUNT(DISTINCT location) as location_count,
    SUM(net_cost) as total_cost,
    AVG(net_cost) as avg_monthly_cost,
    MAX(net_cost) - MIN(net_cost) as cost_range,
    STDDEV(net_cost) as cost_std,
    -- Growth rate calculation
    (SUM(CASE WHEN cost_month = DATE_TRUNC(CURRENT_DATE(), MONTH) THEN net_cost END) - 
     SUM(CASE WHEN cost_month = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH) THEN net_cost END)) /
    NULLIF(SUM(CASE WHEN cost_month = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH) THEN net_cost END), 0) as mom_growth
  FROM service_costs
  GROUP BY project_id, service_name
)
SELECT 
  project_id,
  service_name,
  ROUND(total_cost, 2) as six_month_total,
  ROUND(avg_monthly_cost, 2) as avg_monthly,
  ROUND(mom_growth * 100, 2) as mom_growth_pct,
  sku_count,
  location_count,
  CASE 
    WHEN mom_growth > 0.3 THEN 'Review for optimization - High growth'
    WHEN cost_std / NULLIF(avg_monthly_cost, 0) > 0.5 THEN 'Investigate volatility'
    WHEN location_count > 3 THEN 'Consider location consolidation'
    WHEN sku_count > 10 THEN 'Review SKU utilization'
    ELSE 'Monitoring recommended'
  END as recommendation,
  ROUND(CASE 
    WHEN mom_growth > 0.2 THEN avg_monthly_cost * 0.15
    WHEN location_count > 3 THEN avg_monthly_cost * 0.10
    ELSE avg_monthly_cost * 0.05
  END, 2) as potential_monthly_savings
FROM service_summary
WHERE total_cost > 100  -- Focus on services with significant spend
ORDER BY total_cost DESC;

-- 4. Advanced anomaly detection with machine learning approach
WITH cost_features AS (
  SELECT 
    DATE(usage_start_time) as cost_date,
    project.id as project_id,
    service.description as service_name,
    EXTRACT(DAYOFWEEK FROM usage_start_time) as day_of_week,
    EXTRACT(MONTH FROM usage_start_time) as month,
    SUM(cost + IFNULL((SELECT SUM(amount) FROM UNNEST(credits)), 0)) as daily_cost,
    -- Feature engineering
    LAG(SUM(cost + IFNULL((SELECT SUM(amount) FROM UNNEST(credits)), 0)), 1) OVER (
      PARTITION BY project.id, service.description 
      ORDER BY DATE(usage_start_time)
    ) as prev_day_cost,
    LAG(SUM(cost + IFNULL((SELECT SUM(amount) FROM UNNEST(credits)), 0)), 7) OVER (
      PARTITION BY project.id, service.description 
      ORDER BY DATE(usage_start_time)
    ) as same_day_prev_week
  FROM `{project_id}.{dataset_id}.{table_id}`
  WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
  GROUP BY cost_date, project_id, service_name, day_of_week, month
),
statistical_model AS (
  SELECT 
    *,
    -- Calculate rolling statistics
    AVG(daily_cost) OVER (
      PARTITION BY project_id, service_name 
      ORDER BY cost_date 
      ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
    ) as rolling_mean,
    STDDEV(daily_cost) OVER (
      PARTITION BY project_id, service_name 
      ORDER BY cost_date 
      ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
    ) as rolling_std,
    -- Seasonal adjustment
    AVG(daily_cost) OVER (
      PARTITION BY project_id, service_name, day_of_week 
      ORDER BY cost_date 
      ROWS BETWEEN 21 PRECEDING AND 1 PRECEDING
    ) as seasonal_mean
  FROM cost_features
  WHERE prev_day_cost IS NOT NULL AND same_day_prev_week IS NOT NULL
),
anomaly_scores AS (
  SELECT 
    *,
    -- Z-score based on rolling statistics
    (daily_cost - rolling_mean) / NULLIF(rolling_std, 0) as z_score,
    -- Seasonal anomaly score
    (daily_cost - seasonal_mean) / NULLIF(seasonal_mean, 0) as seasonal_anomaly,
    -- Day-over-day change
    (daily_cost - prev_day_cost) / NULLIF(prev_day_cost, 0) as dod_change,
    -- Week-over-week change
    (daily_cost - same_day_prev_week) / NULLIF(same_day_prev_week, 0) as wow_change
  FROM statistical_model
  WHERE rolling_mean IS NOT NULL
)
SELECT 
  cost_date,
  project_id,
  service_name,
  ROUND(daily_cost, 2) as actual_cost,
  ROUND(rolling_mean, 2) as expected_cost,
  ROUND(z_score, 2) as anomaly_z_score,
  ROUND(seasonal_anomaly * 100, 2) as seasonal_deviation_pct,
  ROUND(dod_change * 100, 2) as day_over_day_pct,
  ROUND(wow_change * 100, 2) as week_over_week_pct,
  CASE 
    WHEN ABS(z_score) > 3 THEN 'Critical'
    WHEN ABS(z_score) > 2 THEN 'High'
    WHEN ABS(seasonal_anomaly) > 0.5 THEN 'Seasonal'
    WHEN ABS(dod_change) > 1 THEN 'Spike'
    ELSE 'Normal'
  END as anomaly_type,
  ROUND(daily_cost - rolling_mean, 2) as cost_impact
FROM anomaly_scores
WHERE cost_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND (ABS(z_score) > 2 OR ABS(seasonal_anomaly) > 0.3 OR ABS(dod_change) > 0.5)
ORDER BY ABS(z_score) DESC, cost_date DESC;

-- 5. Resource rightsizing recommendations
WITH compute_usage AS (
  SELECT 
    project.id as project_id,
    sku.description as instance_type,
    location.location as region,
    DATE(usage_start_time) as usage_date,
    SUM(usage.amount) as usage_hours,
    SUM(cost) as daily_cost,
    -- Extract instance specs from SKU description
    REGEXP_EXTRACT(sku.description, r'(\d+) vCPU') as vcpu_count,
    REGEXP_EXTRACT(sku.description, r'(\d+\.?\d*) GB') as memory_gb
  FROM `{project_id}.{dataset_id}.{table_id}`
  WHERE service.description = 'Compute Engine'
    AND sku.description LIKE '%Instance%'
    AND DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY project_id, instance_type, region, usage_date, sku.description
),
utilization_analysis AS (
  SELECT 
    project_id,
    instance_type,
    region,
    CAST(vcpu_count AS INT64) as vcpus,
    CAST(memory_gb AS FLOAT64) as memory,
    COUNT(DISTINCT usage_date) as active_days,
    AVG(usage_hours) as avg_daily_hours,
    SUM(daily_cost) as total_cost,
    AVG(daily_cost) as avg_daily_cost,
    -- Calculate utilization rate
    AVG(usage_hours) / 24.0 as utilization_rate
  FROM compute_usage
  WHERE vcpu_count IS NOT NULL AND memory_gb IS NOT NULL
  GROUP BY project_id, instance_type, region, vcpus, memory
),
rightsizing_recommendations AS (
  SELECT 
    *,
    CASE 
      WHEN utilization_rate < 0.2 THEN 'Downsize significantly'
      WHEN utilization_rate < 0.4 THEN 'Consider downsizing'
      WHEN utilization_rate > 0.8 THEN 'Consider upsizing'
      ELSE 'Right-sized'
    END as recommendation,
    CASE 
      WHEN utilization_rate < 0.2 THEN total_cost * 0.6  -- 60% savings
      WHEN utilization_rate < 0.4 THEN total_cost * 0.3  -- 30% savings
      ELSE 0
    END as potential_savings
  FROM utilization_analysis
  WHERE active_days >= 15  -- Only consider instances with significant usage
)
SELECT 
  project_id,
  instance_type,
  region,
  vcpus,
  memory,
  active_days,
  ROUND(utilization_rate * 100, 2) as utilization_pct,
  ROUND(total_cost, 2) as monthly_cost,
  recommendation,
  ROUND(potential_savings, 2) as monthly_savings,
  -- Specific rightsizing suggestion
  CASE 
    WHEN utilization_rate < 0.2 AND vcpus > 2 THEN CONCAT('Reduce to ', CAST(GREATEST(1, CAST(vcpus/2 AS INT64)) AS STRING), ' vCPUs')
    WHEN utilization_rate < 0.4 AND memory > 4 THEN CONCAT('Reduce memory to ', CAST(GREATEST(2, memory/2) AS STRING), ' GB')
    WHEN utilization_rate > 0.8 THEN 'Monitor for performance issues'
    ELSE 'No change needed'
  END as specific_action
FROM rightsizing_recommendations
WHERE recommendation != 'Right-sized' OR utilization_rate > 0.8
ORDER BY potential_savings DESC, utilization_rate ASC;