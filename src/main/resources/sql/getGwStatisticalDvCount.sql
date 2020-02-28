select count(*) num_points
  from groundwater_statistical_daily_value
 where time_series_unique_id = ?
