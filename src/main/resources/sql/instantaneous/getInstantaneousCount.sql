select count(*) num_points
  from instantaneous_value
 where time_series_unique_id = ?
