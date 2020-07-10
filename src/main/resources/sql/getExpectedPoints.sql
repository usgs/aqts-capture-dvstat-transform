select num_points
  from time_series_header_info
 where json_data_id = ?
   and partition_number = ?
 