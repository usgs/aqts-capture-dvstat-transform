delete
  from instantaneous_value
where instantaneous_value_id in
   (select instantaneous_value_id
      from time_series_header_info
      join instantaneous_value
        on instantaneous_value.time_series_unique_id = time_series_header_info.time_series_unique_id
     where json_data_id = ? and
           instantaneous_value.time_step between time_series_header_info.time_range_start::date and
           time_series_header_info.time_range_end::date and
           partition_number = ?
   )