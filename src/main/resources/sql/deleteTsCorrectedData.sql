delete
  from groundwater_statistical_daily_value
 where exists
    (select null
       from time_series_header_info
      where json_data_id = ? and
            groundwater_statistical_daily_value.time_series_unique_id = time_series_header_info.time_series_unique_id and
            groundwater_statistical_daily_value.time_step between time_series_header_info.time_range_start::date and
            time_series_header_info.time_range_end::date and
            partition_number = ?
    )
