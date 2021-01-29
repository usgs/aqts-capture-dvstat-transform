with ts_description
  as (select time_series_unique_id,
             (regexp_match(location_identifier, '(\d*)-*(.*)'))[1] location_identifier,
             coalesce(nullif((regexp_match(location_identifier, '(\d*)-*(.*)'))[2], ''), 'USGS') agency_code,
             parm_cd,
             parameter,
             stat_cd,
             unit
        from time_series_description
       where time_series_unique_id = ?
     )
update instantaneous_value
   set instantaneous_value_identifier = ts_description.agency_code || 
           '-' || ts_description.location_identifier ||
           '-' || ts_description.time_series_unique_id,
       monitoring_location_identifier = ts_description.agency_code || 
           '-' || ts_description.location_identifier,
       parameter_code = ts_description.parm_cd,
       statistic_id = ts_description.stat_cd,
       unit_of_measure = ts_description.unit
  from ts_description
 where instantaneous_value.time_series_unique_id = ts_description.time_series_unique_id
