with ts_description
  as (select time_series_unique_id,
             (regexp_match(location_identifier, '(\d*)-*(.*)'))[1] location_identifier,
             coalesce(nullif((regexp_match(location_identifier, '(\d*)-*(.*)'))[2], ''), 'USGS') agency_code,
             parm_cd,
             parameter,
             stat_cd,
             unit
        from time_series_description
     )
insert
  into groundwater_statistical_daily_value (
           groundwater_daily_value_identifier,
           time_series_unique_id,
           monitoring_location_identifier,
           observered_property_id,
           statistic_id,
           time_step,
           unit_of_measure,
           result,
           approvals,
           qualifiers,
           grades
       )
select ts_description.agency_code || 
           '-' || ts_description.location_identifier ||
           '-' || ts_description.time_series_unique_id groundwater_daily_value_identifier,
       ts_description.time_series_unique_id,
       ts_description.agency_code || 
           '-' || ts_description.location_identifier monitoring_location_identifier,
       ts_description.parm_cd observered_property_id,
       ts_description.stat_cd statistic_id,
       time_series_points.time_step,
       ts_description.unit unit_of_measure,
       time_series_points.display_value result,
       (select array_to_json(array_agg(time_series_approvals.level_description))
          from time_series_approvals
         where time_series_points.json_data_id = time_series_approvals.json_data_id and
               time_series_approvals.start_time <= time_series_points.time_step and
               time_series_points.time_step < time_series_approvals.end_time
       ) approvals,
       (select array_to_json(array_agg(time_series_qualifiers.identifier))
          from time_series_qualifiers
         where time_series_points.json_data_id = time_series_qualifiers.json_data_id and
               time_series_qualifiers.start_time <= time_series_points.time_step and
               time_series_points.time_step < time_series_qualifiers.end_time
       ) qualifiers,
       (select array_to_json(array_agg(time_series_grades.grade_code))
          from time_series_grades
         where time_series_points.json_data_id = time_series_grades.json_data_id and
               time_series_grades.start_time <= time_series_points.time_step and
               time_series_points.time_step < time_series_grades.end_time
       ) grades
  from time_series_header_info
       join time_series_points
         on time_series_header_info.json_data_id = time_series_points.json_data_id
       join ts_description
         on time_series_header_info.time_series_unique_id = ts_description.time_series_unique_id
 where time_series_header_info.json_data_id = ?
