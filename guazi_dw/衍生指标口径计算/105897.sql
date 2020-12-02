select count(clue_id)
from realtime_dwd.dwd_consign_task as a
where  task_created_time between '2020-10-24 00:00:00' and '2020-10-24 23:59:59'
    and is_open_platform = 0
    and first_consign_type = 0
    and collect_clue_type = 0
    and substr(consign_audit_time, 1, 10) = substr(task_created_time, 1, 10)

151


select count(a.clue_id)
from guazi_dw_dwd.dwd_consign_task_ymd as a
  join guazi_dw_dwd.dwd_consign_collect_car_ymd as b
   on b.dt = '${date_y_m_d}'
    and a.clue_id = b.clue_id
where  a.dt = '${date_y_m_d}'
    and a.task_created_at between @startTime and @endTime
    and a.is_open_platform = 0
    and a.first_consign_type = 0
    and a.collect_clue_type = 0
    and substr(b.consign_audit_time, 1, 10) = substr(a.task_created_at, 1, 10)
152

dt = '${date_y_m_d}'
    and evaluate_submit_time between @startTime and @endTime
consign_audit_time 没有，在 dwd_consign_collect_car_ymd ( clue_id 做join)