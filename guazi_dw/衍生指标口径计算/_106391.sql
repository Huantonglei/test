select count(clue_id)
from realtime_dwd.dwd_consign_task as a
where  consign_source_type = 4
    and task_created_time between '2020-09-25 00:00:00' and '2020-09-25 23:59:59'
    and is_consigned = 1
    and consign_task_status in (0,1,2,3,4,5,8,21,23,29)
    and car_source_status in (0,3)
    and '2020-09-25 00:00:00' <> ''
    and evaluate_editor > 0

251

车源私海线索量(当日新增线索)

select count(clue_id)
from guazi_dw_dwd.dwd_consign_task_ymd as a
where dt = '${date_y_m_d}'
    and task_created_at between @startTime and @endTime
    and consign_source_type = 4
    and is_consigned = 1
    and consign_task_status in (0,1,2,3,4,5,8,21,23,29)
    and car_source_status in (0,3)
    and @startTime <> ''
    and evaluate_editor > 0

1245

