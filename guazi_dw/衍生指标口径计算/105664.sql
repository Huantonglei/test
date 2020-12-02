select count(distinct clue_id)
from realtime_dwd.dwd_recheck_task as a
where  task_created_time between '2020-10-24 00:00:00' and '2020-10-24 23:59:59' --复检工单创建时间
    and recheck_status in (5,6)
    and recheck_result<>9
    and source_type in (3,4,5,6,13,14,19)
    and recheck_before_audit = 1
    and order_type = 1
    and collect_clue_type = 0
1472

select count(distinct clue_id)
from guazi_dw_dwd.dwd_com_recheck_task_ymd as a
where   dt = '${date_y_m_d}'
	and created_at between @startTime and @endTime
    and recheck_status in (5,6)
    and recheck_result<>9
    and source_type in (3,4,5,6,13,14,19)
    and recheck_before_audit = 1
    and order_type = 1
    and collect_clue_type = 0

task_created_time 离线表没有（created_at 可能是）
1415