select count(*) from realtime_dwd.dwd_evaluate_task as a
where  collect_clue_type = 0
	and create_time between '2020-10-24 00:00:00' and '2020-10-24 23:59:59'
	and fixed_id = 0
4287

select count(*)
from guazi_dw_dwd.dwd_com_evaluate_task_ymd as a
where  dt = '${date_y_m_d}'
	and collect_clue_type = 0
	and create_time between @startTime and @endTime
	and fixed_id = 0
3743