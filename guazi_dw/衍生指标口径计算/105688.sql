select count(clue_id) from realtime_dwd.dwd_consign_task
where task_created_time between '2020-10-24 00:00:00' and '2020-10-24 23:59:59'
	and is_open_platform = 0
	and first_consign_type = 0
	and collect_clue_type = 0


select count(clue_id) from guazi_dw_dwd.dwd_consign_task_ymd as a
where  dt = '2020-10-24'
	and task_created_at between '2020-10-24 00:00:00' and '2020-10-24 23:59:59'
	and is_open_platform = 0
	and first_consign_type = 0
	and collect_clue_type = 0


select count(clue_id) from guazi_dw_dwd.dwd_consign_task_ymd as a
where  dt = '${date_y_m_d}'
	and task_created_at between @startTime and @endTime
	and is_open_platform = 0
	and first_consign_type = 0
	and collect_clue_type = 0
1688