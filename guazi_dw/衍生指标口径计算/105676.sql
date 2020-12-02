select count(clue_id) from realtime_dwd.dwd_evaluate_task as a
where  create_time between '2020-11-24 00:00:00' and '2020-11-24 23:59:59'
	and fixed_id >0
	and collect_clue_type in (0, 3)
	and collect_clue_type = 0
8277

select count(clue_id) from guazi_dw_dwd.dwd_com_evaluate_task_ymd as a
where  dt = '2020-11-24'
	and create_time between '2020-11-24 00:00:00' and '2020-11-24 23:59:59'
	and fixed_id >0
	and collect_clue_type in (0, 3) and collect_clue_type = 0

select count(clue_id) from guazi_dw_dwd.dwd_com_evaluate_task_ymd as a
where  dt = '${date_y_m_d}'
	and create_time between @startTime and @endTime
	and fixed_id >0
	and collect_clue_type in (0, 3) and collect_clue_type = 0
8642