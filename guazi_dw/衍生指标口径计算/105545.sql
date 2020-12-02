select count(*) from realtime_dwd.dwd_recheck_task as a
where  recheck_status = 6 and recheck_result in (-1, 21, 22)
	and source_type in (3, 4, 5, 6, 13, 14, 19)
	and submit_time between '2020-11-24 00:00:00' and '2020-11-24 23:59:59'
	and recheck_before_audit = 1
	and order_type = 1
	and collect_clue_type = 0

	1494


select count(*) from guazi_dw_dwd.dwd_com_recheck_task_ymd
where  dt = '2020-11-24'
	and submit_time between '2020-11-24 00:00:00' and '2020-11-24 23:59:59'
	and recheck_status = 6
	and recheck_result in (-1, 21, 22)
	and source_type in (3, 4, 5, 6, 13, 14, 19)
	and recheck_before_audit = 1
	and order_type = 1
	and collect_clue_type = 0

select count(*) from guazi_dw_dwd.dwd_com_recheck_task_ymd
where  dt = '${date_y_m_d}'
 and submit_time between @startTime and @endTime
 and recheck_status = 6
 and recheck_result in (-1, 21, 22)
 and source_type in (3, 4, 5, 6, 13, 14, 19)
 and recheck_before_audit = 1
 and order_type = 1
 and collect_clue_type = 0

	1506