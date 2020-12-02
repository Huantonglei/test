select count(*) from realtime_dwd.dwd_recheck_task as a
where  source_type in (3, 4, 5, 6, 13, 14, 19)
	and recheck_status in (5, 6)
	and substr(clue_created_time, 1, 10) = substr(submit_time, 1, 10)
	and clue_created_time between '2020-10-24 00:00:00' and '2020-10-24 23:59:59'
	and recheck_result <> 9
	and collect_clue_type = 0
1128

select count(*) from guazi_dw_dwd.dwd_com_recheck_task_ymd as a
where dt = '${date_y_m_d}'
	and source_type in (3, 4, 5, 6, 13, 14, 19)
	and recheck_status in (5, 6)
	and substr(clue_created_time, 1, 10) = substr(submit_time, 1, 10)
	and clue_created_time between @startTime and @endTime
	and recheck_result <> 9
	and collect_clue_type = 0
1165