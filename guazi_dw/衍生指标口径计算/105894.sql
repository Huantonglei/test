select count(distinct clue_id) from realtime_dwd.dwd_recheck_task as a
where  source_type in (3, 4, 5, 6, 13, 14, 19) and recheck_status = 6
	and recheck_result in (-1, 21, 22)
	and submit_time between '2020-11-24 00:00:00' and '2020-11-24 23:59:59'
	and recheck_before_audit = 1
	and order_type = 1
	and collect_clue_type = 0
	and substr(submit_time, 1, 10) = substr(consign_audit_time, 1, 10)
220

select count(distinct clue_id) from guazi_dw_dwd.dwd_com_recheck_task_ymd as a
where  dt = '${date_y_m_d}'
	and submit_time between @startTime and @endTime
	and source_type in (3, 4, 5, 6, 13, 14, 19)
	and recheck_status = 6 and recheck_result in (-1, 21, 22)
	and recheck_before_audit = 1
	and order_type = 1
	and collect_clue_type = 0
	and substr(submit_time, 1, 10) = substr(audit_time, 1, 10)
226


-- select count(distinct clue_id)
-- from guazi_dw_dwd.dwd_com_recheck_task_ymd as a
--  join guazi_dw_dwd.dwd_consign_collect_car_ymd as b
--      on b.dt = '2020-09-24'
--      and b.consign_audit_time is not null
--      and a.clue_id = b.clue_id
-- where  a.dt = '2020-09-24'
-- 	and a.submit_time between '2020-11-24 00:00:00' and '2020-11-24 23:59:59'
-- 	and a.source_type in (3, 4, 5, 6, 13, 14, 19)
-- 	and a.recheck_status = 6
-- 	and a.recheck_result in (-1, 21, 22)
-- 	and a.recheck_before_audit = 1
-- 	and a.order_type = 1
-- 	and a.collect_clue_type = 0
-- 	and substr(a.submit_time, 1, 10) = substr(b.consign_audit_time, 1, 10)



dwd_com_recheck_task_ymd 无   consign_audit_time <- dwd_consign_collect_car_ymd ( clue_id 做join)