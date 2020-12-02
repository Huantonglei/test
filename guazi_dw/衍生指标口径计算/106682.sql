select count(clue_id) from realtime_dwd.dwd_consignment_snapshot as a
where  clue_create_time between '2020-10-24 00:00:00' and '2020-10-24 23:59:59'
	and is_consigned = 1
	and audit_time<>'1970-01-01 08:00:00'
	and substr(discard_time,1,4)='1970'
	and consign_price>=100000
131

select count(clue_id) from guazi_dw_dwd.dwd_consign_collect_contract_snapshot_ymd as a
where  dt = '${date_y_m_d}'
	and task_create_time between @startTime and @endTime
	and is_consigned = 1
	and audit_time<>'1970-01-01 08:00:00'
	and (substr(discard_time,1,4)='1970' or  discard_time is null )
	and consign_price>=100000
49




	discard_time 无时间  ->	and substr(discard_time,1,4)='1970' or discard_time is null
0