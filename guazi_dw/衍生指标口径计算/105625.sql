select count(collect_id) from realtime_dwd.dwd_national_purchase_orders as a
where  collect_start_time between '2020-11-24 00:00:00' and '2020-11-24 23:59:59'
	and qgg_type in ('2101','2102')
	and collect_clue_type = 0

select count(collect_id) from guazi_dw_dwd.dwd_national_purchase_orders_ymd
where dt = '${date_y_m_d}'
	and collect_start_time between @startTime and @endTime
	and qgg_type in ('2101','2102')
	and collect_clue_type = 0