select count(*)
from realtime_dwd.dwd_car_source as a
where  collect_clue_type = 0
	and create_time between '2020-09-24 00:00:00' and '2020-09-24 23:59:59'
	and is_agreement_channel_clue = 0 --是否尊享渠道线索   否
	and is_direct = 0
3724


select count(*)
from guazi_dw_dwd.dim_com_car_source_ymd as a
where  dt = '2020-09-24'
	and collect_clue_type = 0
	and create_time between '2020-09-24 00:00:00' and '2020-09-24 23:59:59'
	and is_virtual_shelf = 0
	and is_direct = 0





