select count(*)
from realtime_dwd.dwd_car_source as a
where  is_agreement_channel_clue = 0
    and create_time between '2020-09-25 00:00:00' and '2020-09-25 23:59:59'
    and collect_clue_type = 1
482

select count(*)
from guazi_dw_dwd.dim_com_car_source_ymd as a
where dt = '${date_y_m_d}'
    and create_time between @startTime and @endTime
    and is_virtual_shelf = 0
    and collect_clue_type = 1
482



