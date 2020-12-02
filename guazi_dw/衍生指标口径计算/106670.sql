select count(*)
from realtime_dwd.dwd_car_source as a
    where  create_time between '2020-09-26 00:00:00' and '2020-09-26 23:59:59'
    and collect_clue_type in (0, 3)
    and is_open_platform_clue=0
    and price>=100000
1222

select count(*)
from guazi_dw_dwd.dim_com_car_source_ymd as a
    where   dt = '${date_y_m_d}'
    and create_time between @startTime and @endTime
    and collect_clue_type in (0, 3)
    and is_open_platform_clue=0
    and price>=100000
1269