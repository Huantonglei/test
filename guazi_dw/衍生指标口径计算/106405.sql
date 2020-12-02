select count(*)
from realtime_dwd.dwd_car_source as a
where  collect_clue_type = 0
    and create_time between '2020-09-26 00:00:00' and '2020-09-26 23:59:59'
    and is_agreement_channel_clue = 0
    and (market_channel_sub_class<>'e-sop其他' or cainfo_ca_s<>'sop_chesupai')
4047

select count(*)
from guazi_dw_dwd.dim_com_car_source_ymd as a
join guazi_dw_dwd.dim_com_car_clue_ymd as b
    on b.dt = '${date_y_m_d}'
    and a.clue_id = b.clue_id
where  a.dt =  '${date_y_m_d}'
    and a.collect_clue_type = 0
    and a.create_time between @startTime and @endTime
    and a.is_virtual_shelf = 0
   and (b.channel_sub_class<>'e-sop其他' or b.first_channel_identification<>'sop_chesupai')
3792