select count(clue_id)
from realtime_dwd.dwd_evaluate_task as a
where  create_time between '2020-09-25 00:00:00' and '2020-09-25 23:59:59' --创建时间
    and collect_clue_type=0
    and is_direct=1
    and (market_channel_sub_class<>'e-sop其他' or cainfo_ca_s<>'sop_chesupai')  --渠道子类名称-市场部专用 or  一级渠道
1590

select count(a.clue_id)
from guazi_dw_dwd.dwd_com_evaluate_task_ymd as a
join guazi_dw_dwd.dim_com_car_clue_ymd as b
    on b.dt = '${date_y_m_d}'
    and a.clue_id = b.clue_id
where a.dt = '${date_y_m_d}'
    and a.create_time between @startTime and @endTime --记录时间（评估）
    and a.collect_clue_type=0
    and a.is_direct=1
    and (b.channel_sub_class<>'e-sop其他' or b.first_channel_identification<>'sop_chesupai')

1628


无 market_channel_sub_class
    cainfo_ca_s


    guazi_dw_dm.dm_car_venus_recheck_evaluator_quality_detail_ymd -> market_channel_sub_class --市场部渠道子类
    guazi_dw_dm.dm_call_venus_keyword_clue_stat_ymd -> ca_s 一级渠道