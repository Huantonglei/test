select count(clue_id)
from realtime_dwd.dwd_evaluate_task as a
where  create_time between '2020-09-25 00:00:00' and '2020-09-25 23:59:59'
    and collect_clue_type=0
    and (market_channel_sub_class<>'e-sop其他' or cainfo_ca_s<>'sop_chesupai')  --渠道子类 ， --一级渠道
6781

select count(a.clue_id)
from guazi_dw_dwd.dwd_com_evaluate_task_ymd as a
join guazi_dw_dwd.dim_com_car_clue_ymd as b
    on b.dt = '${date_y_m_d}'
    and a.clue_id = b.clue_id
where  a.dt = '${date_y_m_d}'
    and a.create_time between @startTime and @endTime
    and a.collect_clue_type=0
    and (b.channel_sub_class<>'e-sop其他' or b.first_channel_identification<>'sop_chesupai')
6942

105825
select count(clue_id)
from realtime_dwd.dwd_car_clue as a
where  create_time between '2020-11-29 00:00:00' and '2020-11-29 23:59:59'
    and collect_clue_type not in (1,3)
    and car_clues_category<>19
    and is_direct=1
    and (market_channel_sub_class<>'e-sop其他' or ca_s<>'sop_chesupai')

select count(a.clue_id)
from (
    select clue_id
    from guazi_dw_dwd.dim_com_car_clue_ymd
    where dt='${date_y_m_d}'
    and create_time between @startTime and @endTime
        and collect_clue_type not in (1,3)
        and car_clues_category<>19
        and (channel_sub_class<>'e-sop其他' or channel_identification<>'sop_chesupai')  ) a
        left join (
            select clue_id
            from guazi_dw_dwb.dwb_evaluate_task_task_tag_day
            where dt='${date_y_m_d}'
                and label_id = 10
                and is_del = 0
                group by clue_id
                ) b on a.clue_id=b.clue_id
                  where b.clue_id is not null



  无  market_channel_sub_class,cainfo_ca_s

  mysql_source.clues___car_clues_channel_statistics_expand :ca_s，ca_n,channel

  dim_com_car_clue_ymd ->  ccse.channel as channel,  -- 渠道大类
        ccse.ca_s as channel_identification,    -- 渠道标识
        ccse.ca_n as first_channel_identification,    -- 一级渠道
        ccse.scode as second_channel_identification,   -- 二级渠道



