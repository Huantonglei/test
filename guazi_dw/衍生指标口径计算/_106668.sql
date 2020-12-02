select count(clue_id)
from realtime_dwd.dwd_evaluate_task as a
where  create_time between '2020-09-26 00:00:00' and '2020-09-26 23:59:59'
    and collect_clue_type in (0, 3)
    and is_open_platform=0
    and price_model>=100000
1626


select count(a.clue_id)
from guazi_dw_dwd.dwd_com_evaluate_task_ymd as a
    join guazi_dw_dwb.dwb_clues_vehicle_c2c_car_clues_expand_day as b
        on b.dt = '${date_y_m_d}'
        and a.clue_id = b.clue_id
        and b.price_model >=100000
where  a.dt = '${date_y_m_d}'
    and a.create_time between @startTime and @endTime
    and a.collect_clue_type in (0, 3)
    and a.is_open_platform=0
1686


select count(distinct a.clue_id)
from (select clue_id
    from guazi_dw_dwd.dim_com_car_clue_ymd
    where dt = '${date_y_m_d}' and create_time between @startTime and @endTime
        and clue_id>0
        and car_clues_category<>19
        and scode<>'107054650010000') a
        left join (
        select clue_id,price_model
            from guazi_dw_dwb.dwb_clues_vehicle_c2c_car_clues_expand_day
            where dt = '${date_y_m_d}') b
            on a.clue_id=b.clue_id
            where b.price_model>=100000







clues___vehicle_c2c_car_clues == > guazi_dw_dwb.dwb_clues_vehicle_c2c_car_clues_day
select
a.clue_id,
a.create_time,
b.collect_clue_type,
case when b.scode = '107054650010000' then 1 else 0 end as is_open_platform,
c.price_model
from
(select * from guazi_dw_dwb.dwb_evaluate_task_evaluate_task_day where dt ='2020-10-24') a
left join
(select  id,
        source_type_code as scode,
        case when clue_status <> -1 and fail_reason in (67, 68, 69, 70, 71, 72) then 2
            when source_type_code = '107054650010000' then 1
            when car_clues_ca_s = 'sop_zijianxiansuo' and car_clues_ca_n = 'zjsjscxs' then 3
            when car_clues_category = 19 then 5
            when car_clues_ca_s in ('sop_chesupai') then 1
            else 0 end as collect_clue_type  -- 收车线索类型 1:开放平台线索 2:B端特殊线索(包含低价及百城关站) 3:4s线索 4:自建线索 0:其他(总部线索) 5:B端直采
    from    guazi_dw_dwb.dwb_clues_vehicle_c2c_car_clues_day
    where   dt = '2020-10-24' and city_id not in (344,345,404,405,406,407)) b
on a.clue_id = b.id
(
    select * from guazi_dw_dwb.dwb_clues_vehicle_c2c_car_clues_expand_day where dt ='2020-10-24') c
    on a.clue_id = c.clue_id
    where b.collect_clue_type in (0,3)
        and is_open_platform =0
        and c.price_model>=100000





















not table
select count(a.clue_id)
from guazi_dw_dwd.dwd_com_evaluate_task_ymd as a
    join guazi_dw_dwd.dwd_call_znkf_task_process_ymd as b
        on b.dt = '2020-09-26'
        and a.clue_id = b.clue_id
        and b.change_price_model_price >=100000
where  a.dt = '2020-09-26'
    and a.create_time between '2020-09-26 00:00:00' and '2020-09-26 23:59:59'
    and a.collect_clue_type in (0, 3)
    and a.is_open_platform=0


select count(a.clue_id)
from guazi_dw_dwd.dwd_com_evaluate_task_ymd as a
    join guazi_dw_dwd.dwd_consign_task_ymd as b
        on b.dt = '2020-09-26'
        and a.clue_id = b.clue_id
        and b.consign_price >=100000
where  a.dt = '2020-09-26'
    and a.create_time between '2020-09-26 00:00:00' and '2020-09-26 23:59:59'
    and a.collect_clue_type in (0, 3)
    and a.is_open_platform=0
