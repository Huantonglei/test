insert overwrite table guazi_dw_dm.dm_tracking_polaris_monitor_ymd partition (dt='{run_date}')

select
    '{run_date}' as date_id,                                        -- 数据日期
    line,                                                           -- 业务线
    platform,                                                       -- 平台类型
    sum(pv)                         as pv,                          -- 当日pv
    sum(uv)                         as uv,                          -- 当日uv
    sum(uv_week)                    as uv_week,                     -- 当周uv
    sum(uv_month)                   as uv_month,                    -- 当月uv
    sum(car_collect_clue_num)       as car_collect_clue_num,        -- 收车线索量
    sum(car_collect_customer_num)   as car_collect_customer_num,    -- 收车线索用户数
    sum(car_sell_clue_num)          as car_sell_clue_num,           -- 售车线索量
    sum(car_sell_customer_num)      as car_sell_customer_num        -- 售车线索用户数
from
(select
    line,
    platform,
    sum(pv_num) as pv,
    0 as uv,
    0 as uv_week,
    0 as uv_month,
    0 as car_collect_clue_num,
    0 as car_collect_customer_num,
    0 as car_sell_clue_num,
    0 as car_sell_customer_num
from guazi_dw_dwd.dwd_tracking_detail_ymd
where dt='{run_date}'
and line in ('xinche','c2c')
and tracking_type='pageload'
group by line,platform  -- 各业务线各平台当日pv

union all

select
    line,
    'all' as platform,
    sum(pv_num) as pv,
    0 as uv,
    0 as uv_week,
    0 as uv_month,
    0 as car_collect_clue_num,
    0 as car_collect_customer_num,
    0 as car_sell_clue_num,
    0 as car_sell_customer_num
from guazi_dw_dwd.dwd_tracking_detail_ymd
where dt='{run_date}'
and line in ('xinche','c2c')
and tracking_type='pageload'
group by line  -- 各业务线当日总pv

union all

select
    line,
    platform,
    0 as pv,
    count(distinct guid) as uv,
    0 as uv_week,
    0 as uv_month,
    0 as car_collect_clue_num,
    0 as car_collect_customer_num,
    0 as car_sell_clue_num,
    0 as car_sell_customer_num
from guazi_dw_dwd.dwd_tracking_guid_ymd
where dt='{run_date}'
and line in ('xinche','c2c')
and is_visit=1
group by line,platform  -- 各平台当日uv

union all

select
    line,
    'all' as platform,
    0 as pv,
    count(distinct case when dt='{run_date}' then guid end) as uv,
    count(distinct case when dt>=date_sub('{run_date}',pmod(datediff('{run_date}','1900-01-08'),7)) then guid end)  as uv_week,
    count(distinct case when dt>=concat(substr('{run_date}',1,7),'-01') then guid end)  as uv_month,
    0 as car_collect_clue_num,
    0 as car_collect_customer_num,
    0 as car_sell_clue_num,
    0 as car_sell_customer_num
from guazi_dw_dwd.dwd_tracking_guid_ymd
where dt>=date_sub(concat(substr('{run_date}',1,7),'-01'),6) and dt<='{run_date}'
and line in ('xinche','c2c')
and is_visit=1
group by line  -- 当日uv 当周uv 当月uv


union all

select
    'xinche' as line,
    'all' as platform,
    0 as pv,
    0 as uv,
    0 as uv_week,
    0 as uv_month,
    count(clue_id) as car_sell_clue_num,
    count(distinct customer_phone) as car_sell_customer_num,
    0 as car_sell_clue_num,
    0 as car_sell_customer_num
from guazi_dw_dwd.dwd_newcar_clue_ymd
where dt = '{run_date}'
and substr(clue_created_at, 1, 10) = '{run_date}'  -- 新车线索量和线索用户数

union all

select
    'c2c' as line,
    'all' as platform,
    0 as pv,
    0 as uv,
    0 as uv_week,
    0 as uv_month,
    count(clue_id) as car_collect_clue_num,
    count(distinct phone_encrypt) as car_collect_customer_num,
    0 as car_sell_clue_num,
    0 as car_sell_customer_num
from
guazi_dw_dwd.dim_com_car_clue_ymd
where dt = '{run_date}'
and substr(create_time, 1, 10)='{run_date}'  -- 收车线索量和线索用户数
and car_clues_category<>19

union all

select
    'c2c' as line,
    'all' as platform,
    0 as pv,
    0 as uv,
    0 as uv_week,
    0 as uv_month,
    0 as car_collect_clue_num,
    0 as car_collect_customer_num,
    count(distinct sell_clue_id) as car_sell_clue_num,
    count(distinct phone_encrypt) as car_sell_customer_num
from
guazi_dw_dwd.dwd_com_growth_sell_clue_ymd
where dt = '{run_date}'
and substr(create_time, 1, 10)='{run_date}'  -- 售车线索量和线索用户数
) t1
group by line,platform
