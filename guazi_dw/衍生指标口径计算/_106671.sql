select count(clue_id) from realtime_dwd.dwd_car_source as a
where  clue_create_time between '2020-10-24 00:00:00' and '2020-10-24 23:59:59' --线索创建时间
	and collect_clue_type in (0, 3)
	and is_open_platform_clue=0
	and price>=100000
1497

该线索创建时间来源于 mysql_source.clues___vehicle_c2c_car_clues的create_time
dim_com_car_clue_ymd里的create_time也是来源于 guazi_dw_dwb.dwb_clues_vehicle_c2c_car_clues_day 的create_time

高价车上架量_漏斗

select count(a.clue_id)
from guazi_dw_dwd.dim_com_car_source_ymd as a
join guazi_dw_dwd.dim_com_car_clue_ymd as b
    on b.dt = '${date_y_m_d}'
    and a.clue_id = b.clue_id
    and b.create_time between @startTime and @endTime
where  a.dt = '${date_y_m_d}'
	and a.collect_clue_type in (0, 3)
	and a.is_open_platform_clue=0
	and a.price>=100000

646

数量有问题


'收车线索类型 1:开放平台线索 2:B端特殊线索(包含低价及百城关站) 3:4s线索 4:自建线索 0:其他(总部线索) 5:B端直采',

 case when clue_status <> -1 and fail_reason in (67, 68, 69, 70, 71, 72) then 2
            when source_type_code = '107054650010000' then 1
            when car_clues_ca_s = 'sop_zijianxiansuo' and car_clues_ca_n = 'zjsjscxs' then 3
            when car_clues_category = 19 then 5
            when car_clues_ca_s in ('sop_chesupai') then 1
            else 0 end as collect_clue_type  -- 收车线索类型 1:开放平台线索 2:B端特殊线索(包含低价及百城关站) 3:4s线索 4:自建线索 0:其他(总部线索) 5:B端直采
    from mysql_source.clues___vehicle_c2c_car_clues
    where city_id not in (344,345,404,405,406,407)
) mid2 on a.clue_id = mid2.id


left join
(
    select
        id
        ,appointment_person
        , case when clue_status <> -1 and fail_reason in (67, 68, 69, 70, 71, 72) then 2
            when source_type_code = '107054650010000' then 1
            when car_clues_ca_s = 'sop_zijianxiansuo' and car_clues_ca_n = 'zjsjscxs' then 3
            -- when source_type_code = '205001797010000' then 4
            when car_clues_category = 19 then 5
            when car_clues_ca_s in ('sop_chesupai') then 1
            else 0 end as collect_clue_type -- 收车线索类型 1:开放平台线索 2:B端特殊线索(包含低价及百城关站) 3:4s线索 0:C1线索
    from guazi_dw_dwb.dwb_clues_vehicle_c2c_car_clues_day
    where dt = '{run_date}' and dh = '00'
) j


select count(clue_id) from guazi_dw_dwd.dim_com_car_source_ymd as a
where  dt = '${date_y_m_d}'
	and create_time between @startTime and @endTime
	and collect_clue_type in (0, 3)
	and is_open_platform_clue=0
	and price>=100000

1453





