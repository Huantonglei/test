select count(clue_id)
from realtime_dwd.dwd_evaluate_task as a
where  clue_create_time between '2020-09-25 00:00:00' and '2020-09-25 23:59:59' --线索创建时间
    and create_time <>'1970-01-01 08:00:00' --上架时间
    and collect_clue_type in (0, 3)
    and is_open_platform=0
    and price_model>=100000
1461

select count(a.clue_id)
from guazi_dw_dwd.dwd_com_evaluate_task_ymd as a
  join guazi_dw_dwb.dwb_clues_vehicle_c2c_car_clues_expand_day as b
        on b.dt = '${date_y_m_d}'
        and a.clue_id = b.clue_id
        and b.price_model >=100000
    join guazi_dw_dwd.dim_com_car_clue_ymd as c
        on c.dt = '${date_y_m_d}'
        and a.clue_id = c.clue_id
        and  c.create_time between @startTime and @endTime
where  a.dt = '${date_y_m_d}'
    and a.create_time <>'1970-01-01 08:00:00'
    and a.collect_clue_type in (0, 3)
    and a.is_open_platform=0
1230

clue_create_time :
guazi_dw_dwd.dim_com_car_clue_ymd -> create_time

price_model 没找到