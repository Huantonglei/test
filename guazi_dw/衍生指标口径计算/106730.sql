select count(*)
from realtime_dwd.dwd_car_source as a
where  create_time between '2020-09-25 00:00:00' and '2020-09-25 23:59:59' --上架时间
    and collect_clue_type<>2
    and is_virtual_shelf=0
    and 1=1
    and fixed_id>0 --固定点ID
892

select count(*)
from guazi_dw_dwd.dim_com_car_source_ymd as a
join guazi_dw_dwd.dwd_com_evaluate_task_ymd as b
 on b.dt = '${date_y_m_d}'
 and b.fixed_id > 0
 and a.clue_id = b.clue_id
where  a.dt = '${date_y_m_d}'
    and a.create_time between @startTime and @endTime
    and a.collect_clue_type<>2
    and a.is_virtual_shelf=0
    and 1=1
892




