select count(*)
from realtime_dwd.dwd_consignment_snapshot as a
where  is_consigned in (3, 4)
    and audit_time between '2020-09-25 00:00:00' and '2020-09-25 23:59:59'
    and collect_clue_type = 0
11

select count(*)
from guazi_dw_dwd.dwd_consign_collect_contract_snapshot_ymd as a
where  dt = '${date_y_m_d}'
    and is_consigned in (3, 4)
    and audit_time between @startTime and @endTime
    and collect_clue_type = 0
11