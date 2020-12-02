select count(*)
from realtime_dwd.dwd_consignment_snapshot as a
where  is_consigned=1
    and audit_time between '2020-09-25 00:00:00' and '2020-09-25 23:59:59'
    and consignment_type <> 'open_platform'
    and substr(discard_time,1,4)='1970'
    and consign_price>=100000
93

select count(*)
from guazi_dw_dwd.dwd_consign_collect_contract_snapshot_ymd as a
where  dt = '${date_y_m_d}'
    and is_consigned=1
    and audit_time between @startTime and @endTime
    and consignment_type <> 'open_platform'
    and (substr(discard_time,1,4)='1970' or discard_time is null)
    and consign_price>=100000
97

    discard_time 大部分为空