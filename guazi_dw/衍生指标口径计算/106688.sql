select count(1)
from realtime_dwd.dwd_ctob_channel_dealer_clue_distribute as a
where  created_at between '2020-09-25 00:00:00' and '2020-09-25 23:59:59'
   and clue_type=30
1398

select count(1)
from guazi_dw_dwd.dwd_ctob_channel_clue_distribute_ymd as a
where  dt = '2020-09-25'
    and created_at between '2020-09-25 00:00:00' and '2020-09-25 23:59:59'
    and clue_type=30


dwd_ctob_channel_clue_distribute_ymd 该表不存在