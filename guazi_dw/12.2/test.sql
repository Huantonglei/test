select *
 from (
 select busi_time,t1_table_ods as ods,t1_table_dwb as dwb,t1_table_dwd as dwd
 ,t1_table_dw as dw,t1_table_dm as dm
 from hive_source.bi_kanban_table_stat_info
 where busi_time='${date_y_m_d}') a
 left join (
 select  busi_time,t1_table_ods as ods2,t1_table_dwb as dwb2,t1_table_dwd as dwd2
 ,t1_table_dw as dw2,t1_table_dm as dm2
 from hive_source.bi_kanban_table_visit_stat_info
 where busi_time='${date_y_m_d}'
 ) b on a.busi_time=b.busi_time