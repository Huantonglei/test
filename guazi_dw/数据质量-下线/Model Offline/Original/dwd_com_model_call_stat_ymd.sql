insert overwrite table guazi_dw_dwd.dwd_com_model_call_stat_ymd partition (dt = '${date_y_m_d}')
select a.model_id,model_name
       ,case when b.user_id is null then a.user_id else b.user_name end user_name
       ,app_id,source_type,pv,code_branch
from (
     select model_id,user_id,app_id,source_type,code_branch,count(1) as pv
     from  gzlc_real.fact_bi_olap_drilldown_info
     where dt='${date_y_m_d}'
     group by model_id,user_id,app_id,source_type,code_branch
) a
left join (
         select user_id,split(email,'@')[0] as user_name
         from  guazi_dw_dwd.dim_com_staff_ymd
         where dt='${date_y_m_d}'
) b on a.user_id=b.user_id
left join (
     select id as model_id,instance_name as model_name
     from guazi_dw_dwb.dwb_newbi_data_model_data_model_day
     where dt='${date_y_m_d}'  and dh = '00' and is_deleted = 0
) c on a.model_id=c.model_id;


