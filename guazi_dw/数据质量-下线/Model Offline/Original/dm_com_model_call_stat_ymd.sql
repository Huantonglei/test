--依赖
guazi_dw_dwb.dwb_newbi_data_model_data_model_day
guazi_dw_dwd.dwd_com_model_call_stat_ymd
guazi_dw_dwd.dim_com_venus_model_indicator_relation_ymd
guazi_dw_dwd.dim_com_staff_department_position_ymd



with a as (
     select id as model_id,
            regexp_replace(instance_name,'\\t','') as model_name,
            source_from,
            source_type,
            origin_type,
            request_source,
            substr(created_at,1,19) as ctime,
            substr(updated_at,1,19) as utime
     from guazi_dw_dwb.dwb_newbi_data_model_data_model_day
     where dt = '${date_y_m_d}' and dh = '00' and is_deleted = 0
),
-- 模型访问量
b as (
	select model_id,
	    sum(case when a.dt='${date_y_m_d}' then a.pv else 0 end) as pv,
		count(distinct case when a.dt='${date_y_m_d}' then a.user_name end) as uv,
		sum(case when a.dt>date_add('${date_y_m_d}', -30) and a.dt<='${date_y_m_d}' then a.pv else 0 end) as pv_30day,
		count(distinct case when a.dt>date_add('${date_y_m_d}', -30) and a.dt<='${date_y_m_d}' then a.user_name end) as uv_30day,
		sum(a.pv) as pv_90day,
		count(distinct a.user_name) as uv_90day
	from
	(
		 select model_id,
		        pv,
		        user_name,
		        dt
	     from guazi_dw_dwd.dwd_com_model_call_stat_ymd
	     where dt>date_add('${date_y_m_d}', -90) and dt<='${date_y_m_d}'
	) a
	left join
	(
		select distinct split(email,'@')[0] as user_name
		from guazi_dw_dwd.dim_com_staff_department_position_ymd
		where dt = '${date_y_m_d}' and department_id in ('104233','110988','115701','115700','113423','113046')
	) b on a.user_name=b.user_name
	where b.user_name is null
	group by a.model_id
),
-- 模型与报表对应关系
e as (
     select model_id,
            self_product_table,
            public_product_table
     from guazi_dw_dwd.dim_com_venus_model_indicator_relation_ymd
     where dt = '${date_y_m_d}'
     group by model_id,self_product_table,public_product_table
)

insert overwrite table guazi_dw_dm.dm_com_model_call_stat_ymd partition (dt = '${date_y_m_d}')
select  a.model_id
       ,a.model_name
       ,COALESCE(b.pv,0) as pv
       ,COALESCE(b.uv,0) as uv
       ,COALESCE(b.pv_30day,0) as pv_30day
       ,COALESCE(b.uv_30day,0) as uv_30day
       ,COALESCE(b.pv_90day,0) as pv_90day
       ,COALESCE(b.uv_90day,0) as uv_90day
       ,ctime
       ,utime
       ,self_product_table
       ,public_product_table
       ,source_from
       ,source_type
       ,origin_type
	   ,a.request_source
from a
left join  b on a.model_id=b.model_id
left join  e on a.model_id=e.model_id;