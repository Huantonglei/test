--依赖
gzlc_real.fact_bi_open_api_report_log
guazi_dw_dwd.dim_com_staff_department_position_ymd
guazi_dw_dwb.dwb_bi_openapi_diy_api_day


  insert overwrite table guazi_dw_dm.dm_com_api_uuid_usage_stat_ymd partition (dt = '${date_y_m_d}')
  select  x.api_uuid
           ,x.api_name
           ,coalesce(y.pv,0) as pv
           ,coalesce(y.uv,0) as uv
           ,coalesce(y.avg_time,0) as avg_time
           ,coalesce(y.max_time,0) as max_time
           ,coalesce(y.cache_pv,0) as cache_pv
           ,coalesce(y.pv_30day,0) as pv_30day
           ,coalesce(y.uv_30day,0) as uv_30day
           ,coalesce(y.avg_time_30day,0) as avg_time_30day
	   	   ,coalesce(y.pv_90day,0) as pv_90day
           ,coalesce(y.uv_90day,0) as uv_90day
           ,coalesce(y.avg_time_90day,0) as avg_time_90day
    from
	(
        select api_uuid, api_name
        from guazi_dw_dwb.dwb_bi_openapi_diy_api_day
        where dt='${date_y_m_d}'  and dh='00' and is_deleted=0
    ) x
	left join
	(
		select a.uuid
		   ,sum(case when a.dt='${date_y_m_d}' then 1 else 0 end) as pv
		   ,count(distinct case when a.dt='${date_y_m_d}' then a.user_id end) as uv
		   ,sum(case when a.dt='${date_y_m_d}' then a.execute_time else 0 end)/sum(case when a.dt='${date_y_m_d}' then 1 else 0 end) as avg_time
		   ,max(case when a.dt='${date_y_m_d}' then a.execute_time end) as max_time
		   ,sum(case when a.dt='${date_y_m_d}' and point_cache=true then 1 else 0 end) cache_pv
		   ,sum(case when a.dt>date_add('${date_y_m_d}', -30) and a.dt<='${date_y_m_d}' then 1 else 0 end) as pv_30day
		   ,count(distinct case when a.dt>date_add('${date_y_m_d}', -30) and a.dt<='${date_y_m_d}' then a.user_id end) as uv_30day
		   ,sum(case when a.dt>date_add('${date_y_m_d}', -30) and a.dt<='${date_y_m_d}' then a.execute_time else 0 end)
			/ sum(case when a.dt>date_add('${date_y_m_d}', -30) and a.dt<='${date_y_m_d}' then 1 else 0 end) as avg_time_30day
		   ,count(1) as pv_90day
		   ,count(distinct a.user_id) as uv_90day
		   ,sum(a.execute_time)/count(1) as avg_time_90day
		from
		(
			  select uuid,
			         user_id,
			         dt,
			         execute_time,
			         point_cache
			  from  gzlc_real.fact_bi_open_api_report_log
			  where dt>date_add('${date_y_m_d}', -90) and dt<='${date_y_m_d}'  and response_code=100
		) a
		left JOIN
		(
			select distinct user_id
			from guazi_dw_dwd.dim_com_staff_department_position_ymd
			where dt = '${date_y_m_d}' and department_id in ('104233','110988','115701','115700','113423','113046')
		) b on a.user_id=b.user_id
		where b.user_id is null
		group by uuid
	) y on x.api_uuid=y.uuid;