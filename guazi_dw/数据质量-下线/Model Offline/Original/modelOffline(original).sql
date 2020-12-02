select * from
(
select a.model_id,
       a.model_name,
       a.pv_30day,
       f.public_product_table,
       f.public_product_table_id,
       COALESCE(e.user_name,c.creator) creator,
       COALESCE(b.user_name,d.user_name,c.editor) as editor
  from
  (
    select model_id,max(model_name) model_name,sum(pv_30day) pv_30day FROM
    (
      select model_id,
             model_name,
             pv,
             pv_30day,
             pv_90day
      from guazi_dw_dm.dm_com_model_call_stat_ymd
      where dt='${date_y_m_d}'  and is_public_model=1 -- and pv_90day =0
      union all
      select b.model_id,a.api_name as model_name,pv,pv_30day,pv_90day
      from
      (
       select api_uuid,api_name,pv,uv,pv_30day,uv_30day,pv_90day,uv_90day
       from guazi_dw_dm.dm_com_api_uuid_usage_stat_ymd
       where dt='${date_y_m_d}'
      ) a
      join
      (
       select venus_model_id as model_id,api_uuid
       from guazi_dw_dwb.dwb_bi_openapi_diy_api_day
       where dt='${date_y_m_d}'
      ) b on a.api_uuid=b.api_uuid
    ) x
    group by model_id -- having sum(pv_30day) <30
  ) a
JOIN=
(
    select
    id as model_id,
    creator,
    editor
  from guazi_dw_dwb.dwb_newbi_data_model_data_model_day
  where dt = '${date_y_m_d}'
    and is_deleted = 0  and origin_type = 1
) c on a.model_id=c.model_id
left JOIN
(
select cast (user_id as varchar) user_id,user_name
from guazi_dw_dwd.dim_com_staff_ymd
where dt = '${date_y_m_d}'
) b on c.editor=b.user_id
left JOIN
(
select email,max(user_name) user_name
from guazi_dw_dwd.dim_com_staff_ymd --员工信息表
where dt = '${date_y_m_d}'
  group by email
) d on c.editor=d.email
left JOIN
(
select email,max(user_name) user_name
from guazi_dw_dwd.dim_com_staff_ymd
where dt = '${date_y_m_d}'
  group by email
) e on c.creator=e.email
left JOIN
(
select
    model_id,
    max(public_product_table) public_product_table, -- 使用该模型的公共报表(分号分隔)
    max(public_product_table_id) public_product_table_id -- 使用该模型的公共报表id(分号分隔)
from guazi_dw_dwd.dim_com_venus_model_indicator_relation_ymd
where dt = '${date_y_m_d}'
group by 1
) f on a.model_id=f.model_id
) x
order by editor,creator