启明星指标维度关系维度表

    insert overwrite table guazi_dw_dwd.dim_com_venus_indicator_dimension_relation_ymd partition(dt = '{run_date}')

select
    coalesce(t2.dimension_id, t3.dimension_id, t4.dimension_id) as dimension_id,  -- 维度id
    t1.data_model_id,  -- 模型id
    t1.title,  -- 模型中维度名称
    t1.dimension_type,  -- 维度类型
    t1.org_idx,  -- org类型
    t1.org_level,  -- org明细
    t1.dw_dim_type,  -- 维度类型，区分是否解耦
    t1.contact_alis,  -- 非组织架构维度关联字段，组织架构维度没有
    coalesce(t2.dim_name, t3.dim_name, t4.dim_name) as dimension_name,  -- 维度名称
    coalesce(t2.dim_name_cn, t3.dim_name_cn, t4.dim_name_cn) as dimension_name_cn,  -- 维度中文名称
    t5.dw_indicator_id,  -- 数据地图指标id
    t5.fact_table_name,  -- 关联事实表名称
    t5.where_condition,  -- 维度关联方式
    t5.indicator_name,  -- 指标名称
    t5.indicator_name_cn,  -- 指标中文名称
    t5.model_name  -- 模型名称
from
(   -- 主表	
    select
        data_model_id,
        title,
        dimension_type,
        contact_alis,
        org_idx,
        org_level,
        city_col,
        district_col,
        user_col,
        dw_dim_type,
        case when dimension_type = 'org' and dw_dim_type = 'business' then coalesce(city_col, user_col) 
            else cast(ceiling(rand() * -65535) as string) end as contact_t2_filter,  -- 关联t2的字段，防止倾斜
        case when dimension_type = 'org' and dw_dim_type <> 'business' then org_level
            else cast(ceiling(rand() * -65535) as string) end as contact_t3_filter,  -- 关联t3的字段，防止倾斜
        case when dimension_type <> 'org' then contact_alis 
            else cast(ceiling(rand() * -65535) as string) end as contact_t4_filter  -- 关联t4的字段，防止倾斜
    from
    (
        select  -- 模型-维度关系表，粒度为 data_model_id + title + dimension_type(因为常规维度和组织架构维度可能重名) 
            data_model_id,  -- 模型id
            title,  -- 维度中文名称
            dimension_type,  -- 维度类型
            col as contact_alis,  -- 非组织架构维度关联字段，组织架构维度没有
            case when dimension_type = 'org' 
                then get_json_object(detail, '$.org_idx') 
                end as org_idx,  -- 组织架构类型
            case when dimension_type = 'org' 
                then concat(get_json_object(detail, '$.org_idx'), 
                   '_', 
                   get_json_object(detail, '$.level'))
                end as org_level,  -- 组织架构层级
            case when get_json_object(detail, '$.city_col') <> '' then get_json_object(detail, '$.city_col') end as city_col,
            case when get_json_object(detail, '$.district_col') <> '' then get_json_object(detail, '$.district_col') end as district_col,
            case when get_json_object(detail, '$.user_col') <> '' then get_json_object(detail, '$.user_col') end as user_col,
            dw_dim_type  -- 维度类型
        from guazi_dw_dwb.dwb_newbi_data_model_dimension_day
        where dt = '{run_date}'
    ) x
) t1
-------------------------------------重新整理一下逻辑-----------------------------------------------------
-- 维度类型分两种 org 和 非org  代表 组织架构维度 和 非组织架构维度 (data_model_dimension)
-- 两种维度的关联方式不尽相同
----- org 类 dimension_type = 'org'
----- org 类维度 分新旧两种模式 解耦 和 非解耦 对应着 dw_dim_type = 'business' 和 dw_dim_type <> 'business'
---------- ①解耦类的org类维度 dw_dim_type = 'business_type'
------------- 人架构 detail.$user_col  城市架构 detail.$city_col  与  bi_dimension的 dim_name 关联(这里需要限制dim_type = 'business'，不然会有重复)
---------- ②非解耦类的org类维度 dw_dim_type <> 'business_type'
------------- dimension的 detail.$org_idx + detail.$org_level 与 bi_dimension的 dim_name 关联(这里需要限制dim_type <> 'business')
----- 非org类维度 dimension_type <> 'org' (已验证所有非org的维度均存在col)
---------- 默认字段 col 关联 bi_dimension的 dim_name
-------------------------- 通过上述方式获取对应的dimension_id 再对应具体的指标去获取关联条件, 考古结果9w6数据，有8w6可关联出维度id
------------------------------------------------------------------------------------------
left join
(   -- org类 ① city 或 user
    select  -- 粒度为dim_name + dim_type，维度名称+维度类型(新旧版本)
        id as dimension_id,  -- 维度id
        dw_dim_id,  -- 数仓维度id
        dim_name,  -- 维度名称
        dim_type,  -- 维度类型
        dim_name_cn  -- 维度中文名称
    from guazi_dw_dwb.dwb_newbi_data_model_bi_dimension_day
    where dt = '{run_date}'
        and dim_status = 'ONLINE'
        and is_deleted = 0
        and dim_type = 'business'
) t2
on t1.contact_t2_filter = t2.dim_name
left join
(   -- org类 ② org_level
    select  -- 粒度为dim_name + dim_type，维度名称+维度类型(新旧版本)
        id as dimension_id,  -- 维度id
        dw_dim_id,  -- 数仓维度id
        dim_name,  -- 维度名称
        dim_type,  -- 维度类型
        dim_name_cn  -- 维度中文名称
    from guazi_dw_dwb.dwb_newbi_data_model_bi_dimension_day
    where dt = '{run_date}'
        and dim_status = 'ONLINE'
        and is_deleted = 0
        and dim_type <> 'business'
) t3
on t1.contact_t3_filter = t3.dim_name
left join
(   -- 非org类维度
    select  -- 粒度为dim_name + dim_type，维度名称+维度类型(新旧版本)
        id as dimension_id,  -- 维度id
        dw_dim_id,  -- 数仓维度id
        dim_name,  -- 维度名称
        dim_type,  -- 维度类型
        dim_name_cn  -- 维度中文名称
    from
    (
        select
            id, dw_dim_id, dim_name, dim_type, dim_name_cn,
            row_number() over(partition by dim_name
                order by case when dim_type = 'business' then 1 else 0 end desc) num  -- 这里优先保证新架构的维度
        from guazi_dw_dwb.dwb_newbi_data_model_bi_dimension_day
        where dt = '{run_date}'
            and dim_status = 'ONLINE'
            and is_deleted = 0
    ) x
    where num = 1
) t4
on t1.contact_t4_filter = t4.dim_name

-- left join dimension t2  -- 以 (city_col or user_col) + dim_type 关联， 最主流的关联方式
-- on coalesce(t1.city_col, t1.user_col) = t2.dim_name
--     and case when t1.dw_dim_type = '' then cast(ceiling(rand() * -65535) as string)
--         else t1.dw_dim_type end = t2.dim_type  -- 虽然只有很少的数据，也防止下数据倾斜
--         
-- left join dimension t3  -- 以 district_col + dim_type 关联，补充信息，把旧有的一些district给补充进来
-- on case when t1.district_col = '' then cast(ceiling(rand() * -65535) as string)
--     else t1.district_col end = t3.dim_name
--     and case when t1.dw_dim_type = '' then cast(ceiling(rand() * -65535) as string)
--         else t1.dw_dim_type end = t3.dim_type  -- 虽然只有很少的数据，也防止下数据倾斜
--         
-- left join dimension t4  -- 存在少量dim_type 为 '' 的，只用dim_name关联，此种方式dim_name是唯一的
-- on case when t1.dw_dim_type = '' then coalesce(t1.city_col, t1.user_col) 
--     else cast(ceiling(rand() * -65535) as string) end = t4.dim_name
--     
-- left join dimension t5  -- 存在少量dim_type 为 '' 的，只用dim_name关联，此种方式dim_name不是唯一的，补充district信息
-- on case when t1.dw_dim_type = '' and t1.district_col <> '' then t1.district_col 
--     else cast(ceiling(rand() * -65535) as string) end = t5.dim_name

left join  --加入指标
(
    select  -- 粒度变成 模型id + 维度id + 指标(这里防止只用维度id关联，会把非该模型的指标也加到其中)
        a.indicator_id,  -- 指标id
        a.dimension_id,  -- 维度id
        a.fact_table_name,  -- 事实表名称
        a.where_condition,  -- 关联方式
        b.model_id,  -- 模型id
        b.model_name,  -- 模型名称
        b.dw_indicator_id,  -- 数据地图指标id
        b.indicator_name,  -- 指标名称
        b.indicator_name_cn  -- 指标中文名称
    from
    (
        select
            *,
            row_number() over(partition by indicator_id, dimension_id order by updated_at desc) num  -- 两个表union all会有重复，这里做出处理
        from
        (
            select -- 指标维度汇总表，粒度为指标id + 维度id
                indicator_id,  -- 指标id
                dimension_id,  -- 维度id
                fact_table_name,  -- 事实表名称
                where_condition,  -- 关联方式
                updated_at
            from guazi_dw_dwb.dwb_newbi_data_model_indicator_dim_table_day  -- 外接维表
            where dt = '{run_date}' 
                and is_deleted = 0
                and dimension_id <> ''  -- 有少量异常

            union all
            select -- 指标维度汇总表，粒度为指标id + 维度id
                indicator_id,  -- 指标id
                dimension_id,  -- 维度id
                -- fact_table_name,  -- 事实表名称
                -- where_condition  -- 关联方式
                null as fact_table_name,
                fact_column_name,  -- 事实表关联字段
                updated_at
            from guazi_dw_dwb.dwb_newbi_data_model_bi_indicator_dimension_day  -- 非外接维表
            where dt = '{run_date}' 
                and is_deleted = 0
        ) x
    ) a
    left join
    (
        select  -- 粒度为 model_id + (indicator_id / dw_indicator_id / bi_indicator_id)
            bi_indicator_id, model_id, model_name, indicator_id, dw_indicator_id, indicator_name, indicator_name_cn
        from guazi_dw_dwd.dim_com_venus_model_indicator_relation_ymd
        where dt = '{run_date}'
    ) b
    on a.indicator_id = b.bi_indicator_id
    where a.num = 1
) t5
on coalesce(t2.dimension_id, t3.dimension_id, t4.dimension_id) = t5.dimension_id  -- 这里关联会导致粒度变化为 data_model_id + title + dimension_type + indicator_id
    and t1.data_model_id = t5.model_id


