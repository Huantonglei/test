
insert overwrite table guazi_dw_dwd.dwd_com_recheck_task_ymd partition (dt = '{run_date}')--复检工单表

select
    ert.id as recheck_task_id
    ,ert.clue_id
    ,ert.city_id as recheck_city_id
    ,ert.area_id
    ,ert.evaluator_id as recheck_evaluator_id
    ,concat(from_unixtime(ert.work_day, 'yyyy-MM-dd'),case
      when ert.time_slot =1  then ' 06:00:00'
      when ert.time_slot =23 then ' 06:15:00'
      when ert.time_slot =2  then ' 06:30:00'
      when ert.time_slot =24 then ' 06:45:00'
      when ert.time_slot =3  then ' 07:00:00'
      when ert.time_slot =25 then ' 07:15:00'
      when ert.time_slot =4  then ' 07:30:00'
      when ert.time_slot =26 then ' 07:45:00'
      when ert.time_slot =5  then ' 08:00:00'
      when ert.time_slot =27 then ' 08:15:00'
      when ert.time_slot =6  then ' 08:30:00'
      when ert.time_slot =28 then ' 08:45:00'
      when ert.time_slot =7  then ' 09:00:00'
      when ert.time_slot =29 then ' 09:15:00'
      when ert.time_slot =15 then ' 09:30:00'
      when ert.time_slot =30 then ' 09:45:00'
      when ert.time_slot =8  then ' 10:00:00'
      when ert.time_slot =31 then ' 10:15:00'
      when ert.time_slot =16 then ' 10:30:00'
      when ert.time_slot =32 then ' 10:45:00'
      when ert.time_slot =9  then ' 11:00:00'
      when ert.time_slot =33 then ' 11:15:00'
      when ert.time_slot =17 then ' 11:30:00'
      when ert.time_slot =34 then ' 11:45:00'
      when ert.time_slot =35 then ' 12:00:00'
      when ert.time_slot =36 then ' 12:15:00'
      when ert.time_slot =37 then ' 12:30:00'
      when ert.time_slot =38 then ' 12:45:00'
      when ert.time_slot =10 then ' 13:00:00'
      when ert.time_slot =39 then ' 13:15:00'
      when ert.time_slot =18 then ' 13:30:00'
      when ert.time_slot =40 then ' 13:45:00'
      when ert.time_slot =11 then ' 14:00:00'
      when ert.time_slot =41 then ' 14:15:00'
      when ert.time_slot =19 then ' 14:30:00'
      when ert.time_slot =42 then ' 14:45:00'
      when ert.time_slot =12 then ' 15:00:00'
      when ert.time_slot =43 then ' 15:15:00'
      when ert.time_slot =20 then ' 15:30:00'
      when ert.time_slot =44 then ' 15:45:00'
      when ert.time_slot =13 then ' 16:00:00'
      when ert.time_slot =45 then ' 16:15:00'
      when ert.time_slot =21 then ' 16:30:00'
      when ert.time_slot =46 then ' 16:45:00'
      when ert.time_slot =14 then ' 17:00:00'
      when ert.time_slot =47 then ' 17:15:00'
      when ert.time_slot =22 then ' 17:30:00'
      when ert.time_slot =48 then ' 17:45:00'
      when ert.time_slot =49 then ' 18:00:00'
      when ert.time_slot =50 then ' 18:15:00'
      when ert.time_slot =51 then ' 18:30:00'
      when ert.time_slot =52 then ' 18:45:00'
      when ert.time_slot =53 then ' 19:00:00'
      when ert.time_slot =54 then ' 19:15:00'
      when ert.time_slot =55 then ' 19:30:00'
      when ert.time_slot =56 then ' 19:45:00'
      when ert.time_slot =57 then ' 20:00:00'
      when ert.time_slot =58 then ' 20:15:00'
      when ert.time_slot =59 then ' 20:30:00'
      when ert.time_slot =60 then ' 20:45:00'
      when ert.time_slot =61 then ' 21:00:00'
      end) as recheck_book_day                                      --复检预约时间
    ,ert.recheck_status                                             --复检状态
    ,ert.is_del                                                     --软删除
    ,ert.cancel_reason                                              --取消复检原因
    ,ert.fail_reason                                                --失败原因
    ,ert.is_rejected                                                --是否驳回
    ,ert.is_agree_relief                                            --复检是否同意免责协议(0:不同意1:同意)
    ,from_unixtime(case when ert.submit_time > 0 then ert.submit_time end,'yyyy-MM-dd HH:mm:ss') as submit_time         --复检报告提交时间
    ,ert.source_type                                                --复检任务来源(1:默认,2:c2b,3:保卖定点)
    ,ert.report_score                                               --复检报告评分
    ,ert.report_level                                               --复检等级，A、B、C、D
    ,ert.is_no_pass                                                 --复检报告是否包含复检不通过项，0:否，1:是
    ,ert.is_sort_out                                                --是否整备
    ,ert.consign_recheck_result                                     --保卖复检评估检测结果
    ,ert.area_type                                                  --1固定点 2流动点 3复检点
    ,substr(ert.created_at, 1, 19) as created_at                                                                        --创建时间
    ,substr(ert.updated_at, 1, 19) as updated_at                                                                        --更新时间
    ,case when ert.num = 1 then 1 else 0 end as is_first_recheck    --是否首次复检
    ,cs.sale_type                                                   --销售类型(0虚拟 1寄售)
    ,cs.pub_city_id                                                 --上架城市
    ,cs.pub_district_id                                             --上架区域
    ,cs.evaluator                                                   --评估师
    ,if(ert.recheck_status in (5,6) and ert.source_type in (3,4,5,6,13, 14), 1, 0) as is_entry_recheck --'是否入库检'
    ,if(ctt2.clue_id is not null, 1, 0) as can_hostling_collect_car --可否整备收车  可整备收车、不可整备收车',
    ,if(carclues.collect_clue_type = 3, 1, 0) as is_4s_collect_car --是否4S收车  是、否',
    ,et.car_owner as  is_public_account --是否公户  公户、私户、公转私',
    ,if(et.pledge is not null, et.pledge, 0) as is_pledge --是否抵押  是、否',
    ,coalesce(firp.first_platform, 0) as first_platform --首次上架平台  B\C\B&C',
    ,cs.platform as current_platform --当前售卖平台  B\C\B&C',
    ,if(ctt.clue_id is not null, ctt.tag, 0) as clue_type --线索类型  0:纯保卖、41:上架B端快卖、42:上架C端快卖、43:保卖转快卖',
    ,ct.task_level as task_level --线索等级  S\A\B\C\D',
    ,null as plate_type --异地类型 1:主城车源本地售卖、2:主城车源异地售卖、3:卫星城车源本地售卖、4:卫星城车源异地售卖',
    ,ert.recheck_result --复检结果  C端复检成功、C端复检成功需整备、B端复检成功&通过售出红线、B端复检成功＆未通过售出红线、复检失败&通过售出红线、复检失败&未通过售出红线'
    ,from_unixtime(case when ert.create_time > 0 then ert.create_time end, 'yyyy-MM-dd HH:mm:ss') as recheck_task_create_time
    ,from_unixtime(rtol.recheck_start_time,'yyyy-MM-dd HH:mm:ss') as recheck_start_time
    ,srtl.recheck_create_trader  
    ,era.area_name --复检点名称
    ,era.address --复检点地址
    ,era.address_lnglat  --复检点坐标
    ,esrh.tag as before_recheck_clue_type --复检前线索类型
    ,besrhbl.before_tag     -- 复检前类型  
    ,aesrhbl.after_tag  -- 复检后类型 c b1 b2 b3
    ,null as area_parent_name --大区名称
    ,null as city_level_detail     --城市等级 SABCD
    , case when audit.max_audit_time is null or from_unixtime(rtol.recheck_start_time) < audit.max_audit_time 
        then 1 else 0 end as recheck_before_audit   -- 是否签约前复检，1是 0否
    , rtol.first_submit_time   -- 首次复检报告提交时间
    , ert.recheck_point_id  -- 复检为后市场门店时对应的后市场门店id
    ,case when besrhbl.order_type is not null then besrhbl.order_type else ordertype.order_type end as order_type   --复检工单类型 1保卖,2C全国购,3B全国购,4开放渠道
    ,audit.max_audit_time as audit_time --签约时间
    , case when carclues.collect_clue_type = 1 then 1 else 0 end as is_open_platform
    , coalesce(carclues.collect_clue_type, 0) as collect_clue_type  -- 收车线索类型 1:开放平台线索 2:B端特殊线索(包含低价及百城关站) 3:4s线索 0:C1线索
    ,ct.clue_created_time -- 保卖线索创建时间
    ,substr(pre_audit.created_at,1,19) as pre_audit_time -- 报告预审核时间
    ,coalesce(car_tag.is_golden2,0) as is_golden2  --是否金牌2.0车源     1:是(目前)  2:是(历史)  0:否
    ,coalesce(p.is_golden2_dealer, 0) as is_golden2_dealer     --是否金牌2.0合作车商   1:是(目前)  2:是(历史)  0:否
from 
(    
    select 
      id
      ,clue_id
      ,city_id
      ,area_id
      ,evaluator_id
      ,work_day
      ,time_slot
      ,recheck_status
      ,is_del
      ,cancel_reason
      ,fail_reason
      ,is_rejected
      ,is_agree_relief
      ,submit_time
      ,source_type
      ,report_score
      ,report_level
      ,is_no_pass
      ,is_sort_out
      ,consign_recheck_result
      ,area_type
      ,created_at
      ,updated_at
      ,recheck_result
      ,create_time
      , recheck_point_id
      ,row_number() over (partition by clue_id order by id) num
    from guazi_dw_dwb.dwb_evaluates_recheck_task_day
    where dt = '{run_date}' 
        and city_id not in (344, 345)
) ert
left join 
(
    select 
        clue_id
        ,platform
        ,sale_type
        ,district_id
        ,pub_district_id
        ,evaluator
        ,pub_city_id
        ,city_id
        , clue_source_type_code
    from guazi_dw_dwb.dwb_cars_car_source_day
    where dt = '{run_date}'
) cs on ert.clue_id = cs.clue_id
left join
(
  select
    clue_id
    ,car_owner
    ,pledge
  from guazi_dw_dwb.dwb_evaluates_vehicle_c2c_evaluate_day
  where dt = '{run_date}' 
) et on ert.clue_id = et.clue_id 
left join
(    --保卖工单标签表
    select clue_id
        ,tag
    from
    (
      select
        clue_id
        ,tag
        ,row_number() OVER (PARTITION BY clue_id ORDER BY created_at DESC) rn
      from guazi_dw_dwb.dwb_consign_task_tag_day
      where dt = '{run_date}'
        and tag in (41, 42, 43)
        and tag_status = 0
    )x where x.rn = 1
) ctt on ert.clue_id = ctt.clue_id
left join
(
  select
    task_id
    ,operator recheck_create_trader
  FROM guazi_dw_dwb.dwb_evaluate_statistic_recheck_task_user_log_day
  WHERE dt = '{run_date}'
) srtl on ert.id = srtl.task_id
left join
(
    select  
        task_id
        ,from_unixtime(min(case when operator_type = 7 and create_time > 0 then create_time end)) as first_submit_time
        ,unix_timestamp(max(case when operator_type = 4 then created_at end)) as recheck_start_time
    from guazi_dw_dwb.dwb_evaluate_statistic_recheck_task_operate_log_day
    WHERE dt = '{run_date}'
        and operator_type in (4, 7)
    group by task_id
) rtol
on ert.id = rtol.task_id

left join
(
  select 
    id
    ,sort_name as area_name --复检点名称
    ,address as address --复检点地址
    ,address_lnglat as address_lnglat  --复检点坐标
  from guazi_dw_dwb.dwb_evaluates_recheck_address_day
  where dt = '{run_date}'
) era on ert.area_id= era.id
left join
(
  select 
    clue_id
  from guazi_dw_dwb.dwb_consign_task_tag_day
  where dt = '{run_date}'
    and tag = 22 
    and tag_status = 0
    group by clue_id
) ctt2 on ert.clue_id =  ctt2.clue_id
left join
(
    select 
        clue_id
        ,task_level
        ,clue_created_time
    from
    (  select 
            clue_id
            ,task_level
            ,substr(created_at, 1, 19) as clue_created_time
            ,row_number() over(partition by clue_id order by updated_at desc) as rn 
        from guazi_dw_dwb.dwb_consign_task_day
        where dt = '{run_date}' and dh = '00'
    )x where x.rn = 1
) ct on ert.clue_id = ct.clue_id
left join( --车源修改日志表
    select
        clue_id, first_platform
    from
    (
        select 
            clue_id
            ,get_json_object(change_info,'$.platform[1]') first_platform
            , row_number() over(partition by clue_id order by created_at desc) num
        from  guazi_dw_dwb.dwb_car_statistic_car_source_change_log_day  
        where dt= '{run_date}'
        and source_key in ('bc_model_insert','che_insert') --涞源
    ) x
    where num = 1
) firp on ert.clue_id = firp.clue_id

left join
(
  SELECT
    task_id
    ,CASE WHEN tag='c' THEN 0 
        WHEN tag = 'b1' THEN 41 
        WHEN tag = 'b2' THEN 42 
        WHEN tag = 'b3' THEN 43 END as tag
    ,row_number() OVER (PARTITION BY task_id ORDER BY id ASC) rn
  FROM guazi_dw_dwb.dwb_evaluate_statistic_recheck_handle_bm_log_day
  WHERE dt = '{run_date}'
) esrh on ert.id = esrh.task_id and esrh.rn = 1
left join
(
    select
        task_id,
        tag as before_tag,
        case when created_at < '2019-10-01' and tag = 'c' then 1 end as order_type
    from
    (
        select
            task_id,
            tag,
            created_at,
            row_number() over (partition by task_id order by created_at asc) num
        from guazi_dw_dwb.dwb_evaluate_statistic_recheck_handle_bm_log_day
        where dt = '{run_date}'
            and record_period = 'before'
    ) x
    where num = 1
) besrhbl
on ert.id = besrhbl.task_id
left join
(   
    select
        task_id,
        tag as after_tag
    from
    (
        select
            task_id,
            tag,
            row_number() over (partition by task_id order by created_at desc) num
        from guazi_dw_dwb.dwb_evaluate_statistic_recheck_handle_bm_log_day
        where dt = '{run_date}'
            and record_period = 'after'
    ) x
    where num = 1
) aesrhbl
on ert.id = aesrhbl.task_id

left join
(
    select
        clue_id, 
        substr(max(audit_at), 1, 19) as max_audit_time
    from guazi_dw_dwb.dwb_contract_log_consignment_snapshot_day
    where dt = '{run_date}'
    group by clue_id
) audit
on ert.clue_id = audit.clue_id
left join 
(
    select id
        ,task_id
        ,order_type
        ,row_number() over (partition by task_id order by created_at desc) rn
    from guazi_dw_dwb.dwb_evaluate_statistic_recheck_task_order_type_log_day
    where dt = '{run_date}'
        AND operation_type = 1  
)ordertype
on ert.id = ordertype.task_id and ordertype.rn = 1
left join
(
    select
        id as clue_id,
        case when clue_status <> -1 and fail_reason in (67, 68, 69, 70, 71, 72) then 2 
            when source_type_code = '107054650010000' then 1
            when car_clues_ca_s = 'sop_zijianxiansuo' and car_clues_ca_n = 'zjsjscxs' then 3
            -- when source_type_code = '205001797010000' then 4
            when car_clues_category = 19 then 5
            when car_clues_ca_s in ('sop_chesupai') then 1
            else 0 end as collect_clue_type  -- 收车线索类型 1:开放平台线索 2:B端特殊线索(包含低价及百城关站) 3:4s线索 0:C1线索 5:B端直采
    from guazi_dw_dwb.dwb_clues_vehicle_c2c_car_clues_day
    where dt = '{run_date}'
) carclues
on ert.clue_id = carclues.clue_id
left join
    (select
        recheck_task_id,
        created_at
    from guazi_dw_dwb.dwb_evaluates_pre_audit_task_day
    where dt='{run_date}' 
    and audit_status = 2) pre_audit
on ert.id=pre_audit.recheck_task_id
left join
    (select
        clue_id,
        max(case when tag = 556 and tag_status = 0 then 1 when tag = 556 and tag_status = -1 then 2 end) as is_golden2
    from
    (
        select
            clue_id, tag_status, tag, created_at,
            row_number() over(partition by clue_id, tag order by id desc) num
        from guazi_dw_dwb.dwb_cars_car_source_tag_day
        where dt='{run_date}' and  tag in (556)
    ) x
    where num = 1
    group by clue_id
)  car_tag
on ert.clue_id = car_tag.clue_id
left join
    (select
        t1.id as clue_id,
        case when t5.is_golden2_dealer = 1 and t5.golden2_delete_flag = 0 and t5.channel_dealer is null then 1
             when t5.is_golden2_dealer = 1 and t5.golden2_delete_flag = 1 and t5.channel_dealer is null then 2 
             else 0 end as is_golden2_dealer  --是否金牌2.0合作车商   1:是(目前)  2:是(历史)  0:否
    from
        (select
            id, store_id, source_type_code, fixed_id
        from guazi_dw_dwb.dwb_evaluate_task_evaluate_task_day
        where dt='{run_date}' and city_id not in (344,345,404,405,406,407)
        and evaluator <> 114137  -- 去除测试数据
        ) t1
    left join
        (select a.dealer_id,
                max(case when tag_id = 13 then 1 end) as is_golden2_dealer,
                max(case when tag_id = 13 then delete_flag end) as golden2_delete_flag,
                max(case when tag_id = 16 then 1 end) as channel_dealer,
                max(case when tag_id = 16 then delete_flag end) as channel_delete_flag
        from guazi_dw_dwb.dwb_dealer_dealer_tag_ref_day a
        where dt='{run_date}' and tag_id in (13,16)
        group by a.dealer_id) t5 
    on t1.store_id = t5.dealer_id ) p
on ert.clue_id = p.clue_id

