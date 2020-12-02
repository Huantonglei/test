insert overwrite table guazi_dw_dwd.dim_com_car_clue_ymd partition (dt = '{day_exec}')--二手车线索维度表
    select a.id as clue_id,
        a.city_id as city_id,
        a.district_id as district_id,
        a.evaluator as evaluator,
        a.appointment_person as appointment_person,
        null as source_id,
        if(a.create_time > 0, from_unixtime(a.create_time,'yyyy-MM-dd HH:mm:ss'), null)  as create_time,
        if(appointment_status_update_time > 0, from_unixtime(appointment_status_update_time,'yyyy-MM-dd HH:mm:ss'), null) as evaluation_appointment_time,
        case when clue_status > 3 and appointment_status_update_time > 0 then from_unixtime(appointment_status_update_time,'yyyy-MM-dd HH:mm:ss') else null end as evaluation_appointment_success_time,
        case when clue_status = 2 and appointment_status_update_time > 0 then from_unixtime(appointment_status_update_time,'yyyy-MM-dd HH:mm:ss') else null end as evaluation_appointment_fail_time,
        case when clue_status in (6, 8, 9, 10, 11, 13, 14) and evaluate_status_update_time > 0 then from_unixtime(evaluate_status_update_time,'yyyy-MM-dd HH:mm:ss') else null end as evaluation_time,
        case when clue_status in (8, 9, 10, 11, 13) and evaluate_status_update_time > 0 then from_unixtime(evaluate_status_update_time,'yyyy-MM-dd HH:mm:ss')  else null end as evaluation_success_time,
        case when clue_status = 6 and evaluate_fail_status in (1,4) and evaluate_status_update_time > 0 then from_unixtime(evaluate_status_update_time,'yyyy-MM-dd HH:mm:ss') else null end as evaluation_fail_time,
        case when (clue_status in (8, 9, 10, 13) or evaluate_fail_status in (1, 4)) and evaluate_status_update_time > 0 then from_unixtime(evaluate_status_update_time,'yyyy-MM-dd HH:mm:ss') else null end as evaluation_actual_time,
        case when clue_status = 9 and clue_post_time > 0 then from_unixtime(clue_post_time,'yyyy-MM-dd HH:mm:ss') else null end as shelve_time,
        case when clue_status = 14 and evaluate_fail_update_time > 0 then from_unixtime(evaluate_fail_update_time,'yyyy-MM-dd HH:mm:ss') else null end as evaluation_cancel_time,
        case when clue_status = 10 and pub_apply_time > 0 then from_unixtime(pub_apply_time,'yyyy-MM-dd HH:mm:ss') else null end as reject_time,
        clue_status as clue_status,       -- 线索状态
        pub_post_type as shelve_type,     -- 线索上架类型(1:c2c,2:c2b)
        case when clue_status = 10 then reject_reason else 0 end as reject_reason,
        -- case when clue_status in (6, 14) then evaluate_fail_reason else 0 end as evaluate_fail_reason,
        a.evaluate_fail_reason as evaluate_fail_reason,
        a.source_type as source_type,
        a.source_type_code as scode,      -- 线索唯一编码
        a.fail_reason as evaluation_appointment_fail_reason,
        -- case when (instr(cast(a.source_type_code as string),'20500000001') >0 or instr(cast(a.source_type_code as string),'10400004901') >0 or instr(cast(a.source_type_code as string),'20500179701') >0)  then 1 else 0 end as is_self,
        case when growth.channel = 'sop' and growth.ca_s = 'sop_zijianxiansuo' then 1 else 0 end as is_self,
        a.clue_level as clue_level,
        case when growth.ca_s in ('sop_xxmdhz') or growth.scode in ('101053489220000')
            then '线下上架'
            when growth.ca_s in ('sop_cytsgj', 'sop_cheyuanpanhuo')
                or growth.scode in ('204003525010000','101053526010000','101053520010000','101053543010000','104053572010000')
            then '车源盘活'
            else '线上收车'
            end as clue_type,
        et.task_type as task_type,   -- 评估工单类型 0 正常模式 2 云评估+评销分离 3 评销分离 4 纯云评估
        growth.channel as channel,  -- 渠道大类
        growth.ca_s as channel_identification,    -- 渠道标识
        growth.ca_n as first_channel_identification,    -- 一级渠道
        growth.scode as second_channel_identification,   -- 二级渠道
        a.car_clues_category as car_clues_category,     -- 线索提交分类  1卖车、2成交记录、3估价，默认值0 未设置
        a.phone_encrypt  as phone_encrypt,   -- 车主加密电话号
        -------------------------------- 添加线索渠道来源分类判断逻辑 -----------------------------------
        case when growth.channel in ('app', 'pz', 'self', 'seo', 'sp', 'bd', 'dh', 'sem', 'tg')
            or growth.ca_n = 'kefuzijian'
            or growth.ca_s = 'sop_huruxiansuo'
            or growth.ca_s = 'sop_ldwjqxj' then '线上主渠道'
            when growth.ca_n = 'jianguazizijian'
            then '捡瓜子自建'
            -- when growth.ca_s = 'sop_xxmdhz'
            --    or growth.ca_n = 'zjsjscxs'
            when growth.ca_s='sop_zijianxiansuo' and growth.ca_n='zjsjscxs'     -- 4S收车口径变更
            then '4S收车'
            when growth.ca_s in ('sop_cytsgj', 'sop_cheyuanpanhuo', 'sop_tianlangxing')
            then '车源盘活'
            when growth.channel = 'sop' and growth.ca_s = 'sop_cheyuanqingxi'
                and growth.ca_n = '90dkcqx' and growth.scode = '107054645110000'
            then '车源清洗'
            else '线上其他' end as channel_class,       -- 线索渠道大类-中文
        case when growth.channel = 'app' then '1-app'
            when growth.channel = 'pz' then '2-pz'
            when growth.channel = 'self' then '3-self'
            when growth.channel = 'seo' then '4-seo'
            when growth.channel = 'sp' then '5-sp'
            when growth.channel = 'bd' then '6-bd'
            when growth.channel = 'dh' then '7-dh'
            when growth.channel = 'sem' then '8-sem'
            when growth.channel = 'tg' then '9-tg'
            when growth.ca_s = 'sop_xiansuozhaohui' then 'f-sop召回'
            when growth.ca_s = 'sop_daorujihuo' then 'g-sop导入'
            when growth.ca_s='sop_zijianxiansuo' and growth.ca_n='zjsjscxs' then 'i-4S收车'
            when growth.ca_s in ('sop_cytsgj', 'sop_cheyuanpanhuo', 'sop_tianlangxing') then 'j-车源盘活'
            when growth.ca_n = 'jianguazizijian' then 'd-捡瓜子自建'
            when growth.ca_n = 'kefuzijian' then 'a-客服自建'
            when growth.ca_s = 'sop_huruxiansuo' then 'b-呼入线索'
            when growth.ca_s = 'sop_ldwjqxj' then 'c-来电未接起'
            when growth.channel = 'sop' and growth.ca_s = 'sop_cheyuanqingxi' and growth.ca_n = '90dkcqx'
                and growth.scode = '107054645110000' then '车源清洗'
            when growth.channel = 'sop' then 'e-sop其他'
            else 'h-other' end as channel_sub_class,     -- 线索渠道子类-中文

        ----------------------------------------------------------------------------------------------
        a.clue_type as clues_clue_type,  -- 工单线索类型（1：新线索；2：流转线索；3：回流线索）
        from_unixtime(case when a.evaluate_time > 0 then a.evaluate_time end) as evaluate_time,  -- 评估时间
        from_unixtime(case when a.reconfirm_time > 0 then a.reconfirm_time end) as reconfirm_time,  -- 回流时间
        growth.platform,  -- 线索来源平台1pc,2wap,3ios,4ipad,5andriod,6other
        case when a.source_type_code = '107054650010000' then 1 else 0 end as is_open_platform, --是否开放平台
        if(gcml.clue_id is not null,1,0) as is_mark_loss, --'折损标识: 1:需折损,0:不需折损’
        coalesce(expand.car_owner_direct_appoint_type, 0) as car_owner_direct_appoint_type, -- 车主直约类型(0：默认值，无任何含义， 1：车主直约--分配给客服， 2：车主直约-自动分单给评估师）)
        case when a.source_type_code = '107054650010000' then 1
            when a.clue_status <> -1 and a.fail_reason in (67, 68, 69, 70, 71, 72) then 2
            when a.car_clues_ca_s = 'sop_zijianxiansuo' and car_clues_ca_n = 'zjsjscxs' then 3
            -- when a.source_type_code = '205001797010000' then 4
            when a.car_clues_category = 19 then 5
            when a.car_clues_ca_s in ('sop_chesupai') then 1
            else 0 end as collect_clue_type, -- 1:开放平台线索 2:B端特殊线索(包含低价及百城关站) 3:4s线索 4:自建线索 0:其他(总部线索) 5:B端直采
        operation_log.first_appoint_success_type,  -- 首次预约成功类型
        operation_log.first_appoint_success_time,  -- 首次预约成功时间
        operation_log.appoint_success_type,  -- 最新预约成功类型
        operation_log.appoint_success_time  -- 最新预约成功时间
    from (
        select
            id
            ,source_type
            ,clue_type
            ,source_type_code
            ,clue_status
            ,fail_reason
            ,evaluate_fail_reason
            ,create_time
            ,phone_encrypt
            ,city_id
            ,district_id
            ,evaluator
            ,evaluate_time
            ,appointment_person
            ,allot_update_time
            ,appointment_status_update_time
            ,evaluate_status_update_time
            ,clue_post_time
            ,pub_apply_time
            ,reject_reason
            ,evaluate_fail_status
            ,evaluate_fail_update_time
            ,reconfirm_time
            ,pub_post_type
            ,clue_level
            ,car_clues_category
            , car_clues_ca_s
            , car_clues_ca_n
        from guazi_dw_dwb.dwb_clues_vehicle_c2c_car_clues_day
        where dt = '{day_exec}'
            and city_id not in (344, 345)   -- 去除测试数据

    )a
    left join
    (   -- 添加线索类型：线下上架、线上收车、车源盘活
        select
            clue_id, ca_s, scode, channel, ca_n, platform
        from guazi_dw_dwb.dwb_guazi_sem_growth_car_clue_subtable_all_day
            where dt = '{day_exec}'
    ) growth
    on a.id = growth.clue_id
    left join
    (
        select
            id, task_type
        from guazi_dw_dwb.dwb_evaluate_task_evaluate_task_day -- 评估主表
        where dt = '{day_exec}'
    ) et
    on a.id = et.id
    left join
    (   --growth折损标记中间表
        select  clue_id
        from guazi_dw_dwb.dwb_guazi_sem_growth_clue_mark_loss_day
        where dt='{day_exec}'
            and line=2 and line_category=1
    )gcml
    on a.id = gcml.clue_id
    left join
    (   --	线索信息扩展表
        select clue_id, car_owner_direct_appoint_type
        from guazi_dw_dwb.dwb_clues_vehicle_c2c_car_clues_expand_day
        where dt = '{day_exec}'
    ) expand
    on a.id = expand.clue_id
    left join
    (
        select
            clue_id,
            max(case when first_appoint_success = 1 then evaluate_type end) as first_appoint_success_type,  -- 首次预约成功类型
            max(case when first_appoint_success = 1 then created_at end) as first_appoint_success_time,  -- 首次预约成功时间
            max(case when appoint_success = 1 then evaluate_type end) as appoint_success_type,  -- 最新预约成功类型
            max(case when appoint_success = 1 then created_at end) as appoint_success_time  -- 最新预约成功时间
        from
        (
            select
                clue_id,
                evaluate_type,
                created_at,
                row_number() over(partition by clue_id order by created_at) first_appoint_success,
                row_number() over(partition by clue_id order by created_at desc) appoint_success
            from
            (
                select
                    clue_id, created_at,
                    remark[1] as evaluate_type,  -- 上门 or 到店 or null
                    remark[2] as clue_type  -- 新增线索、回流线索、流转线索
                from
                (
                    select
                        clue_id, split(substr(remark, 21, 13), '-') as remark, created_at
                    from guazi_dw_dwb.dwb_clue_statistic_vehicle_c2c_operation_log_day
                    where dt = '{day_exec}'
                        -- and (remark like '%预约成功-上门%' or remark like '%预约成功-到店%')
                        and remark like '%[新提交接口]提交预约结果: 预约成功%'
                ) x
            ) x
        ) x
        group by clue_id
    ) operation_log
    on a.id = operation_log.clue_id