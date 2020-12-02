
        set hive.auto.convert.join=false;
        insert overwrite table guazi_dw_dwd.dwd_com_evaluate_task_ymd partition (dt = {day_exec})--二手车评估工单表
        select
           l.clue_id, 
           l.appointment_person,
           l.evaluator,       
           l.city_id,
           l.district_id,       
           l.clue_status,                                                                                               -- 线索状态
           if(l.appointment_status_update_time > 86400, from_unixtime(l.appointment_status_update_time, 'yyyy-MM-dd HH:mm:ss'), null) 
                as evaluation_appointment_success_time,                                                                 -- 预约成功提交时间
           from_unixtime(case when l.create_time > 0 then l.create_time end,'yyyy-MM-dd HH:mm:ss') as create_time,      -- 记录时间
           case when cs.clue_id is not null then cs.source_level else l.clue_level end as clue_level,                   -- 线索等级
           l.source_type,                                                                                               -- 带看任务来源
           l.source_type_code as scode,                                                                                 -- 来源scode
           -- case when thy.clue_id is not null then 1 else 0 end is_self,                                              -- 是否自建 0:否,1:是
           null as is_self,
           case when clues.collect_clue_type = 3 then 1 else 0 end as is_4s_collect_car,                                -- 是否4S收车 1 是 0 否
           ev.car_owner_evaluate,                                                                                       -- 公户类型 1 公户 2 私户 3 公转私
           ev.is_pledge,                                                                                                -- 是否抵押车 1 是 0 否
           -- area.area_parent_name,                                                                                    -- 部门名称
           -- area.city_level_detail,                                                                                   -- 等级明细
           null as area_parent_name,
           null as city_level_detail,
           area.is_consign_city_shelf as is_consign_city_shelf,                                                         --是否保卖城市
           l.seller as collect_seller_id,                                                                               -- 收车销售id
           label.label_name,                                                                                            -- 评估线索标签
           if(l.evaluate_time > 86400, from_unixtime(l.evaluate_time), null) as evaluate_book_time,                         -- 预约评估时间
           if(clues.evaluate_success_time > 0, from_unixtime(clues.evaluate_success_time), null) as evaluate_success_time,      -- 评估成功时间
           l.task_type as task_type,                                                                                    -- 0 正常模式 2 云评估+评销分离 3 评销分离 4 纯云评估
           ev.level as evaluate_level,                                                                                  -- 评估等级
           ev.score as evaluate_score,                                                                                  -- 评估得分
           if(clues.evaluate_submit_time > 0, from_unixtime(clues.evaluate_submit_time), null) as evaluate_submit_time,         -- 实际评估时间/评估报告提交时间
           if(l.start_evaluate_time > 0, from_unixtime(l.start_evaluate_time), null) as evaluate_begin_time,            -- 评估开始时间
           case when ccl.clue_id is not null then l.store_id end as 4s_store_id,                                        -- 4s店id
           case when clues.source_type_code = '107054650010000' then 1 else 0 end as is_open_platform,                  -- 是否开放平台
           if(l.allot_update_time > 0, from_unixtime(l.allot_update_time), null) as allot_update_time,                  -- 下发工单时间
           if(l.evaluate_fail_update_time > 0, from_unixtime(l.evaluate_fail_update_time), null) as evaluate_fail_update_time,  -- 评估失败状态变更时间
           if(l.first_suc_eval_time > 0, from_unixtime(l.first_suc_eval_time), null) as first_suc_eval_time,            -- 首次提交评估成功的时间
           l.contactor,                                                                                                 -- 联系人
           coalesce(clues.collect_clue_type, 0) as collect_clue_type,                                                   -- 收车线索类型 1:开放平台线索 2:B端特殊线索(包含低价及百城关站) 3:4s线索 0:C1线索
           l.fixed_id,                                                                                                  -- 上门or到店工单 0:上门工单，>0:到店工单
           l.store_id,                                                                                                   -- 所有门店信息
            operation_log.first_push_success_type,  -- 首次推送评估端成功类型
            operation_log.first_push_success_time,  -- 首次推送评估端成功时间
            operation_log.push_success_type,  -- 最后一次推送评估端成功类型
            operate_log.evaluating_cancel_time,  -- 评估开始后上架检测取消时间
            operate_log.before_evaluate_cancel_time,  -- 开始评估前上架检测取消时间
            l.evaluate_fail_reason,  -- 评估失败原因
            case when task_tag.clue_id is null then 0 else 1 end as is_direct,  -- 是否直下工单 1直下工单 0非直下工单
            operate_log.last_cancel_evalute_time,
            m.first_contact_create_time, --首次联系车主时间
            clues.evaluate_status_update_time,
            coalesce(tag.is_golden2, 0) as is_golden2,  --是否金牌2.0车源     1:是(目前)  2:是(历史)  0:否
            case when n.is_golden2_dealer = 1 and n.golden2_delete_flag = 0 and n.channel_dealer is null then 1
                 when n.is_golden2_dealer = 1 and n.golden2_delete_flag = 1 and n.channel_dealer is null then 2 
                 else 0 end as is_golden2_dealer  --是否金牌2.0合作车商   1:是(目前)  2:是(历史)  0:否

        from
        (
            select 
                id as clue_id,updated_at,appointment_person,evaluator,clue_status,
                appointment_status_update_time,city_id,district_id,

                seller,source_type_code, evaluate_time, task_type,start_evaluate_time,store_id,
                create_time,clue_level, source_type, allot_update_time, evaluate_fail_update_time, first_suc_eval_time,
                contactor, fixed_id, evaluate_fail_reason
            from guazi_dw_dwb.dwb_evaluate_task_evaluate_task_day
            where dt = {day_exec} and city_id not in (344, 345)
        ) l
        left join (
            select 
                clue_id,
                source_level
            from guazi_dw_dwb.dwb_cars_car_source_day 
            where dt = {day_exec}
        ) cs on l.clue_id = cs.clue_id
        left join 
        (
            select
                clue_id,
                car_owner_evaluate,
                is_pledge,
                level,
                score
            from
            (
                select
                    clue_id,
                    car_owner as car_owner_evaluate,
                    pledge as is_pledge,
                    level,
                    score,
                    row_number() over(partition by clue_id order by id desc) num 
                from guazi_dw_dwb.dwb_evaluates_vehicle_c2c_evaluate_day
                where dt = {day_exec} and dh = '00' 
            ) x
            where num = 1
        )ev on ev.clue_id=l.clue_id
        left join 
        ( 
            select 
                city_id,
                district_id,
                -- max(city_level_detail) as city_level_detail,
                -- max(area_parent_name) as area_parent_name,
                max(is_consign_city_shelf) as is_consign_city_shelf
            from guazi_dw_dwd.dim_com_district_ymd
            where dt = {day_exec}
            group by city_id,district_id
        ) area on area.district_id=l.district_id and area.city_id=l.city_id
        left join
        (
            select
               a.clue_id,
               b.label_name
            from
            (
                select
                    clue_id,
                    label_id
                from
                (
                   select 
                        clue_id,
                        label_id,
                        row_number() over(partition by clue_id order by updated_at desc) rn
                   from
                      guazi_dw_dwb.dwb_evaluate_task_task_tag_day
                   where dt = {day_exec} and dh='00' 
                        and label_id in (3, 4, 5) 
                        and is_del = 0
                ) t
                where rn = 1
            ) a
            left join 
            (
                 select 
                     id,
                     label_name
                from
                guazi_dw_dwb.dwb_evaluate_task_task_tag_config_day
                where dt = {day_exec} and dh='00' 
                        and is_del = 0
            ) b 
            on a.label_id = b.id
        ) label
        on l.clue_id=label.clue_id
        left join 
        (
            select 
                clue_id
            from guazi_dw_dwb.dwb_consign_task_day 
            where dt = {day_exec} and dh = '00'
                and origin_type in (5, 6, 7)
        ) thy 
        on l.clue_id=thy.clue_id
        left join
        (
            select
                id, 
                case when clue_status in (8, 9, 10, 11, 13) then evaluate_status_update_time end 
                    as evaluate_success_time,    -- 评估成功时间，当线索状态为评估成功，上架成功，上架驳回，预评估成功，上架暂存时
                case when clue_status in (8, 9, 10, 13) or evaluate_fail_status in (1, 4) then evaluate_status_update_time end 
                    as evaluate_submit_time,     -- 实际评估时间，当评估失败状态为已通过，或评估成功后
                source_type_code,
                evaluate_status_update_time,
                case when clue_status <> -1 and fail_reason in (67, 68, 69, 70, 71, 72) then 2 
                    when source_type_code = '107054650010000' then 1
                    when car_clues_ca_s = 'sop_zijianxiansuo' and car_clues_ca_n = 'zjsjscxs' then 3
                    -- when source_type_code = '205001797010000' then 4
                    when car_clues_category = 19 then 5
                    when car_clues_ca_s in ('sop_chesupai') then 1
                    else 0 end as collect_clue_type -- 收车线索类型 1:开放平台线索 2:B端特殊线索(包含低价及百城关站) 3:4s线索 0:C1线索 5:B端直采
            from guazi_dw_dwb.dwb_clues_vehicle_c2c_car_clues_day
            where dt = {day_exec}
        ) clues
        on l.clue_id = clues.id
        left join
        (
            -- 4s收车
            select
                clue_id
            from guazi_dw_dwd.dim_com_car_clue_ymd
            where dt = {day_exec}
                and channel_class = '4S收车'
        ) ccl
        on l.clue_id = ccl.clue_id
        left join
        (
            select
                clue_id,
                max(case when first_push_success = 1 then evaluate_type end) as first_push_success_type,  -- 首次推送评估端成功类型
                max(case when first_push_success = 1 then created_at end) as first_push_success_time,  -- 首次推送评估端成功时间
                max(case when push_success = 1 then evaluate_type end) as push_success_type  -- 最后一次推送评估端成功类型
            from
            (
                select
                    clue_id,
                    evaluate_type,
                    created_at,
                    row_number() over(partition by clue_id order by created_at) first_push_success,
                    row_number() over(partition by clue_id order by created_at desc) push_success
                from
                (
                    select
                        clue_id, created_at, 
                        remark[1] as evaluate_type,  -- 上门 or 到店
                        remark[2] as clue_type  -- 新增线索、回流线索、流转线索
                    from
                    (
                        select
                            clue_id, split(remark, '-') as remark, created_at
                        from guazi_dw_dwb.dwb_clue_statistic_vehicle_c2c_operation_log_day
                        where dt = {day_exec}
                            and remark like '%推送评估%'
                            -- and (remark like '%推送评估-上门%' or remark like '%推送评估-到店%')
                    ) x
                ) x
            ) x
            group by clue_id
        ) operation_log
        on l.clue_id = operation_log.clue_id
        left join
        (   
            select
                clue_id,
                min(case when type_key = 3011 then created_at end) as evaluating_cancel_time,  -- 评估开始后上架检测取消时间
                min(case when type_key = 3009 then created_at end) as before_evaluate_cancel_time,  -- 开始评估前上架检测取消时间
                max(case when type_key in (3009, 3017) then created_at else null end) as  last_cancel_evalute_time
            from
            (
                select  
                    clue_id, created_at, type_key,
                    row_number() over (partition by clue_id,type_key order by created_at desc) num
                from guazi_dw_dwb.dwb_evaluate_statistic_vehicle_c2c_clue_evaluate_operate_log_day
                where dt = {day_exec}
                    and type_key in (3009,3011,3017)
            ) x
            where num = 1
            group by clue_id
        ) operate_log
        on l.clue_id = operate_log.clue_id
        left join
        (
            select
                clue_id
            from guazi_dw_dwb.dwb_evaluate_task_task_tag_day
            where dt = {day_exec} and label_id = 10 and is_del = 0
            group by clue_id
        ) task_tag on l.clue_id= task_tag.clue_id  
        left join
        (
            select
                x1.clue_id,
                min(x1.first_contact_create_time) as first_contact_create_time,
                min(x2.call_status) as call_status
            from
            (
                select
                    clue_id, substr(created_at, 1, 19) as first_contact_create_time
                from guazi_dw_dwb.dwb_evaluate_statistic_contact_record_day
                where dt = {day_exec}
            ) x1
            left join
            (
                select
                    clue_id,
                    call_status,
                    created_at
                from guazi_dw_dwb.dwb_evaluates_evaluate_call_day
                where dt = {day_exec} and call_status >= 0
            ) x2 on x1.clue_id = x2.clue_id and substr(x1.first_contact_create_time, 1, 10) = substr(x2.created_at, 1, 10)
            group by x1.clue_id
        ) m on l.clue_id = m.clue_id
        left join (
            select
                clue_id,
                max(case when tag = 556 and tag_status = 0 then 1 when tag = 556 and tag_status = -1 then 2 end) as is_golden2
            from
            (
                select
                    clue_id, tag_status, tag, created_at,
                    row_number() over(partition by clue_id, tag order by id desc) num
                from guazi_dw_dwb.dwb_cars_car_source_tag_day
                where dt={day_exec} and tag in (556)
            ) x
            where num = 1
            group by clue_id
        ) tag on l.clue_id = tag.clue_id
        left join (
            select a.dealer_id,
                max(case when tag_id = 13 then 1 end) as is_golden2_dealer,
                max(case when tag_id = 13 then delete_flag end) as golden2_delete_flag,
                max(case when tag_id = 16 then 1 end) as channel_dealer,
                max(case when tag_id = 16 then delete_flag end) as channel_delete_flag
            from guazi_dw_dwb.dwb_dealer_dealer_tag_ref_day a
            where dt={day_exec}  and tag_id in (13,16)
            group by a.dealer_id
        ) n on l.store_id = n.dealer_id

        ;
       set hive.auto.convert.join=true;

