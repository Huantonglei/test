$SPARK_HOME/bin/spark-submit
-- queue root.dw.main
-- name dwd_consign_task_ymd@lihequn
-- driver-memory 4G
-- driver-cores 2
-- executor-memory 12G
-- executor-cores 4
-- deploy-mode cluster
-- conf spark.shuffle.service.enabled=true
-- conf spark.dynamicAllocation.enabled=true
-- conf spark.dynamicAllocation.minExecutors=2
-- conf spark.dynamicAllocation.maxExecutors=32
-- conf spark.dynamicAllocation.initialExecutors=2
-- conf spark.default.parallelism=400
-- conf spark.sql.shuffle.partitions=200
-- conf spark.driver.extraJavaOptions="-Xms4g"
-- conf spark.driver.maxResultSize=2g
-- conf spark.sql.crossJoin.enabled=true /data/release/guazi_dw/conf/sql_demo.py  "
        insert overwrite table guazi_dw_dwd.dwd_consign_task_ymd partition (dt = '2020-11-17' --线索工单表
        select
            a.clue_id,  -- 车源clue_id
            a.city_id,  -- 车源城市 ID
            a.district_id,  -- 车源区域 ID 
            a.consign_user_id,  -- 寄售专员
            a.recheck_id,   -- 复检工单ID
            a.car_address_id,   -- 存车地点
            d.contract_user_id,     -- 销售id
            d.evaluate_editor,  -- 评估师维护人
            case when l.id is not null then l.evaluator
                 when m.clue_id is not null and m.clue_status>3 and m.appointment_status_update_time>0 then m.evaluator
                 else 0 end as evaluator,   -- 评估师user_id
            city.city_name,  -- 城市名
            dist.district_name,
            a.car_source_status,    -- 车源状态
            case when a.car_source_status = '0' then '在售'
                when a.car_source_status = '1' then '已定'
                when a.car_source_status = '2' then '已售'
                when a.car_source_status = '3' then '停售'
                when a.car_source_status = '4' then '退车'
                when a.car_source_status = '5' then '已定审核' end as car_source_status_desc,   -- 车源状态描述
            from_unixtime(a.pub_time) as shelve_time,   -- 上架时间
            a.task_status as consign_task_status,   -- 工单状态
            case when a.task_status = 0 then '待分配保卖专员'
                when a.task_status = 1 then '保卖取消'
                when a.task_status = 2 then '待预约复检'
                when a.task_status = 3 then '待复检'
                when a.task_status = 4 then '保卖失败'
                when a.task_status = 5 then '待签约'
                when a.task_status = 6 then '待合同审核'
                when a.task_status = 7 then '待付签约定金'
                when a.task_status = 8 then '签约驳回'
                when a.task_status = 9 then '待存车'
                when a.task_status = 10 then '保卖成功'
                when a.task_status = 11 then '待付售卖尾款'
                when a.task_status = 12 then '保卖完成'
                when a.task_status = 13 then '待收购'
                when a.task_status = 14 then '待付收购尾款'
                when a.task_status = 15 then '已收购'
                when a.task_status = 16 then '待付差价'
                when a.task_status = 17 then '待解押'
                when a.task_status = 18 then '解押成功'
                when a.task_status = 19 then '解押失败'
                when a.task_status = 21 then '预约暂存'
                when a.task_status = 22 then '寄售待合同审核'
                when a.task_status = 23 then '寄售签约驳回'
                when a.task_status = 24 then '寄售待存车'
                when a.task_status = 25 then '寄售成功'
                when a.task_status = 26 then '寄售待付售卖尾款'
                when a.task_status = 27 then '寄售完成'
                when a.task_status = 28 then '寄售到期'
                when a.task_status = 29 then '寄售结束'
                end as consign_task_status_desc,    -- 工单状态描述
            a.real_price,   -- 成交价
            a.service_price,    -- 服务费
            a.contract_price,   -- 合同签约价
            a.proportion,   -- 定金百分比
            from_unixtime(unix_timestamp(a.created_at)) as task_created_at,    -- 创建时间
            a.fail_reason as consign_fail_reason,   -- 保卖失败原因
            case when a.fail_reason = 1 then '复检停售'
                when a.fail_reason = 2 then '复检转B'
                when a.fail_reason = 3 then '车主违约'
                when a.fail_reason = 4 then '评估改底价'
                end as consign_fail_reason_desc,    -- 保卖失败原因描述
            a.cancel_reason as consign_cancel_reason,   -- 保卖取消原因
            a.source_type as consign_source_type,   -- 业务类型
            a.origin_type,  -- 工单来源(1上架 2自建)
            a.contract_type,    -- 合同类型(0未签,1纸质,2电子)
            if(a.prepay_time>0,from_unixtime(a.prepay_time),null) as prepay_time,  -- 车源已定时间
            case when ss.discard_at = 0 then substr(ss.audit_at, 1, 19) end as contract_audit_time,  -- 签约时间
            c.reject_consign_reason,    -- 不设置托底寄售原因
            case when g.clue_id is not null then g.consign_sale_source_type else 0 end as consign_sale_source_type,     -- 创建来源
            case when g.clue_id is not null then g.consign_sale_task_status else 1 end as consign_sale_task_status,     -- 工单状态，1待处理2保卖跟进3保卖不跟进
            case when case when g.clue_id is not null then g.consign_sale_task_status else 1 end = 1 then '待处理'
                when case when g.clue_id is not null then g.consign_sale_task_status else 1 end = 2 then '保卖跟进'
                when case when g.clue_id is not null then g.consign_sale_task_status else 1 end = 3 then '保卖不跟进'
                end as consign_sale_task_status_desc,   -- 工单状态，1待处理2保卖跟进3保卖不跟进
            case when g.clue_id is not null then from_unixtime(unix_timestamp(g.consign_sale_created_at)) end as consign_sale_created_at,   -- 记录创建时间
            case when i.clue_id is not null then i.trade_status else 0 end as trade_status,
            case when i.clue_id is not null then i.pay_car_price else 0 end as pay_car_price,
            case when k.clue_id is not null then 1 else 0 end as is_sys_source,     -- 车源渠道是否来自系统分配
            case when k1.clue_id is not null then 1 else 0 end as is_evaluator_source,  -- 车源状态是否来自评估师推荐
            n.consign_price,    -- 模型价格
            a.task_level,   -- 线索等级（S 10  A 20  B 30  C 40  D 50 其中D是定级失败，0表示未定级）
            case when nn.clue_id is not null then 1 else 0 end as is_people_make,
            a.sold_type,    -- 售卖类型
            null as contract_submit_time,     -- 合同最后一次提交时间 2019-09-26 zzl 
            a.consignment_type,     -- 保卖合同类型 0.普通保卖合同 1.抵押保卖合同
            case when n.consign_price is null then 1
                when n.consign_price = 0 or n.consign_price = 10000000 then 2
                else 3 end as apply_price_type,     --定价类型  1 模型定价 2 纯人工定价 3 模型+人工
            a.platform,     -- 上架平台（位运算 1(c2c) 2(c2b)  3(c2c&c2b)）
            xscc.sku_level,
            gct.operator_price as guaranteed_operator_price,    -- 审批价格 主管担保收车任务表的审批价格
            apt.operator_price as apply_price_operator_price,   -- 审批价格 申请价格任务信息表中最高审批价格
            case when ss.collect_type = 'qgg' then 3
                when ctt.clue_id is not null then 2
                -- when a.origin_type = 8 then 4 -- 线索回捞自建 不计入保卖线索统计
                when open_platform.clue_id is not null then 5 --开放平台车源
                else 1 end as is_consigned,  --保卖标志
            if(cc.collect_clue_type = 3,1,0) as is_4s_collect_car,
            c.car_owner as car_owner_evaluate,
            c.pledge as is_pledge,
            coalesce(firp.first_platform,0) as first_platform,
            nvl(cte.tag,0) as first_clue_type,
            --d.first_consign_type end as first_consign_type,  -- 首次线索类型 0c 1b
            coalesce(case when a.origin_type != 8 then d.first_consign_type end, -1) as first_consign_type, --首次线索类型 0c 1b
            area.area_parent_name,
            area.city_level_detail,
            4s_tag.tag as tag_4s,    -- 4s店标签，50 4s店个人，51 4s店背户
            if(k2.clue_id is null,0,1) as is_national_service,  --是否标记全国购
            substr(k2.first_national_service_time,1,19) as first_national_service_time, --首次标记全国购时间
            if(gg.contract_status=1,1,0) as contract_status, --是否签订意向合同
            ss.collect_type, --收车签约类型
            gg.first_contract_time, --首次签订意向合同时间
            asdf.label_name,
            cs.car_source_evaluate_editor,   -- 保卖线索对应的检测师
            a.base_price as base_price,  -- 寄售底价
            null as return_time, -- 2019-09-26 zzl
            case when ctt.tag in (41,42,43) then ctt.tag else 0 end as clue_type,
            if(ccl.clue_id is null,0,1) as is_liquidation,   --是否斩仓车源
            ccl.created_at as liquidation_satrt_time,  --成为斩仓车时间
            editor_log.after_evaluate_editor as first_consign_evaluator_id,  -- 首次保卖线索维护评估师id
            cg.goods_status,
            a.task_type as task_type, -- 工单类型
            case when open_platform.clue_id is not null then 1 else 0 end as is_open_platform,  -- 是否开放平台车源
            d.seller_service_fee_pct as seller_service_fee_pct, -- 保卖收车服务费比例
            case when qwe.clue_id is not null then 1 else 0 end is_consign_return_car,--是否保卖退车退款(收车过程退车）
            qwe.consign_return_time, --保卖退车退款完成时间(收车过程退车） 
            cc.collect_clue_type
        from 
        (
            select 
                * 
            from 
            (
                select 
                    *,
                    row_number() over(partition by clue_id order by updated_at desc) as rn 
                from guazi_dw_dwb.dwb_consign_task_day 
                where dt = '2020-11-17' and dh = '00'
                AND city_id not in (344, 345, 404, 405, 406, 407)
            ) nn 
            where rn = 1
        ) a
        left join 
        (
            select 
                * 
            from 
            (
                select 
                    *,
                    row_number() over(partition by clue_id order by updated_at desc) as rn 
                from guazi_dw_dwb.dwb_consign_task_time_day 
                where dt = '2020-11-17' and dh = '00' 
            ) a 
            where rn = 1
        ) b 
        on a.clue_id = b.clue_id
        left join 
        (
            select 
                * 
            from
            (
                select 
                    *,
                    row_number() over(partition by clue_id order by updated_at desc) as rn 
                from guazi_dw_dwb.dwb_evaluates_vehicle_c2c_evaluate_day 
                where dt = '2020-11-17' and dh = '00'
            ) nn 
            where rn = 1
        ) c 
        on a.clue_id = c.clue_id
        left join 
        (
            select 
                *  
            from 
            (
                select 
                    *,
                    row_number() over(partition by clue_id order by updated_at desc) as rn 
                from guazi_dw_dwb.dwb_consign_task_extra_day 
                where dt = '2020-11-17' and dh = '00'
            ) a 
            where rn = 1
        ) d 
        on a.clue_id = d.clue_id
        left join 
        (
            select 
                * 
            from
            (
                select 
                    *,
                    row_number() over(partition by clue_id order by updated_at desc) as rn 
                from
                (
                    select
                        f.clue_id clue_id, 
                        f.source_type as consign_sale_source_type,
                        f.task_status as consign_sale_task_status,
                        f.created_at as consign_sale_created_at,
                        f.updated_at as updated_at
                    from 
                    (
                        select 
                            * 
                        from
                        (
                            select 
                                *,
                                row_number() over(partition by contract_user_id order by updated_at desc) as rn 
                            from guazi_dw_dwb.dwb_consign_task_extra_day 
                            where dt = '2020-11-17' and dh = '00'
                        ) nn 
                        where rn = 1
                    ) e
                    left join 
                    (
                        select 
                            * 
                        from guazi_dw_dwb.dwb_sale_sale_consign_task_day 
                        where dt = '2020-11-17' and dh = '00'
                    ) f 
                    on e.contract_user_id = f.sales_id
                ) nn
            ) mm 
            where rn = 1 
        )as g 
        on a.clue_id = g.clue_id
        left join 
        (
            select 
                * 
            from
            (
                select 
                    *,
                    row_number() over(partition by clue_id order by update_at desc) as rn 
                from guazi_dw_dwb.dwb_guazi_contracts_vehicle_c2c_order_day 
                where dt = '2020-11-17' and dh = '00'
            ) nn 
            where rn = 1
        ) i 
        on a.clue_id = i.clue_id
        left join
        (
            select 
                clue_id 
            from
            (
                select 
                    *,
                    row_number() over(partition by clue_id order by updated_at desc) as rn 
                from guazi_dw_dwb.dwb_cars_car_source_tag_day 
                where dt = '2020-11-17' and dh = '00' 
                    and tag = 53
            ) nn 
            where rn = 1
        ) as k 
        on a.clue_id = k.clue_id
        left join
        (
            select 
                clue_id 
            from
            (
                select 
                    *,
                    row_number() over(partition by clue_id order by updated_at desc) as rn 
                from guazi_dw_dwb.dwb_cars_car_source_tag_day 
                where dt = '2020-11-17' and dh = '00' 
                    and tag = 55
            ) nn 
            where rn = 1
        ) as k1 
        on a.clue_id = k1.clue_id
        left join
        (
            select 
                clue_id
                ,created_at as first_national_service_time
            from
            (
                select 
                    *,
                    row_number() over(partition by clue_id order by created_at asc) as rn 
                from guazi_dw_dwb.dwb_cars_car_source_tag_day 
                where dt = '2020-11-17' and dh = '00'
                    and tag = 187
            ) nn 
            where rn = 1
        ) as k2 
        on a.clue_id = k2.clue_id
        left join 
        (
            select
                clue_id, consign_price
            from
            (
                select 
                    clue_id,
                    consign_price,  -- 模型价格
                    row_number() over (partition by clue_id order by updated_at desc) rn
                from guazi_dw_dwb.dwb_consign_apply_price_task_day
                where dt = '2020-11-17' and dh = '00' 
                    and apply_status = 1
            ) x
            where rn = 1
        ) as n 
        on a.clue_id = n.clue_id
        left join 
        (
            select 
                * 
            from guazi_dw_dwb.dwb_clues_vehicle_c2c_car_clues_day 
            where dt = '2020-11-17' and dh = '00'
        ) as l 
        on a.clue_id = l.id
        left join 
        (
            select 
                * 
            from 
            (
                select 
                    *,
                    row_number() over(partition by clue_id order by updated_at desc) as rn 
                from guazi_dw_dwb.dwb_guazi_call_data_log_vehicle_c2c_car_clues_day 
                where dt = '2020-11-17' and dh = '00'
            ) nn 
            where rn = 1
        ) m 
        on a.clue_id = m.clue_id
        left join 
        (
            select 
                n1.clue_id
            from
            (
                select 
                    clue_id
                from guazi_dw_dwb.dwb_consign_task_day 
                where dt = '2020-11-17' and dh = '00'
                    and origin_type in (1,2) 
                    and city_id not in (344, 345, 404, 405, 406, 407)
                group by clue_id
            ) n1
            inner join 
            (
                select 
                    n2.clue_id 
                from
                (
                    select 
                        clue_id, proposer, source_type
                    from 
                    (
                        select 
                            clue_id, proposer, source_type,
                            row_number() over(partition by clue_id order by updated_at desc) as rn 
                        from guazi_dw_dwb.dwb_consign_apply_price_task_day 
                        where dt = '2020-11-17' and dh = '00'
                    ) nn where rn = 1
                ) n2
                inner join 
                (
                    select 
                        clue_id, evaluate_editor
                    from
                    (
                        select 
                            clue_id, evaluate_editor,
                            row_number() over(partition by clue_id, evaluate_editor order by updated_at desc) as rn 
                        from guazi_dw_dwb.dwb_consign_task_extra_day 
                        where dt = '2020-11-17' and dh = '00'
                    ) nn where rn = 1
                ) n3 
                on n2.clue_id = n3.clue_id and n2.proposer = n3.evaluate_editor 
                where n2.source_type = 2 
            ) as n4 
            on n1.clue_id = n4.clue_id 
        ) nn 
        on a.clue_id = nn.clue_id
        left join 
        
            (
                select 
                    a.foreign_id as clue_id,
                    car_park_id,
                    goods_status,
                    delivery_time,
                    city_id,
                    district_id,
                    unix_timestamp(created_at) as wait_park_time,
                    entry_time,
                    print_card -- 是否打印二维码，1为打印，0为未打印
                from
                (
                    select
                        id, foreign_id, car_park_id, goods_status, delivery_time, city_id,
                        district_id, created_at, entry_time, print_card
                    from guazi_dw_dwb.dwb_warehouse_car_goods_day
                    where dt='2020-11-17' and dh='00'
                ) a
                join
                (
                    select
                        foreign_id, max(id) as max_id
                    from guazi_dw_dwb.dwb_warehouse_car_goods_day
                    where dt='2020-11-17' and dh='00'
                    group by foreign_id
                ) b
                on a.id = b.max_id
            ) cg on a.clue_id = cg.clue_id

        --20180613 add by liuchenghai 
        --for collect car refunt task
        left join
        (   
            select 
                district_id,
                short_name as district_name
            from guazi_dw_dwb.dwb_misc_misc_district_day
            where dt = '2020-11-17'
        ) dist 
        on coalesce(cg.district_id, a.district_id) = dist.district_id
        left join
        (   
            select 
                city_id,
                short_name as city_name
            from guazi_dw_dwb.dwb_misc_misc_city_day
            where dt = '2020-11-17'
        ) city 
        on coalesce(cg.city_id, a.city_id) = city.city_id
        left join
        (
            select 
                a.clue_id, c.sku_level
            from
            (
                select 
                    clue_id,
                    contract_price
                from 
                (
                    select 
                        *,
                        row_number() over(partition by clue_id order by updated_at desc) as rn 
                    from guazi_dw_dwb.dwb_consign_task_day 
                    where dt = '2020-11-17' and dh = '00'
                ) nn 
                where rn = 1
            ) a
            left join 
            ( 
                select 
                    city_id,
                    clue_id,
                    tag_name
                from guazi_dw_dwb.dwb_cars_car_source_day 
                where dt = '2020-11-17' and dh = '00'
            ) b 
            on a.clue_id = b.clue_id
            left join
            (
                select 
                    tag_name,
                    price_segment_min,
                    price_segment_max,
                    city_id,
                    sku_level
                from
                (
                    select 
                        tag_name,
                        price_segment_min,
                        price_segment_max,
                        sku_version,
                        city_id,
                        sku_level
                    from guazi_dw_dwb.dwb_xlssyfx_sku_city_collect_day
                    where dt = '2020-11-17' and dh = '00'
                )t1
                join
                (
                    select        
                        max(sku_version) sku_version
                    from guazi_dw_dwb.dwb_xlssyfx_sku_city_collect_day
                    where dt = '2020-11-17' and dh = '00'
                )t2 on t1.sku_version = t2.sku_version
            ) c on b.city_id = c.city_id and b.tag_name = c.tag_name 
            where a.contract_price >= c.price_segment_min 
                and a.contract_price < c.price_segment_max
        )xscc  
        on a.clue_id = xscc.clue_id

        --20180621 add by liuchenghai 
        --for special collect car analysis
        left join
        (
            select
                clue_id, operator_price
            from 
            (
                select 
                    clue_id,
                    operator_price,     -- 主管担保收车任务表的审批价格
                    row_number() over(partition by clue_id order by id desc) as rn  
                from guazi_dw_dwb.dwb_consign_guaranteed_car_task_day
                where dt = '2020-11-17' and dh = '00'
                    and apply_status = 1
            ) x
            where rn = 1
        ) gct on gct.clue_id = a.clue_id
        left join
        (
            select 
                clue_id,
                max(operator_price) as operator_price --申请价格任务信息表中最高审批价格
            from guazi_dw_dwb.dwb_consign_apply_price_task_day
            where apply_status = 1
                and dt = '2020-11-17' and dh = '00'
            group by clue_id 
        ) apt on apt.clue_id = a.clue_id 
        left join 
        (
            select clue_id
                ,tag
                ,row_number() over(partition by clue_id order by created_at desc) rn
            from guazi_dw_dwb.dwb_consign_task_tag_day
            where dt = '2020-11-17' and  tag in(41, 42, 43) and tag_status = 0 
        ) ctt on a.clue_id = ctt.clue_id and ctt.rn = 1
        left join 
        (     
            select
                clue_id,collect_clue_type
            from guazi_dw_dwd.dim_com_car_clue_ymd
            where dt = '2020-11-17'
                -- and collect_clue_type = 3
        )cc on cc.clue_id=a.clue_id
        left join
        (   select clue_id,
                first_platform
            from(
                 select clue_id
                        ,get_json_object(change_info,'$.platform[1]') first_platform
                        , row_number() over(partition by clue_id order by created_at desc) num
                 from  guazi_dw_dwb.dwb_car_statistic_car_source_change_log_day  
                 where dt= '2020-11-17'
                 and source_key in ('bc_model_insert','che_insert')
                 )x
            where num = 1
        ) firp on a.clue_id = firp.clue_id
        left join 
        (
        SELECT
             t1.clue_id,
             t2.tag
         FROM
           (
                  SELECT clue_id
                  FROM guazi_dw_dwb.dwb_consign_task_extra_day
                  WHERE dt ='2020-11-17' AND dh = '00' AND first_consign_type = 1
           ) t1
         LEFT JOIN 
           (
                  SELECT
                      clue_id,
                      tag,
                      row_number() OVER (PARTITION BY clue_id ORDER BY id ASC) rn
                  FROM guazi_dw_dwb.dwb_consign_task_tag_day
                  WHERE dt ='2020-11-17' AND dh = '00' 
                  AND tag IN (41, 42, 43) 
                  AND tag_status IN (0,-1)

            ) t2 ON t1.clue_id = t2.clue_id and t2.rn=1
        )cte on cte.clue_id=a.clue_id
        left join 
        ( select city_id
                ,district_id
                ,max(city_level_detail) as city_level_detail
                ,max(area_parent_name) as area_parent_name	
         from guazi_dw_dwd.dim_com_district_ymd
         where dt = '2020-11-17'
         group by city_id,district_id
        )area on area.district_id=coalesce(cg.district_id,a.district_id) and area.city_id=coalesce(cg.city_id, a.city_id)
        left join
        (
            -- 添加4s店个人、4s店背户的标签，这里clue_id唯一，写法为预防万一
            select
                clue_id,
                max(tag) as tag
            from guazi_dw_dwb.dwb_consign_task_tag_day
            where dt = '2020-11-17' and dh = '00'
                and tag in (50, 51)
                and tag_status = 0
            group by clue_id
        ) 4s_tag
        on a.clue_id = 4s_tag.clue_id
       left join
         (SELECT 
               c.pdt_src_id clue_id,
               substr(min(a.created_at), 1, 19) first_contract_time,
               max(IF(a.sign_status = 'confirmed', 1, 0)) contract_status
          FROM
            (
              SELECT
                   business_number,
                   created_at,
                   sign_status
              FROM guazi_dw_dwb.dwb_bs_elec_contract_contract_common_day
              WHERE dt ='2020-11-17'
              AND business_line = 'qgg'
              ) a
              INNER JOIN (
              SELECT id
              FROM guazi_dw_dwb.dwb_order_center_orders_day
              WHERE dt ='2020-11-17' 
              AND type_id = '2036'
              ) b ON a.business_number = b.id
              INNER JOIN (
              SELECT
                    order_id,
                    pdt_src_id
              FROM guazi_dw_dwb.dwb_order_center_order_product_day
              WHERE dt = '2020-11-17'
           ) c ON b.id = c.order_id
         GROUP BY c.pdt_src_id
          )gg on gg.clue_id=a.clue_id
        left join 
        (
            select
                clue_id, audit_at, consignment_type as collect_type, discard_at,
                row_number() over (partition by clue_id order by audit_at desc) num
            from guazi_dw_dwb.dwb_contract_log_consignment_snapshot_day
            where dt = '2020-11-17'
                and city_id not in (344, 345, 404, 405, 406, 407)
        ) ss on  ss.clue_id=a.clue_id and ss.num = 1 
           left join
      (SELECT
           a.clue_id,
           b.label_name
      FROM
          (
           SELECT 
               clue_id,
                label_id
           FROM
              guazi_dw_dwb.dwb_evaluate_task_task_tag_day
           WHERE dt = '2020-11-17' and dh='00' AND label_id IN (3, 4, 5) AND is_del = 0
           group by clue_id,label_id
            ) a
       LEFT JOIN (
             SELECT 
                 id,
                 label_name
            FROM
            guazi_dw_dwb.dwb_evaluate_task_task_tag_config_day
            WHERE dt = '2020-11-17' and dh='00' AND is_del = 0
                 ) b 
        ON a.label_id = b.id
       )asdf 
       on a.clue_id=asdf.clue_id
        left join
        (
            select
                clue_id,
                evaluate_editor as car_source_evaluate_editor
            from guazi_dw_dwb.dwb_cars_car_source_day
            where dt = '2020-11-17' and dh = '00'
        ) cs
        on a.clue_id = cs.clue_id
        left join (
            select clue_id
                ,substr(created_at,1,19) as created_at
                ,row_number() over(partition by clue_id order by created_at asc) rn
           from guazi_dw_dwb.dwb_consign_car_liquidation_day
           where dt = '2020-11-17'
        ) ccl on a.clue_id = ccl.clue_id and ccl.rn = 1
left join
(
    select
        clue_id,
        after_evaluate_editor
    from
    (
        select
            clue_id, after_evaluate_editor,
            row_number() over(partition by clue_id order by created_at) num
        from guazi_dw_dwb.dwb_consign_task_extra_evaluate_editor_log_day
        where dt = '2020-11-17'
            and before_evaluate_editor = 0
            and after_evaluate_editor > 0
    ) x
    where num = 1
) editor_log
on a.clue_id = editor_log.clue_id
left join
(   -- 开放平台车源
    select 
        clue_id
    from guazi_dw_dwb.dwb_cars_car_source_tag_day
    where dt = '2020-11-17'
        and tag = 390
    group by clue_id
) open_platform
on a.clue_id = open_platform.clue_id
left join
(select 
      clue_id
     ,substr(updated_at,1,19) as consign_return_time  
    from
    (
      select
        clue_id
        ,updated_at
        ,refund_type
        ,row_number() over(partition by clue_id order by updated_at desc) as rnk
      from guazi_dw_dwb.dwb_consign_refund_task_day
      where dt='2020-11-17' and dh='00'
      and refund_status=3 --退款完成
      and refund_type = 0 -- 退款退车 
    )k
    where rnk=1
)qwe
on a.clue_id =qwe.clue_id
         "