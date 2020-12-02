    insert overwrite table guazi_dw_dwd.dwd_com_account_contract_ymd partition (dt = '{day_exec}')--二手车台账合同表

    select
        t1.id as contract_id,
        t1.contract_number as contract_number,
        t1.clue_id as clue_id,
        t1.raw_contract_id as raw_contract_id,
        t2.task_id as task_id,
        case when t1.business_line='c2c-csp' then 1
            when t5.source_type=26 then 2
            else t1.contract_source end as contract_source,
        t1.contract_type as contract_type,
        t1.city_id as city_id,
        t1.district_id as district_id,
        t1.service_price as sell_service_price, --优惠后应收服务费
        t1.loan_price as loan_price,
        t1.loan_service_price as loan_service_price,
        from_unixtime(t1.create_time,'yyyy-MM-dd HH:mm:ss') as create_time,
        from_unixtime(t1.sign_time,'yyyy-MM-dd HH:mm:ss') as sign_time,
        case when t1.transfer_time > 0 then from_unixtime(t1.transfer_time,'yyyy-MM-dd HH:mm:ss') end as transfer_time,
        case when t1.prepay_time>0 then from_unixtime(t1.prepay_time,'yyyy-MM-dd HH:mm:ss')
            when t1.prepay_time=0 and t1.refund_status=1 then from_unixtime(t1.refund_time,'yyyy-MM-dd HH:mm:ss')
            else null end as prepay_time,
        case when t1.sold_time=0 then null else from_unixtime(t1.sold_time,'yyyy-MM-dd HH:mm:ss') end as sold_time,
        case when t1.refund_time=0 then null else from_unixtime(t1.refund_time,'yyyy-MM-dd HH:mm:ss') end as refund_time,
        t1.buyer_phone_encrypt as buyer_phone_encode,
        t1.trader as trader,
        t1.dealer as dealer,
        t1.evaluator as evaluator,
        case when t1.deal_audit_time=0 then null else from_unixtime(t1.deal_audit_time,'yyyy-MM-dd HH:mm:ss') end as deal_audit_time,
        t1.task_operator as task_operator,
        t1.task_source_type as task_source_type,
        t1.refund_status as refund_status,
        t1.finance_refund_type as finance_refund_type,
        case when t9.qgg_type in ('2103','2105') then 1
            when t5.source_type = 27 then 2
            when t1.contract_sign_type = 'consignment'  then 1
            else 0 end as is_consigned,
        t1.refund_type as refund_type,
        t1.company_name as company_name,
        if(t1.business_line is null,t2.business_line,t1.business_line) as business_line,
        t4.special_audit as special_audit,
        t4.service_amount_expect as service_amount_expect,
        nvl(t6.tag_value, 0) as task_id_sale_type,
        t1.sales_service_fee,
        t1.outward_service as outward_service_fee,
        t1.maintenance_fee,
        t1.deal_price,
        if(t1.contract_sign_type is null,t2.contract_sign_type,t1.contract_sign_type) as contract_sign_type,
        case when t7.is_dealer is not null then 1 else  0 end  as is_dealer     -- 是否车商
        ,t1.deal_trader_id
        ,t1.sold_trader_id
        ,case when t2.transfer_to_id > 0 then t2.transfer_to_id else t1.trader
            end as refund_trader_id    -- 退车不退款提交人
        ,case when t9.qgg_type is not null then t9.qgg_type
            when t1.business_line = '2104' then '2104'
            when t5.source_type = 27 then null
            when t1.contract_sign_type = 'consignment' then '2105'
            end as qgg_type --全国购类型
        ,nvl(t10.refund_fee, 0) as refund_fee     -- 售车服务费返现
        ,nvl(t11.buyer_coupon, 0) + nvl(t11.saler_coupon, 0) as gift_coupon_fee   -- 双保券成本
        ,t12.call_user_id as call_user_id
        ,case when t9.sub_qgg_type is not null then t9.sub_qgg_type
            when t1.business_line = '2104' then '2104'
            when t5.source_type = 27 then null
            when t1.contract_sign_type = 'consignment' then '2105'
            end as sub_qgg_type, --全国购类型
        case when coalesce(t1.trade_father_order_number,'') != '' then t1.trade_father_order_number
            when t8.father_id <> '0' then t8.father_id end as trade_father_order_number, --全国购订单号
        t5.customer_id, --关联客户id
        t1.id as order_contract_id,
        -- business_type 1 全国购 2 严选 3 c2c 4 c2b 5 B全国购 6 B快卖 7 Q车源 9 尊享线索
        case when t14.source_type in (30,48,49) then 4
            when t15.clue_enjoy_vip_flag = 1 then 9
            when t1.contract_sign_type = 'qgg_qlocal' then 7
            when t9.qgg_type in ('2103','2105') then 2
            when t5.source_type = 27 then 6
            when t1.contract_sign_type = 'consignment'  then 2
            when t9.qgg_type in ('2101','2102','2104','23100','23200') then 1
            when t1.business_line = '2104' then 1
            when t1.business_line='c2c-csp' then 3
            when t5.source_type = 26 then 4
            when t1.contract_source = 1 then 3
            when t1.contract_source = 2 then 4
        end as business_type
        --source_system_type，1 老台账 2 订单中心 3 c2b
        ,'老台账' as source_system_type
        ,t2.contract_audit_status
        ,t1.intermediate_service_fee -- 售车居间服务费
        ,t1.guarantee_service_fee -- 售车保障服务费
        ,t13.original_deal_price  --金融优享原始车价
        ,case when t1.contract_invalid_time > 0 then from_unixtime(cast(t1.contract_invalid_time/1000 as int),'yyyy-MM-dd HH:mm:ss') end as contract_invalid_time--合同作废时间
        ,t1.trader as contract_create_trader
        ,null as car_service_fee
        ,t1.service_price as before_discount_sell_service_price --优惠前应收服务费
        ,t1.sales_service_fee as before_discount_sales_service_fee --优惠前售车服务费
        ,t1.maintenance_fee as before_discount_maintenance_fee --优惠前养护费
        ,0 as is_qgg_red_line
        ,case when t2.contract_audit_status = 5 and t2.contract_status = 1 and t2.refund_time > 0 then 1 else 0 end as is_electric_contract_invalid --电子合同作废标示[王佳、刘晓雅提供逻辑优惠券专用]
        ,case when t2.contract_audit_status = 5 and t2.contract_status = 1 and t2.refund_time > 0 then from_unixtime(t2.refund_time) end as electric_contract_invalid_time --电子合同作废时间[王佳、刘晓雅提供逻辑优惠券专用]
        ,t1.deal_price as bare_car_price
        ,t8.order_id as order_center_id
        ,case when t17.contract_id is not null then t17.refund_reason
            when t16.contract_number is not null then t16.type_name
            else '' end as refund_reason_name   --退款原因大类
        ,case when t17.contract_id is not null then t17.refund_reason_detail
            when t16.contract_number is not null then t16.sub_type_name
            else '' end as refund_reason_sub_name --退款原因细类
    from
    (
        select *
        from guazi_dw_dwb.dwb_accounting_contracts_day
        where dt='{day_exec}'
        and city_id not in (344, 345)
    )t1
    left join
    (
        select *
        from guazi_dw_dwb.dwb_guazi_contracts_vehicle_c2c_contract_day
        where dt='{day_exec}'
    )t2 on t1.raw_contract_id=t2.id
    left join
    (
        select *
            ,row_number() over(partition by raw_contract_id order by updated_at desc) num
        from guazi_dw_dwb.dwb_guazi_audit_contracts_day
        where dt='{day_exec}'
    )t4 on t4.raw_contract_id = t1.raw_contract_id and t4.num=1
    left join
    (
        select id
            ,is_consigned
            ,status_update_time
            ,appoint_status
            ,customer_id
            ,buyer_phone_encode
            ,create_time
            ,is_both_appoint
            ,source_type
        from guazi_dw_dwb.dwb_sale_appoint_task_day
        where dt ='{day_exec}'
    )t5 on t5.id=t2.task_id
    left join
    (   --表粒度为 带看工单号(task_id)+标签项(tag_type)
        --group by 只为保证工单粒度
        select task_id,
            tag_value,
            row_number() over (partition by task_id order by updated_at desc ) num
        from guazi_dw_dwb.dwb_sale_appoint_task_tag_day
        where  dt = '{day_exec}'  and tag_type=5
    ) t6 on t2.task_id=t6.task_id and t6.num=1
    left join
    (   --疑似车商
        select
            phone_encode,  dealer_category  as is_dealer
        from guazi_dw_dwd.dim_com_suspect_dealer_ymd
        where dt = '{day_exec}'
    ) t7 on t1.buyer_phone_encrypt = t7.phone_encode
    left join
    (   --合同去重取最新一条订单中心主订单ID
        select max(father_id) father_id
            ,contract_no
            ,max(id) as order_id
        from guazi_dw_dwb.dwb_order_center_orders_day
        where dt='{day_exec}'
        and type_id in ('2031','2040')
        -- and father_id <> '0'
        group by contract_no
    ) t8 on t1.contract_number = t8.contract_no
    left join
    (
        select id
            ,case when type_id like '2101%' then '2101'
                when type_id like '2102%' then '2102'
                when type_id like '2103%' then '2103'
                when type_id like '2105%' then '2105'
                else type_id end as qgg_type
            ,type_id as sub_qgg_type
        from guazi_dw_dwb.dwb_order_center_orders_day
        where dt='{day_exec}'
        and father_id = '0'
        and (
            type_id = '2104'
            or
            type_id like '2101%'
            or
            type_id like '2102%'
            or
            type_id like '2103%'
            or
            type_id like '2105%'
            or
            type_id like '231%'
            or
            type_id like '2320%')
    ) t9 on t8.father_id = t9.id
    left join
    (   -- 已售后折扣返现
        select
            contract_id,
            sum(refund_price) as refund_fee
        from guazi_dw_dwb.dwb_consign_sale_refund_day
        where dt = '{day_exec}'
        and refund_stage in (1, 2, 3, 4)    -- refund_stage:0 已售退款, 1 金融加免责返现, 2 保卖免责返现, 3 金融返现, 4 非金融免责返现
        group by contract_id
    ) t10 on t1.raw_contract_id = t10.contract_id
    left join
    (   -- 双保券成本
        select contract_number,
            nvl(get_json_object(buyer_gift_coupons,'$\[0\].cost'), 0) as buyer_coupon,  -- 买家自有赠品券成本
            nvl(get_json_object(gift_coupon_packet,'$.cost'), 0) as saler_coupon    -- 销售自有赠品券成本
        from
        (   --电子合同去重取最新一条
            select
                contract_number,
                buyer_gift_coupons,
                gift_coupon_packet,
                row_number() over (partition by contract_number order by created_at desc) num
            from guazi_dw_dwb.dwb_consign_sale_price_reduce_application_day
            where dt = '{day_exec}'
        ) x
        where num = 1
    ) t11 on t1.raw_contract_id = t11.contract_number
    left join
    (
        select task_id,call_user_id
        from
        (
            select last_scrp_id,task_id
            from guazi_dw_dwb.dwb_guazi_call_sale_clue_task_relation_day
            where dt='{day_exec}' and task_id<>0 and last_scrp_id<>0
            group by last_scrp_id,task_id
        ) a
        join
        (
            select operator as call_user_id,car_id as clue_id,created_at,id as process_id,result_type
            from guazi_dw_dwb.dwb_guazi_call_sale_clue_process_record_day
            where dt='{day_exec}'
        ) b on a.last_scrp_id = b.process_id
    ) t12 on t2.task_id = t12.task_id
    left join
    (   --表粒度为 订单号(order_id)+费用项(expense_id)
        --group by 只为保证订单粒度
        select order_id
            ,sum(cast(display_amount/100 as int)) as original_deal_price
        from guazi_dw_dwb.dwb_order_center_order_expense_day
        where dt='{day_exec}'
        and expense_id = '10003'
        group by order_id
    ) t13 on t8.father_id = t13.order_id
    left join (
        select sales_task_id
            ,store_id
            ,sale_create
            ,order_id
            ,customer_service_id
            ,user_id
            ,source_type
        from (
            select sales_task_id
                ,store_id
                ,id
                ,sale_create
                ,order_id
                ,customer_service_id -- added by zhangpeng91 2019-09-18
                ,user_id             -- added by wangyanfei5 2019-10-11
                ,source_type --48新尊享回购 49新尊享未招商
                ,row_number()over(partition by sales_task_id order by updated_at desc,created_at desc) num
            from guazi_dw_dwb.dwb_ctob_vehicle_auction_appoint_day
            where dt='{day_exec}'
            and sales_task_id <> 0
        ) as t1
         where num = 1
    ) t14 on t2.task_id = t14.sales_task_id
    left join (
        --表粒度为 车源号(clue_id)+标签项(tag)
        --group by 只为保证粒度
        select clue_id
            ,max(case when tag in (444,458) then 1 else 0 end) as clue_enjoy_vip_flag
        from guazi_dw_dwb.dwb_cars_car_source_tag_day
        where dt = '{day_exec}'
        and tag in (444,458) -- 444 458 均是新尊享标记 11月30日凌晨3点之前 新尊享的车商签约部分走老台账 只不过将历史数据中的部分444 update成了444
        and tag_status = 0
        group by clue_id
    ) t15 on t1.clue_id = t15.clue_id
    left join (
        select contract_number
            ,type_name
            ,sub_type_name
        from (
            select contract_number
                ,refund_conf_id
                ,type_name
                ,sub_type_name
                ,row_number() OVER (PARTITION BY contract_number ORDER BY refund_at DESC) row_num
            from (
                select contract_number
                    ,refund_conf_id
                    ,refund_at
                from guazi_dw_dwb.dwb_guazi_contracts_contract_refunds_day
                where dt = '{day_exec}'
                and refund_status = 2
            ) tta
            left join (
                select id
                    ,type_name
                    ,sub_type_name
                from guazi_dw_dwb.dwb_guazi_contracts_refund_conf_day
                where dt = '{day_exec}'
                and type_name not in ('已售退服务费')
            ) ttb on tta.refund_conf_id = ttb.id
        ) ta
        where row_num = 1
    ) t16 on t1.contract_number = t16.contract_number
    left join (
        select contract_id
            ,refund_reason
            ,refund_reason_detail
        from (
            select contract_id
                ,refund_reason
                ,refund_reason_detail
                ,created_at
                ,row_number() over(partition by contract_id order by created_at desc) as rn
            from (
                select contract_id,rule_conf_id,created_at
                from  guazi_dw_dwb.dwb_sale_fund_refund_flow_day
                where  dt = '{day_exec}'
                and contract_id != ''
                and refund_status = 2
            ) t1
            left join (
                select conf_id,refund_reason,refund_reason_detail
                from guazi_dw_dwb.dwb_sale_fund_refund_rule_conf_day
                where dt = '{day_exec}'
            ) t2 on t1.rule_conf_id = t2.conf_id
        ) ta
        where rn = 1
    ) t17 on t1.contract_number = t17.contract_id
    where t9.sub_qgg_type not in ('23100','23200','210111','210112','210211','210212'
        ,'21015','21025','210113','210213','23101','23201','210114','210214','21035','21055'
        ,'23102','23202','23103','23203','210115','210215','210116','210216'
        ,'23104','23105','210117','210118','21034','21054','23106','23107','210119','210120','23108','23109','23110') or t9.qgg_type is null
union all
    select null as contract_id
        ,t2.contract_no as contract_number
        ,t4.clue_id as clue_id
        ,t11.contract_id as raw_contract_id
        ,t4.task_id as task_id
        ,1 as contract_source
        ,null as contract_type
        ,t1.city_id as city_id
        ,coalesce(t1.district_id,t4.order_district_id,0) as district_id
        ,case when t15.is_transaction_unification = 1 then t13.sell_service_fee_amount
            when t4.business_line in ('all_country_city','open_platform') and t13.sell_service_fee_amount > 0
                then coalesce(t13.sell_service_fee_amount,0) - coalesce(t13.discount_service_fee_amount,0)
            when t15.is_transaction_unification = 0 and t1.qgg_type in ('23100','23200','2101','2102') and t13.sell_service_fee_amount > 0
                then coalesce(t13.sell_service_fee_amount,0) - coalesce(t13.discount_service_fee_amount,0)
            when t13.sell_service_fee_amount > 0 then t13.sell_service_fee_amount
            when t5.service_fee_amount > 0 then t5.service_fee_amount
            else 0.0 end as sell_service_price --优惠前应收服务费
        ,null as loan_price
        ,null as loan_service_price
        ,substr(t2.created_at,1,19) as create_time
        ,null as sign_time
        ,null as transfer_time
        ,t3.prepay_time
        ,t3.sold_time
        ,t3.refund_time
        ,coalesce(t4.phone,t7.buyer_phone_encode) as buyer_phone_encode
        ,t4.appoint_person as trader
        ,null as dealer
        ,coalesce(t14.evaluator,t12.uct_evaluator) as evaluator
        ,null as deal_audit_time
        ,null as task_operator
        ,null as task_source_type
        ,case when t2.seq_num > 1 then 2
            when t1.sub_qgg_type in ('23108','23109','23110') and t3.refund_time is not null then 2 --金牌车商全部全额退
            when t13.order_id is not null and t1.order_id > '20201001' and t13.service_fee_actual_amount_no_chewu = 0 then 2 --20年10月后订单，余额不计算异地与车务费用
		    when t13.order_id is not null and t1.order_id > '20201001' and t13.service_fee_amount_no_chewu > t13.service_fee_actual_amount_no_chewu then 3 --20年10月后订单，余额不计算异地与车务费用
            when t13.order_id is not null and t13.service_fee_actual_amount = 0 then 2
            when t13.order_id is not null and t13.service_fee_amount > t13.service_fee_actual_amount then 3
            when t13.order_id is null and t5.service_fee_actual_amount = 0 then 2
            when t13.order_id is null and t5.service_fee_amount > t5.service_fee_actual_amount then 3
            else 0 end as refund_status
        ,null as finance_refund_type
        ,case when t4.business_line = 'consign' then 1
            when t1.qgg_type in ('2103', '2105') then 1
            else 0 end as is_consigned
        ,null as refund_type
        ,null as company_name
        ,null as business_line
        ,null as special_audit
        ,null as service_amount_expect
        ,coalesce(t8.tag_value, 0) as task_id_sale_type
        ,case when t15.is_transaction_unification = 1 then t13.sales_service_fee
            when t4.business_line in ('all_country_city','open_platform') and t13.sales_service_fee > 0
                then coalesce(t13.sales_service_fee,0) - coalesce(t13.discount_sales_service_fee,0)
            when t15.is_transaction_unification = 0 and t1.qgg_type in ('23100','23200','2101','2102') and t13.sales_service_fee > 0
                then coalesce(t13.sales_service_fee,0) - coalesce(t13.discount_sales_service_fee,0)
            when t13.sales_service_fee > 0 then t13.sales_service_fee
            when t5.sales_service_fee > 0 then t5.sales_service_fee
            else 0.0 end as sales_service_fee --优惠后售车服务费
        ,case when t13.outward_service_fee > 0 then t13.outward_service_fee
            when t5.outward_service_fee > 0 then t5.outward_service_fee
            else 0.0 end as outward_service_fee
        ,case when t15.is_transaction_unification = 1 then t13.maintenance_fee
            when t4.business_line in ('all_country_city','open_platform') and t13.maintenance_fee > 0
                then coalesce(t13.maintenance_fee,0) - coalesce(t13.discount_maintenance_fee,0)
            when t15.is_transaction_unification = 0 and t1.qgg_type in ('23100','23200','2101','2102') and t13.maintenance_fee > 0
                then coalesce(t13.maintenance_fee,0) - coalesce(t13.discount_maintenance_fee,0)
            when t13.maintenance_fee > 0 then t13.maintenance_fee
            when t5.maintenance_fee > 0 then t5.maintenance_fee
            else 0.0 end as maintenance_fee --优惠后服务费
        ,t6.deal_price
        ,null as contract_sign_type
        ,case when t9.is_dealer is not null then 1 else  0 end as is_dealer
        ,coalesce(t14.prepay_seller,t12.uct_prepay_seller) as deal_trader_id
        ,coalesce(t14.sold_seller,t12.uct_sold_seller) as sold_trader_id
        ,coalesce(t14.de_prepay_seller,t12.uct_de_prepay_seller) as refund_trader_id
        ,case when t4.is_local = 1 and t4.business_line = 'all_country_city' then '2101'
            when t4.is_local = 0 and t4.business_line = 'all_country_city' then '2102'
            when t4.is_local = 1 and t4.business_line = 'consign' then '2105'
            when t4.is_local = 0 and t4.business_line = 'consign' then '2103'
            when t4.is_local = 1 and t4.business_line = 'open_platform' then '23100'
            when t4.is_local = 0 and t4.business_line = 'open_platform' then '23200'
            else t1.qgg_type end as qgg_type
        ,nvl(t11.refund_fee, 0) as refund_fee     -- 售车服务费返现
        ,nvl(t11.buyer_coupon, 0) + nvl(t11.saler_coupon, 0) as gift_coupon_fee   -- 双保券成本
        ,t10.call_user_id
        ,t1.sub_qgg_type as sub_qgg_type
        ,t1.order_id as trade_father_order_number
        ,t7.customer_id
        ,t2.id as order_contract_id
        -- business_type 1 全国购 2 严选 3 c2c 4 c2b 5 B全国购 6 B快卖
        ,case when t1.sub_qgg_type in ('23108','23109','23110') then 11
            when t4.business_line in ('all_country_city','open_platform') then 1
            when t4.business_line = 'consign' then 2
            when t1.qgg_type in ('23100','23200','2101','2102') then 1
            when t1.qgg_type in ('2103', '2105') then 2 end as business_type
        --source_system_type，1 老台账 2 订单中心 3 c2b
        ,'订单中心' as source_system_type
        ,t11.contract_audit_status
        ,null intermediate_service_fee -- 售车居间服务费
        ,null guarantee_service_fee -- 售车保障服务费
        ,t13.original_deal_price  --原始车价
        ,case when t14.contract_invalid_time > 0 then from_unixtime(cast(t14.contract_invalid_time/1000 as int),'yyyy-MM-dd HH:mm:ss') end as contract_invalid_time--合同作废时间
        ,t11.appoint_person as contract_create_trader
        ,case when t13.car_service_fee > 0 then t13.car_service_fee
            when t5.car_service_fee > 0 then t5.car_service_fee
            else 0.0 end as car_service_fee
        ,case when t15.is_transaction_unification = 1 and t13.display_service_fee_amount > 0 then t13.display_service_fee_amount
            when t13.sell_service_fee_amount > 0 then t13.sell_service_fee_amount
            when t5.service_fee_amount > 0 then t5.service_fee_amount
            else 0.0 end as before_discount_sell_service_price --优惠前应收服务费
        ,case when t15.is_transaction_unification = 1 and t13.display_sales_service_fee > 0 then t13.display_sales_service_fee
            when t13.sales_service_fee > 0 then t13.sales_service_fee
            when t5.sales_service_fee > 0 then t5.sales_service_fee
            else 0.0 end as before_discount_sales_service_fee --优惠前售车服务费
        ,case when t15.is_transaction_unification = 1 and t13.display_maintenance_fee > 0 then t13.display_maintenance_fee
            when t13.maintenance_fee > 0 then t13.maintenance_fee
            when t5.maintenance_fee > 0 then t5.maintenance_fee
            else 0.0 end as before_discount_maintenance_fee --优惠前养护费
        ,0 as is_qgg_red_line
        ,case when t11.contract_audit_status = 5 and t11.contract_status = 1 and t11.refund_time > 0 then 1 else 0 end as is_electric_contract_invalid --电子合同作废标示
        ,case when t11.contract_audit_status = 5 and t11.contract_status = 1 and t11.refund_time > 0 then from_unixtime(t11.refund_time) end as electric_contract_invalid_time --电子合同作废时间[王佳、刘晓雅提供逻辑优惠券专用]
        ,case when t15.is_qgg_exhibition_price = 1 then t13.bare_car_price
            else t6.deal_price end as bare_car_price --裸车价
        ,t2.id as order_center_id
        ,case when t17.contract_id is not null then t17.refund_reason
            when t16.contract_number is not null then t16.type_name
            else '' end as refund_reason_name   --退款原因大类
        ,case when t17.contract_id is not null then t17.refund_reason_detail
            when t16.contract_number is not null then t16.sub_type_name
            else '' end as refund_reason_sub_name --退款原因细类
    from
    (
        select id as order_id
            ,case when type_id like '2101%' then '2101'
                when type_id like '2102%' then '2102'
                when type_id like '231%' then '23100'
                when type_id like '2320%' then '23200'
                when type_id like '2103%' then '2103'
                when type_id like '2105%' then '2105'
                else type_id end as qgg_type
            ,type_id as sub_qgg_type
            ,get_json_object(add_code,'$.city') as city_id
            ,get_json_object(add_code,'$.district') as district_id
            ,order_ver
        from guazi_dw_dwb.dwb_order_center_orders_day
        where dt = '{day_exec}'
        and get_json_object(add_code,'$.city') not in ('344','345')
        and type_id in ('23100','23200','210111','210112','210211','210212','21015'
            ,'21025','210113','210213','23101','23201','210114','210214','21035','21055'
            ,'23102','23202','23103','23203','210115','210215','210116','210216','23104','23105','210117','210118'
            ,'21034','21054','23106','23107','210119','210120','23108','23109','23110')
    ) t1
    join
    (
        select id
            ,father_id
            ,type_id
            ,created_at
            ,contract_no
            ,valid_status
            ,row_number() over (partition by father_id  order by created_at desc) seq_num
        from
        (   --合同号去重 保证合同粒度
            select id
                ,father_id
                ,type_id
                ,substr(created_at,1,19) as created_at
                ,contract_no
                ,valid_status
                ,row_number() over (partition by contract_no order by created_at desc) num
            from guazi_dw_dwb.dwb_order_center_orders_day
            where dt = '{day_exec}'
            and type_id in ('2031','2040')
            and father_id <> '0'
        ) tta
        where tta.num = 1
    ) t2 on t1.order_id = t2.father_id
    left join
    (   --订单状态变化日志
        --group by 保证订单粒度,减少表关联优化SQL
        select order_id
            ,min(case when state = '10320000' then substr(state_updated,1,19) end) as prepay_time
            ,max(case when state = '10920000' then substr(state_updated,1,19) end) as sold_time
            ,max(case when state = '20100000' then substr(state_updated,1,19) end) as refund_time
        from guazi_dw_dwb.dwb_order_center_order_status_transfer_day
        where dt = '{day_exec}'
        and state in ('10320000','10920000','20100000')
        group by order_id
    ) t3 on t2.id = t3.order_id
    left join
    (
        select tta.order_id
            ,tta.clue_id
            ,tta.task_id
            ,tta.appoint_person
            ,tta.business_line
            ,tta.is_local
            ,tta.customer_id
            ,tta.order_district_id
            ,ttb.phone
        from (
            select order_id
                ,clue_id
                ,task_id
                ,sales_id as appoint_person
                ,business_line
                ,case when inflow_site_id > 0 and outflow_site_id > 0 and inflow_site_id = outflow_site_id then 1
                    when inflow_site_id > 0 and outflow_site_id > 0 and inflow_site_id != outflow_site_id then 0
                    else -1 end as is_local
                ,order_district_id
                ,customer_id
            from guazi_dw_dwb.dwb_sale_order_orders_day
            where dt = '{day_exec}'
            and city_id not in (344, 345)
        ) tta
        left join (
            select user_id,phone
            from guazi_dw_dwb.dwb_user_center_user_info_subtable_all_day
            where dt = '{day_exec}'
        ) ttb on tta.customer_id = ttb.user_id
    ) t4 on t4.order_id = t1.order_id
    left join
    (   --表粒度为 订单号(order_id)+费用项(expense_id)
        --group by 只为保证订单粒度
        select order_id
            ,sum(case when expense_id in ('10001','10002','10020','10021','10017','10018','10099','10123') then cast(amount/100 as DECIMAL(18,2)) end) as service_fee_amount
            ,sum(case when expense_id in ('10001','10002','10020','10021','10017','10018','10099','10123') then cast(actual_amount/100 as DECIMAL(18,2)) end) as service_fee_actual_amount
            ,sum(case when expense_id = '10001' then cast(amount/100 as DECIMAL(18,2)) end) as sales_service_fee
            ,sum(case when expense_id = '10021' then cast(amount/100 as DECIMAL(18,2)) end) as maintenance_fee
            ,sum(case when expense_id = '10020' then cast(amount/100 as DECIMAL(18,2)) end) as outward_service_fee
            ,sum(case when expense_id = '10123' then cast(amount/100 as DECIMAL(18,2)) end) as car_service_fee
        from guazi_dw_dwb.dwb_order_center_order_expense_day
        where dt = '{day_exec}'
        and expense_id in ('10001','10002','10020','10021','10017','10018','10099','10123')
        group by order_id
    ) t5 on t5.order_id = t2.id
    left join
    (   --表粒度为 订单号(order_id)+费用项(expense_id)
        --group by 只为保证订单粒度
        select order_id
            ,sum(amount/100) as deal_price
        from guazi_dw_dwb.dwb_order_center_order_expense_day
        where dt = '{day_exec}'
        and expense_id = '10003'
        group by order_id
    ) t6 on t6.order_id = t2.id
    left join
    (
        select id,buyer_phone_encode ,customer_id,city_id
            ,district_id
        from guazi_dw_dwb.dwb_sale_appoint_task_day
        where dt ='{day_exec}'
        and city_id not in (344, 345)
    ) t7 on t7.id = t4.task_id
    left join
    (   --表粒度为 带看工单号(task_id)+标签项(tag_type)
        --group by 只为保证工单粒度
        select task_id,
            tag_value,
            row_number() over (partition by task_id order by updated_at desc ) num
        from guazi_dw_dwb.dwb_sale_appoint_task_tag_day
        where  dt = '{day_exec}'
        and tag_type=5
    ) t8 on t8.task_id = t4.task_id and t8.num=1
    left join
    (
        select
            phone_encode,  dealer_category  as is_dealer
        from guazi_dw_dwd.dim_com_suspect_dealer_ymd
        where dt = '{day_exec}'
    ) t9 on t7.buyer_phone_encode = t9.phone_encode
    left join
    (
        select task_id,call_user_id
        from
        (
            select last_scrp_id,task_id
            from guazi_dw_dwb.dwb_guazi_call_sale_clue_task_relation_day
            where dt='{day_exec}' and task_id<>0 and last_scrp_id<>0
            group by last_scrp_id,task_id
        ) a
        join
        (
            select operator as call_user_id,car_id as clue_id,created_at,id as process_id,result_type
            from guazi_dw_dwb.dwb_guazi_call_sale_clue_process_record_day
            where dt='{day_exec}'
        ) b on a.last_scrp_id = b.process_id
    ) t10 on t4.task_id = t10.task_id
    left join
    (   --保证电子合同contract_number 唯一
        select tta.contract_number
            ,id as contract_id
            ,contract_audit_status
            ,platform
            ,contract_sign_type
            ,refund_fee
            ,buyer_coupon
            ,saler_coupon
            ,appoint_person
            ,contract_status
            ,refund_time
            ,row_number() over (partition by tta.contract_number order by id desc) num
        from (
            select  contract_number
                ,id
                ,contract_audit_status
                ,platform
                ,contract_sign_type
                ,appoint_person
                ,contract_status
                ,refund_time
            from guazi_dw_dwb.dwb_guazi_contracts_vehicle_c2c_contract_day
            where dt= '{day_exec}'
        ) tta
        left join (   -- 已售后折扣返现
            select
                contract_id,
                sum(refund_price) as refund_fee
            from guazi_dw_dwb.dwb_consign_sale_refund_day
            where dt = '{day_exec}'
            and refund_stage in (1, 2, 3, 4)    -- refund_stage:0 已售退款, 1 金融加免责返现, 2 保卖免责返现, 3 金融返现, 4 非金融免责返现
            group by contract_id
        ) ttb on tta.id = ttb.contract_id
        left join (   -- 双保券成本
            select contract_number
                ,get_json_object(buyer_gift_coupons,'$\[0\].cost') as buyer_coupon  -- 买家自有赠品券成本
                ,get_json_object(gift_coupon_packet,'$.cost') as saler_coupon    -- 销售自有赠品券成本
            from
            (
                select
                    contract_number,
                    buyer_gift_coupons,
                    gift_coupon_packet,
                    row_number() over (partition by contract_number order by created_at desc) num
                from guazi_dw_dwb.dwb_consign_sale_price_reduce_application_day
                where dt = '{day_exec}'
            ) x
            where num = 1
        ) ttc on tta.id = ttc.contract_number
    ) t11 on t2.contract_no = t11.contract_number and t11.num = 1
    left join
    (
        select busi_obj_number
            ,uct_prepay_seller
            ,uct_sold_seller
            ,uct_de_prepay_seller
            ,uct_evaluator
        from guazi_dw_dwb.dwb_ps_accounting_ti_unit_main_info_day
        where dt='{day_exec}'
        and busi_category ='10'         -- 业务大类
        and busi_obj_source_type ='101000101'  -- 业务对象来源
    ) t12 on t1.order_id = t12.busi_obj_number
    left join
    (   --表粒度为 订单号(order_id)+费用项(expense_id)
        --group by 只为保证订单粒度
        select order_id
            ,sum(case when expense_id in ('10001','10002','10021','10017','10018','10099') then amount end) as service_fee_amount_no_chewu
            ,sum(case when expense_id in ('10001','10002','10021','10017','10018','10099') then actual_amount end) as service_fee_actual_amount_no_chewu
            ,sum(case when expense_id in ('10001','10002','10020','10021','10017','10018','10099','10123','10169') then cast(amount/100 as DECIMAL(18,2)) end) as sell_service_fee_amount
            ,sum(case when expense_id in ('10001','10002','10020','10021','10017','10018','10099')
                then cast(amount/100 as DECIMAL(18,2)) end) as service_fee_amount
            ,sum(case when expense_id in ('10001','10002','10020','10021','10017','10018','10099')
                then cast(actual_amount/100 as DECIMAL(18,2)) end) as service_fee_actual_amount
            ,sum(case when expense_id in ('10001','10002','10020','10021','10017','10018','10099','10123','10169')
                then cast(display_amount/100 as DECIMAL(18,2)) end) as display_service_fee_amount
            ,sum(case when expense_id in ('10001','10002','10020','10021','10017','10018','10099','10123','10169')
                then cast(discount_amount/100 as DECIMAL(18,2)) end) as discount_service_fee_amount
            ,sum(case when expense_id = '10003' then cast(display_amount/100 as int) end) as original_deal_price
            ,sum(case when expense_id = '10001' then cast(amount/100 as DECIMAL(18,2)) end) as sales_service_fee
            ,sum(case when expense_id = '10021' then cast(amount/100 as DECIMAL(18,2)) end) as maintenance_fee
            ,sum(case when expense_id = '10020' then cast(amount/100 as DECIMAL(18,2)) end) as outward_service_fee
            ,sum(case when expense_id = '10123' then cast(amount/100 as DECIMAL(18,2)) end) as car_service_fee
            ,sum(case when expense_id = '10001' then cast(display_amount/100 as DECIMAL(18,2)) end) as display_sales_service_fee
            ,sum(case when expense_id = '10021' then cast(display_amount/100 as DECIMAL(18,2)) end) as display_maintenance_fee
            ,sum(case when expense_id = '10001' then cast(discount_amount/100 as DECIMAL(18,2)) end) as discount_sales_service_fee
            ,sum(case when expense_id = '10021' then cast(discount_amount/100 as DECIMAL(18,2)) end) as discount_maintenance_fee
            ,sum(case when expense_id = '10124' then cast(amount/100 as DECIMAL(18,2)) end) as bare_car_price
        from guazi_dw_dwb.dwb_order_center_order_expense_day
        where dt='{day_exec}'
        and expense_id in ('10001','10002','10003','10020','10021','10017','10018','10099','10123','10124','10169')
        group by order_id
    ) t13 on t1.order_id = t13.order_id
    left join (
        select contract_no
            ,de_prepay_seller --待已定销售人
            ,prepay_seller --已定销售人
            ,sold_seller --已售人
            ,evaluator --评估师
            ,contract_invalid_time --合同作废时间
        from guazi_dw_dwb.dwb_ps_accounting_ti_unit_acc_contracts_day
        where dt = '{day_exec}' and deleted_at = 0
    ) t14 on t2.contract_no = t14.contract_no and t1.order_ver = 'v20191018'
    left join ( --严选全国购一体化
        select order_id
            ,max(case when tag_signal = 'transaction_unification' then 1 else 0 end) is_transaction_unification
            ,max(case when tag_signal = 'qgg_exhibition_price' then 1 else 0 end) is_qgg_exhibition_price
        from guazi_dw_dwb.dwb_sale_order_order_tags_day
        where dt = '{day_exec}'
        and tag_signal in ('transaction_unification','qgg_exhibition_price')
        and tag_status = 1
        group by order_id
    ) t15 on t1.order_id = t15.order_id
    left join (
        select contract_number
            ,type_name
            ,sub_type_name
        from (
            select contract_number
                ,refund_conf_id
                ,type_name
                ,sub_type_name
                ,row_number() OVER (PARTITION BY contract_number ORDER BY refund_at DESC) row_num
            from (
                select contract_number
                    ,refund_conf_id
                    ,refund_at
                from guazi_dw_dwb.dwb_guazi_contracts_contract_refunds_day
                where dt = '{day_exec}'
                and refund_status = 2
            ) tta
            left join (
                select id
                    ,type_name
                    ,sub_type_name
                from guazi_dw_dwb.dwb_guazi_contracts_refund_conf_day
                where dt = '{day_exec}'
                and type_name not in ('已售退服务费')
            ) ttb on tta.refund_conf_id = ttb.id
        ) ta
        where row_num = 1
    ) t16 on t2.contract_no = t16.contract_number
    left join (
        select contract_id
            ,refund_reason
            ,refund_reason_detail
        from (
            select contract_id
                ,refund_reason
                ,refund_reason_detail
                ,created_at
                ,row_number() over(partition by contract_id order by created_at desc) as rn
            from (
                select contract_id,rule_conf_id,created_at
                from  guazi_dw_dwb.dwb_sale_fund_refund_flow_day
                where  dt = '{day_exec}'
                and contract_id != ''
                and refund_status = 2
            ) t1
            left join (
                select conf_id,refund_reason,refund_reason_detail
                from guazi_dw_dwb.dwb_sale_fund_refund_rule_conf_day
                where dt = '{day_exec}'
            ) t2 on t1.rule_conf_id = t2.conf_id
        ) ta
        where rn = 1
    ) t17 on t2.contract_no = t17.contract_id
union all
    select null as contract_id
        ,t1.contract_id as contract_number
        ,t1.clue_id as clue_id
        ,null as raw_contract_id
        ,t13.task_id as task_id
        ,2 as contract_source
        ,null as contract_type
        ,t1.city_id as city_id
        ,case when t1.order_type in (8,9,10,11) then district_id else 0 end as district_id
        ,t5.service_fee_amount/100 as sell_service_price --应收服务费
        ,null as loan_price
        ,null as loan_service_price
        ,t1.create_time
        ,null as sign_time
        ,null as transfer_time
        ,t3.prepay_time
        ,t3.sold_time
        ,case when t3.prepay_time <= t3.cancel_time then t3.cancel_time end as refund_time
        ,t1.buyer_phone_encrypt
        ,case when t1.order_type = 11 then t12.operator_id when t1.order_type =10 then t1.operator_id end as trader
        ,null as dealer
        ,t10.uct_evaluator as evaluator
        ,null as deal_audit_time
        ,null as task_operator
        ,null as task_source_type
        ,case when t5.service_fee_actual_amount = 0 then 2
            when t5.service_fee_amount > t5.service_fee_actual_amount then 3
            else 0 end refund_status
        ,null as finance_refund_type
        ,case when t4.clue_id is not null then 1
            when t1.order_type in (1,2) then 2
            else 0 end as is_consigned
        ,null as refund_type
        ,null as company_name
        ,null as business_line
        ,null as special_audit
        ,null as service_amount_expect
        ,0 as task_id_sale_type
        ,t5.sales_service_fee
        ,t5.outward_service_fee
        ,t5.maintenance_fee
        ,t6.deal_price
        ,null as contract_sign_type
        ,case when t9.is_dealer is not null then 1 else  0 end as is_dealer
        ,case when t1.order_type = 11 then t12.operator_id when t1.order_type =10 then t1.operator_id else t10.uct_prepay_seller end as deal_trader_id
        ,case when t1.order_type = 11 then t12.operator_id when t1.order_type =10 then t1.operator_id else t10.uct_sold_seller end as sold_trader_id
        ,t10.uct_de_prepay_seller as refund_trader_id
        ,t2.qgg_type
        ,null as refund_fee
        ,null as gift_coupon_fee
        ,null as call_user_id
        ,t2.type_id as sub_qgg_type
        ,t1.order_id as trade_father_order_number
        ,t1.buyer_user_id as customer_id
        ,t1.order_id as order_contract_id
        -- business_type 1 全国购 2 严选 3 c2c 4 c2b 5 B全国购 6 B快卖 8 红线车 9 尊享线索 10 4S转B
        ,case when t1.order_type = 8 then 9
            when t1.order_type = 9  and t11.car_type = 0 and t11.buyback = 1 then 4
            when t1.order_type = 10 then 4  --普B切订单类型
            when t1.order_type = 11 then 10
            when t2.is_qgg_redline = 1 then 1
            when t4.clue_id is not null then 2
            when t1.order_type in (1,2) then 6
            else 5 end as business_type
        --source_system_type，1 老台账 2 订单中心 3 c2b
        ,'c2b' as source_system_type
        ,null as contract_audit_status
        ,null intermediate_service_fee -- 售车居间服务费
        ,null guarantee_service_fee -- 售车保障服务费
        ,t5.original_deal_price
        ,null as contract_invalid_time
        ,null as contract_create_trader
        ,null as car_service_fee
        ,t5.service_fee_amount/100 as before_discount_sell_service_price --优惠前应收服务费
        ,t5.sales_service_fee as before_discount_sales_service_fee --优惠前售车服务费
        ,t5.maintenance_fee as before_discount_maintenance_fee --优惠前养护费
        ,case when t2.is_qgg_redline = 1 then 1 else 0 end as is_qgg_red_line
        ,0 as is_electric_contract_invalid --电子合同作废标示
        ,null as electric_contract_invalid_time --电子合同作废时间[王佳、刘晓雅提供逻辑优惠券专用]
        ,t6.deal_price as bare_car_price --裸车价
        ,t1.order_id as order_center_id
        ,null as refund_reason_name   --退款原因大类
        ,null as refund_reason_sub_name --退款原因细类
    from
    (
        select gz_order_id as order_id
            ,clue_id
            ,contract_id
            ,case when order_type in (8,9,10,11) then source_city_id else destination_city_id end as city_id
            ,district_id
            ,buyer_phone_encrypt
            ,order_type
            ,buyer_user_id
            ,from_unixtime(create_time,'yyyy-MM-dd HH:mm:ss') as create_time
            ,operator_id
            ,order_id as auction_order_id
        from guazi_dw_dwb.dwb_ctob_trade_order_day
        where dt = '{day_exec}'
        and source_city_id not in(344,345)
        and id not in (42197,42198,42199) --剔除异数据
    ) t1
    left join (
        select id
            ,type_id
            ,case when type_id like '2201%' then '2201'
                else '2202' end as qgg_type
            ,case when get_json_object(ext,'$.is_ctb')='true' then 1 else 0 end as is_qgg_redline
        from guazi_dw_dwb.dwb_order_center_orders_day
        where dt = '{day_exec}'
    ) t2 on t1.order_id = t2.id
    left join
    (   --订单状态变化日志
        --group by 保证订单粒度,减少表关联优化SQL
        select order_id
            ,min(case when state = '10320000' then substr(state_updated,1,19) end) as prepay_time
            ,max(case when state = '10920000' then substr(state_updated,1,19) end) as sold_time
            ,max(case when state = '3' then substr(state_updated,1,19) end) as cancel_time
        from guazi_dw_dwb.dwb_order_center_order_status_transfer_day
        where dt = '{day_exec}'
        and state in ('10320000','10920000','3')
        group by order_id
    ) t3 on t1.order_id = t3.order_id
    left join
    (
        select clue_id
        from guazi_dw_dwb.dwb_cars_car_source_tag_day
        where dt = '{day_exec}'
        and tag in (107,139,147,311,312) -- 老斩仓107,139,147   新斩仓311,312
        and tag_status = 0
        group by clue_id
    ) t4 on t1.clue_id = t4.clue_id
    left join
    (   --表粒度为 订单号(order_id)+费用项(expense_id)
        --group by 只为保证订单粒度
        select order_id
            ,sum(case when expense_id in ('10001','10020','10021') then amount end) as service_fee_amount
            ,sum(case when expense_id in ('10001','10020','10021') then actual_amount end) as service_fee_actual_amount
            ,sum(case when expense_id = '10003' then cast(display_amount/100 as int) end) as original_deal_price
            ,sum(case when expense_id = '10001' then cast(actual_amount/100 as DECIMAL(18,2)) end) as sales_service_fee
            ,sum(case when expense_id = '10021' then cast(amount/100 as DECIMAL(18,2)) end) as maintenance_fee
            ,sum(case when expense_id = '10020' then cast(amount/100 as DECIMAL(18,2)) end) as outward_service_fee
        from guazi_dw_dwb.dwb_order_center_order_expense_day
        where dt = '{day_exec}'
        and expense_id in ('10001','10003','10020','10021')
        group by order_id
    ) t5 on t5.order_id = t1.order_id
    left join
    (   --表粒度为 订单号(order_id)
        --group by 只为保证订单粒度
        select order_id
            ,actual_price/100 as deal_price
            ,row_number() over(partition by order_id order by id desc) rn
        from guazi_dw_dwb.dwb_order_center_order_product_day
        where dt = '{day_exec}'
    ) t6 on t6.order_id = t1.order_id and t6.rn = 1
    left join
    (
        select
            phone_encode,  dealer_category  as is_dealer
        from guazi_dw_dwd.dim_com_suspect_dealer_ymd
        where dt = '{day_exec}'
    ) t9 on t1.buyer_phone_encrypt = t9.phone_encode
    left join
    (
        select busi_obj_number
            ,uct_prepay_seller
            ,uct_sold_seller
            ,uct_de_prepay_seller
            ,uct_evaluator
        from guazi_dw_dwb.dwb_ps_accounting_ti_unit_main_info_day
        where dt='{day_exec}'
        and busi_category ='10'         -- 业务大类
        and busi_obj_source_type ='101000101'  -- 业务对象来源
    ) t10 on t1.order_id = t10.busi_obj_number
    left join (
        select clue_id
            ,user_id
            ,car_type
            ,buyback
            ,created_at
            ,row_number() over (partition by clue_id,user_id order by created_at desc) num
        from guazi_dw_dwb.dwb_ctob_vehicle_auction_buy_back_day
        where dt='{day_exec}'
    ) as t11 on t1.clue_id = t11.clue_id and t1.buyer_user_id = t11.user_id and t11.num = 1
    left join (
        select order_id
            ,operator_id
            ,row_number()over(partition by order_id order by id desc) as rn
        from guazi_dw_dwb.dwb_ctob_trade_task_day
        where dt='{day_exec}'
    ) as t12 on t1.order_id = t12.order_id and t12.rn = 1
    left join
    (
       select order_id as auction_order_id,
              task_id
         from
             (
              select tmp1.order_id,
                     tmp2.id as task_id,
                     row_number() over(partition by tmp1.order_id order by tmp1.id desc) rn
                from
                     (
                     select *
                       from guazi_dw_dwb.dwb_ctob_vehicle_auction_appoint_day
                      where dt='{day_exec}'
                        and sales_task_id>0
                     ) tmp1
                inner join
                     (
                     select *
                       from guazi_dw_dwb.dwb_sale_appoint_task_day
                      where dt ='{day_exec}'
                     ) tmp2
                     on tmp1.sales_task_id=tmp2.id
              ) tmp
         where tmp.rn=1
      ) t13
      on t1.auction_order_id=t13.auction_order_id