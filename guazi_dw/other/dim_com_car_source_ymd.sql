

alter table guazi_dw_dwd.dim_com_car_source_ymd
add columns (SKU_Desc string, SKU string) cascade;

alter table guazi_dw_dwd.dim_com_car_source_ymd
add columns (stop_sale_time string comment '停售时间', unshelf_time string comment '下架时间') cascade;


    set hive.auto.convert.join=false;
    set hive.exec.parallel=true;
    insert overwrite table guazi_dw_dwd.dim_com_car_source_ymd partition (dt ='{run_date}')--二手车车源维度表
    select
    a.id as source_id,              --车源上架id
    a.clue_id,                      --线索id
    a.car_id,                       --车型id
    a.city_id,                      --车源所在城市id
    a.district_id,                  --地区id
    a.dealer,                       --售车客服
    a.evaluator,                    --评估师id
    a.license_date as license_year, --年限-年
    a.license_month,                --年限-月
    a.road_haul,                    --里程
    a.transfer_num,                 --过户次数
    a.create_operator,              --上架人
    a.car_year,                     --车型年数
    a.stop_sale_reason,             --停售原因
    a.source_level,                 --上线车源等级（1：A级，2：B级，3：C级,99:默认值）
    a.auto_type,                    --车辆类型
    a.carriages,                    --车辆结构（1单厢 2两厢 3三厢)
    a.fuel_type,                    --燃油类型（1电力 2插电式混动 3增程式电动 4油电混合
    a.gearbox,                      --变速箱(1 手动 2自动)
    a.emission_standard,            --排放标准
    a.car_color,                    --颜色
    a.guobie,                       --国别
    a.seats,                        --座位数
    a.minor_category_id,            --自定义车型名称
    a.tag_id,                       --车系ID
    a.minor_category_name,          --品牌
    a.tag_name,                     --车系名称
    cast(a.air_displacement as double) as air_displacement,     --排量
    a.gender,                       --性别 （1男，2女)
    a.special_audit,                --特批(0无 1.车况确认报备 2.年限里程车型特批 3.无登记证,过户次数报备)
    a.is_unique_car_source,         --是否是独家车源（1：是；0：否)
    a.platform,                     --上架平台（位运算：1(c2c) 2(c2b)  3(c2c&c2b)）
    coalesce(car_tag.sale_type_tag, a.sale_type) as sale_type,                             --销售类型(0虚拟 1寄售)
    case when a.car_source_status = 3 and a.status_update_time>0 and a.status_update_time>a.create_time
         then datediff(from_unixtime(a.status_update_time), from_unixtime(a.create_time))
         when a.car_source_status in (1, 2, 5) and a.prepay_time>=0 and a.prepay_time>a.create_time
         then datediff(from_unixtime(a.prepay_time), from_unixtime(a.create_time))
         else datediff(from_unixtime(unix_timestamp()),from_unixtime(a.create_time)) end as on_shelf_days,  --上架天数
    j.appointment_person as evaluation_custom_service,      --评估人
    a.plate_city_id,                --车牌所属城市
    a.evaluate_level,               --评估等级
    a.evaluate_score,               --评估师评分
    a.pub_city_id,                  --上架城市
    coalesce(car_tag.is_sys_source,0) as is_sys_source,  -- 车源渠道是否来自系统分配
    coalesce(car_tag.is_evaluator_source,0) as is_evaluator_source,  -- 车源状态是否来自评估师推荐
    null as is_consign_city_shelf,  --是否保卖城市
    a.pub_district_id,              --上架城市的区域
    a.vin_encrypt,                  --vin码密文
    coalesce(car_tag.zhancang_tag,0) as zhancang_tag,  --斩仓标签
    coalesce(car_tag.recheck_tag,0) as recheck_tag,   --复检标签
    case when car_tag.is_virtual_shelf = 1 then 2
        else coalesce(firp.first_platform, 0)
        end as first_platform,                                    --首次上架平台
    a.car_source_status,                                                                  --车源状态
    case when a.car_source_status = 0                                                     --0在售
        and a.platform in (1, 3)                                                          --上架平台
        and ctl.audit_at is not null                                                   --审核时间
        and a.pub_city_id not in (344, 345)                                               --上架城市id
        and wcg.goods_status in (1, 2, 3)                                                 --货物状态（1待入库、2在库、3待出库、4已出库）
        and wcg.hostling_status = 10                                                      --整备状态（1待整备 5进行中 10整备完成）
        then 1 else 0 end as consign_can_sell,                                            --保卖可售
    from_unixtime(a.create_time,'yyyy-MM-dd HH:mm:ss') as create_time,                    --上架时间
    coalesce(n.tag,0) as insurance_tag,                                                   --承保标识
    if(a.prepay_time > 0, from_unixtime(a.prepay_time,'yyyy-MM-dd HH:mm:ss'), null) as prepay_time,     --定车时间
    substr(ctl.audit_at, 1, 19) as consign_contract_audit_time                            --保卖合同审核时间
    ,a.price                                                                              --出售价格
    ,a.deal_price                                                                         --成交价格
    ,evce.car_owner as is_public_account     -- 车辆所有者性质（1：公户 2：私户 3：公转私）
    ,che300.dealer_buy_price               -- 车商收购价
    ,che300.low_price                      -- 车况一般的个人交易价
    ,che300.good_price                     -- 车况良好的个人交易价
    ,che300.high_price,                    -- 车况优秀的个人交易价
    wchr.is_transfer as transfer_intact,   -- 清单复检是否可过户 1可过户 0不可过户
    cbro.status as violation_status,       -- 是否违章 -1未知 0未处理 1已处理
    case when ccse.is_date=1 then ccse.strong_insurance_full_date end as strong_insurance_full_date,    -- 交强险日期 yyyy-MM-dd
    a.year_inspection_date as year_inspection_date,   -- 年检到期年月 yyyy-MM
    case when wchr.is_transfer = 1
        and car_tag.pledge_create_time is null  -- 不是抵押车
        and (cbro.status <> 0 or cbro.status is null)      -- 无未处理违章记录
        and ccse.is_date=1 and ccse.strong_insurance_full_date >= '{run_date}'  -- 交强险日期大于当日
        and a.year_inspection_date >= '{run_date}' then 1 else 0 end    -- 年检年月大于等于当月
        as can_transfer,     -- 是否可过户，1是 0否
  concat((case when year('{run_date}') - license_date between 0 and 1 then '0-1年' -- 0-1年
        when year('{run_date}') - license_date between 1 and 3 then  '1-3年' -- 1-3年
        when year('{run_date}') - license_date between 3 and 5 then '3-5年' -- 3-5年
        else '5年+' end),
        nvl(dertd.report_level, 'B'),
        nvl(a.car_color_desc, ''),
        nvl(dcty.car_type, '')) as SKU_Desc,    --5年+B黑色比亚迪F0 2009款 爱国版 1.0L 舒适型
  concat(if(a.license_date is null, 3, case when year('{run_date}') - license_date between 0 and 1 then '1'
                                            when year('{run_date}') - license_date between 1 and 3 then '2'
                                            when year('{run_date}') - license_date between 3 and 5 then '3'
                                            else 4 end),
    (case when dertd.report_level = 'A' then 1
          when dertd.report_level = 'B' then 2
          when dertd.report_level = 'C' then 3
          when dertd.report_level = 'D' then 3
          when dertd.report_level = 'E' then 3
          else 2 end), --默认值
    if(nvl(a.car_color, 13) < 10, concat('0', nvl(a.car_color, 13)), nvl(a.car_color, 13)),
    a.padding_carid) as SKU,        --4201AAAAAA10752
    case when a.car_source_status = 3 then if(a.status_update_time = 0, null, from_unixtime(a.status_update_time, 'yyyy-MM-dd HH:mm:ss')) else null end as stop_sale_time, -- 停售时间
    case when a.car_source_status = 3 then if(a.status_update_time = 0, null, from_unixtime(a.status_update_time, 'yyyy-MM-dd HH:mm:ss'))
         when a.car_source_status in (1, 2, 5) then if(a.prepay_time = 0, null, from_unixtime(a.prepay_time, 'yyyy-MM-dd HH:mm:ss'))
         else null end as unshelf_time, -- 下架时间
    case when car_tag.qgg_tag is not null then car_tag.qgg_tag else 0 end as qgg_tag,  --全国购标签
    case when car_tag.pledge_create_time is null then 0 else 1 end as is_pledge,    -- 是否抵押车
    car_tag.pledge_create_time as pledge_time,     -- 成为抵押车时间
    cbro.violation_time,    -- 未处理违章时间
    wchr.created_at as document_lack_time,   -- 文件缺失时间
    base_price, -- 出售底价
    title, --车源标题
    case when car_tag.local_qgg_tag is not null then 1 else 0 end as is_local_qgg_car,--是否本地购车源
    case when car_tag.qgg_tag is not null and substr(car_tag.qgg_create_time,1,4) <> '1970' then car_tag.qgg_create_time
        end as qgg_car_time, --成为全国购车源时间
    case when coalesce(tail.star_level, tail_other.star_level) in (0, 1) then '尾部车系'
        when coalesce(tail.star_level, tail_other.star_level) = 2 then '腰部车系'
        when coalesce(tail.star_level, tail_other.star_level) = 3 then '头部车系'
        when coalesce(tail.star_level, tail_other.star_level) is null then '尾部车系'
        else '未知枚举车系' end as category_tag,
    che300.individual_price,                                            --个人交易价
    coalesce(car_tag.is_consign_can_sell, 0) as is_consign_can_sell,    --是否保卖可售
    coalesce(car_tag.is_b_qgg_can_sell, 0) as is_b_qgg_can_sell,        --是否b全国购可售
    coalesce(car_tag.is_consign_protect, 0) as is_consign_protect,      --是否保卖保护
    car_tag.first_c_qgg_tag_time as first_c_qgg_tag_time,  -- 首次标记C全国购可售标签时间
    car_tag.first_b_qgg_tag_time as first_b_qgg_tag_time,  -- 首次标记B全国购可售标签时间
    car_tag.first_consign_protect_tag_time as first_consign_protect_tag_time,  -- 首次标记保卖3天保护标签时间
    car_tag.first_consign_tag_time as first_consign_tag_time,  -- 首次标记保卖可售标签时间
    case when a.clue_source_type_code = '107054650010000' then 1 else 0 end as is_open_platform_clue,     --是否开放平台线索
    coalesce(ccse.list_hidden,0) as list_hidden,                                 --是否列表隐藏(0显示,1隐藏)
    coalesce(is_zhancang, 0) as is_zhancang,  -- 是否斩仓
    coalesce(national_price.nation_wide_price, 0) as nation_wide_price,  -- 全国购最新模型价
    a.clue_source_type_code, --车源来源渠道
    coalesce(car_tag.is_consign_joint_price, 0) is_consign_joint_price,
    case when is_high_seller_intention = 1 then 3
        when is_mid_seller_intention = 1 then 2
        when is_low_seller_intention = 1 then 1
        end as seller_intention, --车主售车意向 1低 2中 3高
    car_tag.del_qgg_tag as del_qgg_tag,             --全国购标签是否被删除
    car_tag.del_qgg_tag_time as del_qgg_tag_time,   --全国购标签最后被删除的时间
    car_tags.tags as valid_tags,                     --车源有效标签
    ccse.contactor,                                  --上架联系人
    a.phone_encrypt,                                 --车主联系方式
    coalesce(car_tag.is_consign_cancel, 0) as is_consign_cancel,   --是否保卖取消 1是 0否
    release.finished_time as relief_complete_time,                 --解抵押完成时间
    release.expected_relief_complete_time,                          --预计解抵押时间
    dcty.new_category1 as car_category,                              --车品类
    coalesce(car_tag.is_virtual_shelf, 0) as is_virtual_shelf,  -- 是否转B未由瓜子上架的虚拟上架 1是 0否
    coalesce(j.collect_clue_type, 0) as collect_clue_type,  -- 收车线索类型 1:开放平台线索 2:B端特殊线索(包含低价及百城关站) 3:4s线索 0:C1线索
    coalesce(store.open_platform_store_id,0) as open_platform_store_id, -- -- 开放平台车商ID
    concat('https://www.guazi.com/', misc_city.domain, '/', a.uni_code, 'x.htm') as car_url,
    case when membrane.clue_id is not null then 1 else 0 end is_membrane_clue, -- 是否撕膜车
    coalesce(store.maintain_bd_id, 0) as maintain_bd_id,  -- 开放平台车商管家id
    coalesce(shelve_level.cost_performance_level, 99) as shelve_level,  -- 上架等级
    coalesce(on_sale_level.cost_performance_level, 99) as on_sale_level,  -- 在售等级
    membrane.created_at as membrane_create_time,-- 撕膜车创建时间
    a.car_number_encrypt, -- 车牌号密文
    ccse.is_operation, -- 车辆性质，1：营运；0：非运营
    coalesce(car_tag.is_golden2,0) as is_golden2,  --是否金牌2.0车源 1:是(目前)  2:是(历史)  0:否
    case when task_tag.clue_id is null then 0 else 1 end as is_direct,  -- 是否直下工单 1直下工单 0非直下工单
    coalesce(store.is_golden2_dealer, 0) as is_golden2_dealer     --是否金牌2.0合作车商   1:是(目前)  2:是(历史)  0:否
from
(
    select  id,
            clue_id,
            car_source_status,
            create_time,
            title,
            price,
            base_price,
            minor_category_name,
            tag_name,
            car_id,
            license_date,
            license_month,
            road_haul,
            phone_encrypt,
            seller_description,
            transfer_num,
            vin_encrypt,
            prepay_time,
            deal_price,
            status_update_time,
            create_operator,
            car_year,
            city_id,
            district_id,
            trader,
            dealer,
            stop_sale_reason,
            clue_source_type,
            evaluator,
            source_level,
            auto_type,
            carriages,
            fuel_type,
            gearbox,
            emission_standard,
            car_color,
            guobie,
            seats,
            minor_category_id,
            tag_id,
            gender,
            special_audit,
            is_unique_car_source,
            platform,
            plate_city_id,
            car_owner_type,
            clue_source_type_code,
            evaluate_score,
            evaluate_level,
            pub_city_id,
            pub_district_id,
            created_at,
            sale_type,
            evaluate_editor,
            air_displacement,
            case when audit_year between 2000 and 2099 and audit_month between 1 and 12
                then last_day(concat(audit_year,'-',audit_month,'-01')) end as year_inspection_date,
            dt,
            dh,
            from_unixtime(create_time, 'yyyy-MM-dd') as dt1,
           -- 车颜色描述
           (case when car_color =1 then '黑色'
           when car_color =2 then '白色'
           when car_color =3 then '银灰色'
           when car_color =4 then '深灰色'
           when car_color =5 then '咖啡色'
           when car_color =6 then '红色'
           when car_color =7 then '蓝色'
           when car_color =8 then '绿色'
           when car_color =9 then '黄色'
           when car_color =10 then '橙色'
           when car_color =11 then '香槟色'
           when car_color =12 then '紫色'
           when car_color =13 then '多色'
           else '其他' end) as car_color_desc,
           uni_code,
           -- 被用来做SKU
          substring(concat('AAAAAAAAAAA',car_id),-11) as padding_carid,
          car_number_encrypt
    from guazi_dw_dwb.dwb_cars_car_source_day
    where dt = '{run_date}' and dh = '00'
        and city_id not in (344, 345)
) a

left join
(
    select
        id
        ,appointment_person
        , case when clue_status <> -1 and fail_reason in (67, 68, 69, 70, 71, 72) then 2
            when source_type_code = '107054650010000' then 1
            when car_clues_ca_s = 'sop_zijianxiansuo' and car_clues_ca_n = 'zjsjscxs' then 3
            -- when source_type_code = '205001797010000' then 4
            when car_clues_category = 19 then 5
            when car_clues_ca_s in ('sop_chesupai') then 1
            else 0 end as collect_clue_type -- 收车线索类型 1:开放平台线索 2:B端特殊线索(包含低价及百城关站) 3:4s线索 0:C1线索
    from guazi_dw_dwb.dwb_clues_vehicle_c2c_car_clues_day
    where dt = '{run_date}' and dh = '00'
) j
on a.clue_id = j.id

left join(  --车源修改日志表
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
        and source_key in ('bc_model_insert','che_insert', 'cars-info 上架') --涞源
    ) x
    where num = 1
) firp on a.clue_id = firp.clue_id

left join
(   --收车合同快照表
    select
       clue_id,
       audit_at     --审核时间
   from guazi_dw_dwb.dwb_contract_log_consignment_snapshot_day
   where dt = '{run_date}'
       and discard_at = 0 and city_id not in (344,345)
) ctl
on a.clue_id = ctl.clue_id
left join
(   --车源货物表
    select
        foreign_id as clue_id,
        goods_status,           --货物状态（1待入库、2在库、3待出库、4已出库）
        hostling_status         --整备状态（1待整备 5进行中 10整备完成）
    from guazi_dw_dwb.dwb_warehouse_car_goods_day
    where dt = '{run_date}'
        and dh = '00'
        and goods_type = 1      --货物类型(1保卖 2新车 3车商 4寄售 5保卖寄售 6快卖车)
) wcg
on a.clue_id = wcg.clue_id

left join
(   --车源车300数据缓存
    select clue_id
           ,dealer_buy_price                --车商收购价
           ,low_price                       --车况一般的个人交易价
           ,good_price                      --车况良好的个人交易价
           ,high_price                      --车况优秀的个人交易价
           ,individual_price                --个人交易价
    from guazi_dw_dwb.dwb_bm_statistic_car_source_che300_cache_day
    where dt ='{run_date}'
) che300
on a.clue_id = che300.clue_id
left join
(   -- 清单复检是否可过户
   select
        clue_id, is_transfer, created_at
    from
    (
        select
            clue_id, is_transfer, from_unixtime(create_time) created_at,
            row_number() over (partition by clue_id order by create_time desc) num
        from gzlc_real.fact_car_service_ccs_node_transfer
        WHERE dt > '2000-01-01' and node_id=1003
    ) x
    where num = 1
) wchr
on a.clue_id = wchr.clue_id
left join
(   -- 违章订单表
    select
        clue_id,
        status,
        violation_time
    from
    (
        select
             clue_id,
             status,
             violation_time,
             row_number() over (partition by clue_id order by violation_time desc) num
        from
        (
            select
                clue_id,
                cast(get_json_object(raw_response, '$\[0\].status') as int) as status,  -- 是否违章 -1未知 0未处理 1已处理
                cast(get_json_object(raw_response, '$\[0\].time') as string) as violation_time ---最新一次违章情况
            from guazi_dw_dwb.dwb_consign_break_rules_order_day
            where dt = '{run_date}' and dh = '00'
                and raw_response != '\[\]'
        ) x
    ) x
    where num = 1 and status is not null
) cbro
on a.clue_id = cbro.clue_id
left join
(   -- 交强险日期
    select
        clue_id, strong_insurance_full_date
        ,case when b.date_id is not null then 1 end as is_date
        ,a.list_hidden
        ,a.contactor
        ,a.is_operation
    from
    (
        select
            clue_id,
            case when strong_insurance_full_date > 0 then strong_insurance_full_date end as strong_insurance_full_date_in,    -- 在正常时间的范围内
            case when strong_insurance_full_date > 0 then concat(substr(strong_insurance_full_date, 1, 4), '-', substr(strong_insurance_full_date, 5, 2), '-', substr(strong_insurance_full_date, 7, 2)) end
                as strong_insurance_full_date
            ,list_hidden
            ,contactor
            ,is_operation
        from guazi_dw_dwb.dwb_cars_car_source_expand_day
        where dt = '{run_date}' and dh = '00'

    ) a
    left join
    (   -- 剔除非正常日期
        select
            cast(date_id as int) date_id
        from guazi_dw_dwd.dim_com_date_info_ymd
    ) b
    on a.strong_insurance_full_date_in = b.date_id
) ccse
on a.clue_id = ccse.clue_id
left join
(   -- 年检到期年月 -- 以月底作为到期年月日
    select
        clue_id, month_end_date as year_inspection_date,car_owner
    from
    (  --评估师报告记录信息表
        select
            clue_id,
            car_owner,
            audit_year as year,
            audit_month as month
        from guazi_dw_dwb.dwb_evaluates_vehicle_c2c_evaluate_day
        where dt = '{run_date}' and dh = '00'
    ) a
    left join
    (
        select
            cast(date_id / 10000 as int) as year, month, max(month_end_date) as month_end_date
        from guazi_dw_dwd.dim_com_date_info_ymd
        group by cast(date_id / 10000 as int), month
    ) b
    on a.year = b.year and a.month = b.month
) evce
on a.clue_id = evce.clue_id
left join (
    select car_id, nvl(concat(nvl(new_chexi, ''), ' ' , nvl(new_chexing, '')), '') as car_type,tag_id,new_category1,new_chexi
    from guazi_dw_dwd.dim_com_car_type_ymd
    where dt = '{run_date}'
) dcty
on a.car_id = dcty.car_id
left join (
    select clue_id, report_level --复检报告等级 abcd
    from ( -- 默认B
        select clue_id, if(length(report_level)=0, 'B', report_level) as report_level, row_number() over(partition by clue_id order by id desc) rank
        from guazi_dw_dwb.dwb_evaluates_recheck_task_day
        where dt = '{run_date}'
    ) x where x.rank = 1
) dertd
on a.clue_id = dertd.clue_id

left join
(
    select clue_id,tag,row_number() OVER (PARTITION BY clue_id ORDER BY id DESC) AS rank
    from guazi_dw_dwb.dwb_cars_car_source_tag_day
    where
    dt = '{run_date}' and tag_status=0 -----标签未作废
        and tag in (7,8,9,10,11,12,58,66)
) n on a.clue_id=n.clue_id and n.rank=1

left join (
    select
        clue_id
        ,max(case when tag = 53 then 1 end) as is_sys_source
        ,max(case when tag = 55 then 1 end ) as is_evaluator_source
        ,max(case when tag = 107 and tag_status = 0 then tag end) as zhancang_tag
        ,max(case when tag = 79 and tag_status = 0 then tag end) as recheck_tag
        ,max(case when tag = 143 and tag_status = 0 then 2 end ) as sale_type_tag
        ,max(case when tag = 187 and tag_status = 0 then tag end) as qgg_tag
        ,max(case when tag = 187 and tag_status = 0 then from_unixtime(create_time) end) as qgg_create_time
        ,max(case when tag = 187 and tag_status = -1 then tag end) as del_qgg_tag
        ,max(case when tag = 187 and tag_status = -1 then substr(updated_at, 1, 19) end) as del_qgg_tag_time
        ,max(case when tag = 270 and tag_status = 0 then tag end) as local_qgg_tag
        ,max(case when tag = 45 and tag_status = 0 then 1 end) as is_consign_can_sell
        ,max(case when tag = 142 and tag_status = 0 then 1 end) as is_b_qgg_can_sell
        ,max(case when tag = 99 and tag_status = 0 then 1 end) as is_consign_protect
        ,max(case when tag = 414 and tag_status = 0 then 1 end) as is_consign_joint_price
        ,max(case when tag = 396 and tag_status = 0 then 1 end) as is_high_seller_intention
        ,max(case when tag = 397 and tag_status = 0 then 1 end) as is_mid_seller_intention
        ,max(case when tag = 398 and tag_status = 0 then 1 end) as is_low_seller_intention
        ,max(case when tag = 390 then 1 end) as is_open_platform_clue
        ,max(case when tag = 49 and tag_status = 0 then substr(created_at, 1, 19) end) as pledge_create_time
        ,max(case when tag = 48 and tag_status = 0 then 1 end) as is_consign_cancel
        --------------------------------------------------------------------------
        -- 这里记录了标签生成的开始时间
        -- 这张表近似是一个log表，当标签无效时将tag_status update为-1，当再次打标签时，新增tag_status为0的记录
        --------------------------------------------------------------------------
        ,min(case when tag = 187 then substr(created_at, 1, 19) end) as first_c_qgg_tag_time
        ,min(case when tag = 142 then substr(created_at, 1, 19) end) as first_b_qgg_tag_time
        ,min(case when tag = 99 then substr(created_at, 1, 19) end) as first_consign_protect_tag_time
        ,min(case when tag = 45 then substr(created_at, 1, 19) end) as first_consign_tag_time
        ,max(case when tag in (107, 139, 147, 311, 312) then 1 end) as is_zhancang
        , max(case when tag = 444 and tag_status = 0 then 1 end) as is_virtual_shelf
        , max(case when tag = 666 and tag_status = 0 then 1 end) as btob_tag
        , max(case when tag = 556 and tag_status = 0 then 1 when tag = 556 and tag_status = -1 then 2 end) as is_golden2
        from guazi_dw_dwb.dwb_cars_car_source_tag_day
    where dt = '{run_date}' and dh = '00'
        and tag in (7,8,9,10,11,12,45,48,49,53,55,58,66,79,99,107,143,142,187,270,390,396,397,398,414, 444, 666,556)
    group by clue_id
) car_tag on a.clue_id = car_tag.clue_id
left join
(   -- 按城市、车型、车颜色匹配一次
    select
        city_id, car_id, car_color, max(star_level) as star_level
    from guazi_dw_dwb.dwb_consign_price_adjust_blacklist_day
    where dt = '{run_date}'
        and city_id <> 0
    group by 1, 2, 3
) tail
on a.city_id = tail.city_id
    and a.car_id = tail.car_id
    and a.car_color = tail.car_color
left join
(   -- 去除城市按车型、车颜色匹配一次
    select
        car_id, car_color, star_level
    from
    (
        select
            car_id, car_color, star_level,
            row_number() over(partition by car_id, car_color order by id) num
        from guazi_dw_dwb.dwb_consign_price_adjust_blacklist_day
        where dt = '{run_date}'
            and city_id = 0
    ) x
    where num = 1
) tail_other
on a.car_id = tail_other.car_id
    and a.car_color = tail_other.car_color
left join
(
    select
        coalesce(t1.clue_id, t2.clue_id) as clue_id,
        coalesce(t1.consign_price, t2.nation_wide_price) as nation_wide_price
    from
    (   --保卖模型数据
        select
            clue_id, consign_price  --保卖价
        from guazi_dw_dwb.dwb_consign_consign_model_day
        where dt = '{run_date}'
            and consign_price <> 10000000 and consign_price >= 25000
    ) t1
    full join
    (   --全国购模型请求日志
        select
            clue_id, nation_wide_price  --全国购价格
        from
        (
            select
                clue_id, nation_wide_price,
                row_number() over (partition by clue_id order by created_at desc) num
            from guazi_dw_dwb.dwb_consign_nation_wide_model_log_day
            where dt = '{run_date}'
        ) x
        where num = 1
    ) t2
    on t1.clue_id = t2.clue_id
) national_price
on a.clue_id = national_price.clue_id
left join
(   --车源标签表
    select clue_id
        ,concat_ws(',',collect_set(cast(tag as string))) as tags
    from guazi_dw_dwb.dwb_cars_car_source_tag_day
    where dt = '{run_date}'
        and tag_status = 0
    group by clue_id
)car_tags
on a.clue_id = car_tags.clue_id
left join
(
    select clue_id
        ,max(case when finished_time > 0 then from_unixtime(finished_time,'yyyy-MM-dd HH:mm:ss') end) as finished_time         --解押完成时间
        ,max(case when expected_relief_complete_time > 0
            then from_unixtime(expected_relief_complete_time,'yyyy-MM-dd HH:mm:ss') end) as expected_relief_complete_time --预计解抵押时间
    from guazi_dw_dwb.dwb_consign_release_pledge_task_day
    where dt = '{run_date}'
    group by clue_id
)release on a.clue_id = release.clue_id
left join
(
    select
        a.id as clue_id
        , b.id as open_platform_store_id               -- 开放平台车商ID
        , case when a.store_id > 0 then c.staff_id
             end as maintain_bd_id  -- 开放平台车商管家id
        ,case when f.is_golden2_dealer = 1 and f.golden2_delete_flag = 0 and f.channel_dealer is null then 1
              when f.is_golden2_dealer = 1 and f.golden2_delete_flag = 1 and f.channel_dealer is null then 2
              else 0 end as is_golden2_dealer  --是否金牌2.0合作车商   1:是(目前)  2:是(历史)  0:否

    from
    (
        select
            id
            ,store_id
        from guazi_dw_dwb.dwb_evaluate_task_evaluate_task_day
        where dt = '{run_date}'
            and city_id not in (344,345,404,405,406,407)
            and evaluator <> 114137
    )a
    left join
    (   -- 车商配置表
        select
            id
        from guazi_dw_dwb.dwb_dealer_dealer_day
        where dt = '{run_date}'
            and source_type = 1
    )b
    on a.store_id = b.id
    left join
    (
        select
            dealer_id, staff_id
        from guazi_dw_dwb.dwb_dealer_dealer_staff_ref_day
        where dt = '{run_date}'
            and staff_type = 1 and delete_flag = 0
    ) c
    on a.store_id = c.dealer_id
    left join (
        select a.dealer_id,
            max(case when tag_id = 13 then 1 end) as is_golden2_dealer,
            max(case when tag_id = 13 then delete_flag end) as golden2_delete_flag,
            max(case when tag_id = 16 then 1 end) as channel_dealer,
            max(case when tag_id = 16 then delete_flag end) as channel_delete_flag
        from guazi_dw_dwb.dwb_dealer_dealer_tag_ref_day a
        where dt = '{run_date}' and tag_id in (13,16)
        group by a.dealer_id
    ) f on a.store_id = f.dealer_id

)store
on a.clue_id = store.clue_id
left join
(
    select
        city_id, domain
    from guazi_dw_dwb.dwb_misc_misc_city_day
    where dt = '{run_date}'
) misc_city
on a.city_id = misc_city.city_id
left join
(select
clue_id,created_at
,row_number() over (partition by clue_id order by created_at desc,id desc) num
from guazi_dw_dwb.dwb_sale_mall_remove_membrane_clue_day
where dt = '{run_date}'
and is_delete=0)membrane
on a.clue_id=membrane.clue_id
    and membrane.num=1
left join
(
    select
        clue_id, cost_performance_level
    from
    (
        select
            clue_id, cost_performance_level,
            row_number() over(partition by clue_id order by id) num
        from guazi_dw_dwb.dwb_c2b_dealer_car_product_cost_performance_level_log_day
        where dt = '{run_date}'
            and node = 1
    ) x
    where num = 1
) shelve_level
on a.clue_id = shelve_level.clue_id
left join
(
    select
        clue_id, cost_performance_level
    from
    (
        select
            clue_id, cost_performance_level,
            row_number() over(partition by clue_id order by id desc) num
        from guazi_dw_dwb.dwb_c2b_dealer_car_product_cost_performance_level_log_day
        where dt = '{run_date}'
    ) x
    where num = 1
) on_sale_level
on a.clue_id = on_sale_level.clue_id
left join
(
    select
        clue_id
    from guazi_dw_dwb.dwb_evaluate_task_task_tag_day
    where dt = '{run_date}' and label_id = 10 and is_del = 0
    group by clue_id
) task_tag on a.clue_id= task_tag.clue_id
where car_tag.btob_tag is null


