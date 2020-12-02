insert overwrite table guazi_dw_dwd.dim_consign_car_source_ymd partition (dt='{run_date}') --严选车源表
        select
          wcg.id,
          wcg.clue_id,
          wcg.city_id,
          wcg.district_id,
          wcg.car_park_id,
          wcg.car_park_detail_id,
          wcg.brand_id,
          wcg.tag_id,
          wcg.car_id,
          wcg.goods_type,
          wcg.goods_type_desc,
          wcg.goods_status,
          wcg.goods_status_desc,
          wcg.key_status,
          wcg.key_status_desc,
          mc.brand_desc,
          mc.new_tag_desc,
          mc.new_car_desc,
          wcg.car_number,
          wcg.entry_time,
          wcg.delivery_receiver,
          wcg.delivery_appoint_time,
          wcg.delivery_time,
          wcg.inventory_time,
          wcg.key_applicant,
          wcg.key_apply_time,
          wcg.delivery_appoint_address,
          wcg.delivery_pending_time,
          wcg.entry_pending_time,
          wcg.tags,
          wcg.deprice,
          wcg.deprice_desc,
          wcg.print_card,
          wcg.print_card_desc,
          wcg.not_entry_reason,
          wcg.not_entry_reason_desc,
          wcg.goods_child_status,
          wcg.goods_child_status_desc,
          wcg.hostling_status,
          wcg.hostling_status_desc,
          nvl(ukc.is_use_key_cabinet, 0) as is_use_key_cabinet,
          hosso.equipment_status,
          ims.image_status,
          nvl(gps.have_gps, 0) as have_gps
          ,if(unix_timestamp(wcg.created_at) > 0, substr(wcg.created_at, 1, 19), null) as created_at
          ,if(unix_timestamp(wlcgl.first_entry_time) > 0, substr(wlcgl.first_entry_time, 1, 19), null) as first_entry_time
          ,case when kt.car_source_status=0 and kt.platform in (1, 3) and ctl.contract_audit_time >0 and kt.city_id not in (344,345) and kt.pub_city_id not in (344, 345)
         and wcg.goods_status in (1,2,3) and (wcg.hostling_status =10 or kt.city_id=241) then 1 else 0 end consign_can_sell
         ,ak.car_park_number
         ,null is_key_valid --钥匙是否可用
         ,quota_car --是否背户车 0-否 1-是（用于判断是否本地过户车源）
         ,is_material_lack --入库时三证是否齐全 1是0否
         ,case when tr.clue_id is not null then 1 else 0 end is_pledge  ---收车合同签约的时候是否为抵押车
         ,coalesce(wcp2.car_park_type,-1) car_park_type --0 严选车场,1 寄售车场,2 异地流转车场,3 车速拍车场,4 城市发展部车场,5 交付中心车场,6 瓜子二手车合伙人门店
        from
        (
          select
            id,
            foreign_id as clue_id,
            goods_type,
            case when goods_type = 1 then '保卖'
              when goods_type = 2 then '新车'
              when goods_type = 3 then '车商'
              when goods_type = 4 then '寄售'
              else '' end as goods_type_desc,
            goods_status,
            case when goods_status = 0 then '待入库'
              when goods_status = 1 then '已入库'
              when goods_status = 2 then '预约出库'
              when goods_status = 3 then '待出库'
              when goods_status = 4 then '已出库'
              when goods_status = 5 then '暂存'
              else '' end as goods_status_desc,
            city_id,
            district_id,
            car_park_id,
            car_park_detail_id,
            key_status,
            case when key_status = 0 then '空箱未存储'
              when key_status = 1 then '入库待存'
              when key_status = 2 then '在库'
              when key_status = 3 then '看车借出待取'
              when key_status = 4 then '看车借出已取'
              when key_status = 5 then '看车入库待存'
              when key_status = 6 then '盘点中'
              when key_status = 7 then '出库待取'
              when key_status = 8 then '车检待取'
              when key_status = 9 then '车检已取'
              when key_status = 10 then '车检代存'
              when key_status = 11 then '复检待取'
              when key_status = 12 then '复检已取'
              when key_status = 13 then '复检待存'
              else '' end as key_status_desc,
            brand_id,
            tag_id,
            car_id,
            car_number,
            if(entry_time > 0, from_unixtime(entry_time), null) as entry_time,
            delivery_receiver,
            if(delivery_appoint_time > 0, from_unixtime(delivery_appoint_time), null) as delivery_appoint_time,
            if(delivery_time > 0, from_unixtime(delivery_time), null) as delivery_time,
            if(inventory_time > 0, from_unixtime(inventory_time), null) as inventory_time,
            key_applicant,
            if(key_apply_time > 0, from_unixtime(key_apply_time), null) as key_apply_time,
            delivery_appoint_address,
            if(delivery_pending_time > 0, from_unixtime(delivery_pending_time), null) as delivery_pending_time,
            if(entry_pending_time > 0, from_unixtime(entry_pending_time), null) as entry_pending_time,
            tags,
            deprice,
            case when deprice = 1 then '降价'
              else '未降价' end as deprice_desc,
            print_card,
            case when print_card = 1 then '已打印'
              else '未打印' end as print_card_desc,
            not_entry_reason,
            --未入库原因（consign库的car_tag中tag_source=2，tag_status=0的元数据）,用户可配置，需定期更新
            case when not_entry_reason = 85 then '复检有争议'
              when not_entry_reason = 86 then '资料不齐'
              when not_entry_reason = 87 then '需退车'
              when not_entry_reason =88 then '未及时操作入库'
              else '未知' end as not_entry_reason_desc,
            goods_child_status,
            case when goods_child_status = 11 then '复检离库'
              when goods_child_status = 12 then '整备离库'
              when goods_child_status = 13 then '试驾离库'
              when goods_child_status = 14 then '外检离库'
              else '未知' end as goods_child_status_desc,
            hostling_status,
            case when hostling_status = 0 then '无整备信息'
                when hostling_status = 1 then '待整备'
                when hostling_status = 5 then '进行中'
                when hostling_status = 10 then '整备完成'
                when hostling_status = 15 then '整备待入库'
                when hostling_status = 20 then '待客户验收'
                when hostling_status = 25 then '整备待出库'
            else '未知' end as hostling_status_desc
            ,created_at
            ,quota_car
            ,row_number() over( partition by foreign_id order by id desc) as rn
          from guazi_dw_dwb.dwb_warehouse_car_goods_day
          where dt = '{run_date}'
            and city_id not in (344, 345)
        ) wcg
        left join
        (
          select
            car_id,
            pinpai as brand_desc, --'品牌'
            new_chexi as new_tag_desc, --'车系'
            new_chexing as new_car_desc --'车型'
          from guazi_dw_dwd.dim_com_car_type_ymd
          where dt = '{run_date}'
        ) mc on wcg.car_id = mc.car_id

        --is_use_key_cabinet 是否使用钥匙柜
        left join(
          select
            a4.foreign_id as clue_id,
            1 as is_use_key_cabinet
          from(
              select
                clue_id,
                device_id
              from guazi_dw_dwb.dwb_warehouse_key_box_day
              where dt = '{run_date}'
              and box_type = 0
              and store_status in (1, 2, 5, 9, 12)
          ) a2
          left join(
              select
                foreign_id
              from guazi_dw_dwb.dwb_warehouse_car_goods_day
              where dt = '{run_date}'
              and goods_type = 1   -- 货物类型(1保卖 2新车 3车商)
              and goods_status in (1,2,3) -- 货物状态（0待入库、1在库、2预约出库、3待出库、4已出库）
              and city_id not in (344,345)
          ) a4 on a2.clue_id = a4.foreign_id
          where a4.foreign_id is not null
        ) ukc on wcg.clue_id = ukc.clue_id

        --equipment_status, --装备完成状态 1：已完成 0：未完成
        left join
        (
          select
            a.clue_id,
            case when a.count >= 3 then 1 else 0 end equipment_status
          from
          (
              select
                clue_id,
                count(1) as count
              from guazi_dw_dwb.dwb_hostling_order_self_support_orders_day
              where dt = '{run_date}'
              and order_status in (25, 60, 80)
              group by clue_id
          ) a
        ) hosso on wcg.clue_id = hosso.clue_id
        --image_status int, --照片审核状态 1 审核通过 2: 审核驳回
        left join
        (
          select
            a.clue_id,
            a.task_status as image_status --task_status:1 审核通过 2: 审核驳回
          from
          (
            select
              id,
              clue_id,
              task_status
            from guazi_dw_dwb.dwb_consign_optimize_image_task_day
            where dt = '{run_date}'
          ) a
          join
          (
            select
              max(id) as id,
              clue_id
            from guazi_dw_dwb.dwb_consign_optimize_image_task_day
            where dt = '{run_date}'
            group by clue_id
          )b on a.id = b.id and a.clue_id = b.clue_id
        ) ims on wcg.clue_id = ims.clue_id
        --have_gps int --是否有GPS 1：有 0：没有
        left join
        (
          select
            clue_id,
            1 as have_gps
          from guazi_dw_dwb.dwb_warehouse_car_gps_day
          where dt = '{run_date}'
            and bind_status in (1, 2, 3)
          group by clue_id
        ) gps on wcg.clue_id = gps.clue_id
        left join
        (
            select *
            from guazi_dw_dwb.dwb_warehouse_car_park_day
            where dt='{run_date}'
        ) wcp2 on wcg.car_park_id=wcp2.id
        left join (
            select goods_id
                ,min(created_at) as first_entry_time
            from guazi_dw_dwb.dwb_warehouse_log_car_goods_log_day
            where dt = '{run_date}'
            and operate_type in (1,17,19)
            group by goods_id
        ) wlcgl on wcg.id = wlcgl.goods_id
        left join
    (
        select clue_id
            ,case when contract_audit_time = 1 then 0 else contract_audit_time end as contract_audit_time
        from (
            select
                clue_id,
                unix_timestamp(audited_at, 'yyyy-MM-dd HH:mm:ss') as contract_audit_time,
                row_number() over(partition by clue_id order by updated_at desc) num
            from guazi_dw_dwb.dwb_consign_bs_contract_day
            where dt = '{run_date}'
        ) ctl
        where num = 1
    ) ctl on wcg.clue_id = ctl.clue_id
     left join
    (
    select
        clue_id,car_source_status, platform,pub_city_id,city_id
    from guazi_dw_dwb.dwb_cars_car_source_day
    where dt = '{run_date}' and dh = '00'
    ) kt
    on wcg.clue_id = kt.clue_id
    left join ( --可视化车位（包含临时车位+签约车位）
            select
              clue_id,
              max(car_park_number) car_park_number
            from guazi_dw_dwb.dwb_warehouse_car_space_relation_visual_day
            where dt = '{run_date}' and dh = '00'
              and park_area = 0
              and is_deleted = 0
              group by clue_id
     )ak on wcg.clue_id = ak.clue_id
     left join
     (select
      clue_id,if(stuff_list like '%"code":1,"hasStuff":1%'
        and stuff_list like '%"code":2,"hasStuff":1%'
        and stuff_list like '%"code":4,"hasStuff":1%'
        ,1,0) is_material_lack
        ,operation_time  -- "提交时间"
        from
        (
        select
          id,
          clue_id,
          stuff_list,FROM_UNIXTIME(operation_time)operation_time
          ,rank() over(partition by clue_id order by id asc) as rnk
        from guazi_dw_dwb.dwb_sale_car_service_clean_car_source_record_day
        where dt='{run_date}' and dh='00'
        and node_id = 1003    -- 1001整备环节，1002收车环节，1003场地环节
        )jk
        where rnk=1
     )pu on wcg.clue_id = pu.clue_id
     left join
     (SELECT clue_id
      FROM guazi_dw_dwb.dwb_consign_pledge_info_day
      WHERE dt='{run_date}'  AND dh = '00'
      group by clue_id
      )tr on wcg.clue_id = tr.clue_id
        where wcg.rn = 1