db : 全量，离线
evaluate_config :ditect_config_redline  	1
				:detect_redline_related 	1

xinche_after_base:table_log 	3023202 -> 3028113

xinche_mall_card:operation_log 	35 -> 35 1
				:operation_log_detail

全量，实时
clue:customer_manager_task->ods_clues_customer_manager_task
	:clue_customer_manager


流量-TOPN——新车绝对值
首付指标变更为 106548



11.24

db : 全量，离线
wechat_work:user_customer_tag -> dwb_wechat_work_user_customer_tag_day	1
		   :corp_tags -> dwb_wechat_work_corp_tags_day		1

clues:clue_customer_info -> dwb_clues_clue_customer_info_day	1



11.25

未完成
spc_application,
spc_administrator,
spc_department,
spc_position,
spc_user,
spc_user_department_position,
spc_resource,
spc_resource_item,
spc_permission,
spc_role,
spc_permission_resource,
spc_role_permission,
spc_role_user_scope


sale_car_service:delivery_order->guazi_dw_dwb.dwb_sale_car_service_delivery_order_day_online