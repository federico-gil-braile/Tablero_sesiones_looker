CREATE TABLE `peya-food-and-groceries.user_federico_gil.check_sessions_funnel_tribe_and_vertical` AS

SELECT 
fs.partition_date AS date
,   fs.client_id
,   fs.session_id
,   CAST(fs.global_entity_id AS STRING) AS global_entity_id
,   fs.platform
,   fs.app_browser
,   fs.user_id
,   u.lifecycle_stage
,   u.is_plus
,   i.income
,   i.segment
,   fs.Tribe
,   fs.businessType        
,   fs.shop_list_dummy         AS shop_list
,   fs.shop_details_dummy      AS shop_details
,   fs.product_clicked_dummy   AS product_clicked
,   fs.add_to_cart_dummy       AS add_to_cart
,   fs.cart_clicked_dummy      AS cart_clicked
,   fs.checkout_loaded_dummy   AS checkout_loaded
,   fs.transaction_dummy       AS transaction
,   CAST(CONCAT(fs.session_id, fs.client_id, fs.global_entity_id, fs.platform, fs.partition_date) AS STRING) AS session_concat
,   MAX(fs.visit_type) AS visit_type
,   MAX(fs.country) AS country     
,   MAX(fs.city.city_name) AS city_name
FROM `peya-bi-tools-pro.il_sessions.perseus_fact_sessions_funnel_by_verticals` fs
LEFT JOIN `peya-bi-tools-pro.il_core.dim_user` u ON fs.user_id = CAST(u.user_id AS STRING)
LEFT JOIN `peya-bi-tools-pro.il_growth.user_income` i ON fs.user_id = CAST(i.user_id AS STRING)
WHERE fs.partition_date >= DATE_ADD(CURRENT_DATE(), INTERVAL -22 DAY)--partition_date >= DATE_ADD('{{ next_ds }}', INTERVAL -2 DAY)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
