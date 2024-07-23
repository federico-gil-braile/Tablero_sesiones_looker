--INSERT `peya-food-and-groceries.automated_tables_reports.fact_sessions_funnel_all_agg`

WITH raw AS (
    SELECT 
            date
        ,   client_id
        ,   session_id
        ,   platform
        ,   session_sk
        ,   app_browser
        ,   session_concat
        ,   shop_list
        ,   shop_details
        ,   checkout_loaded
        ,   transaction
        ,   visit_type
        ,   country
        ,   sessionId
        ,   case when shop_list then session_concat ELSE NULL END         as  shop_list_dummy
        ,   case when shop_details then session_concat ELSE NULL END      as shop_details_dummy
        ,   case when checkout_loaded then session_concat ELSE NULL END   as checkout_loaded_dummy
        ,   case when transaction is not null then session_concat ELSE NULL END   as transaction_dummy
        ,   case when shop_list and transaction is not null  then session_concat ELSE NULL END    as transaction_CVR2
        ,   case when shop_details and transaction is not null  then session_concat ELSE NULL END as transaction_CVR3
        ,   case when shop_list and shop_details then session_concat ELSE NULL END     as shop_details_mCVR2
        ,   case when shop_details and checkout_loaded then session_concat ELSE NULL END      as checkout_loaded_mCVR3
        ,   case when checkout_loaded and  transaction is not null then session_concat ELSE NULL END  as transaction_mCVR4
        ,   is_transaction -- AGREGADO 
        ,   CASE WHEN home_loaded = true THEN session_concat end as has_home_loaded_mCVR1 -- AGREGADO
        ,   CASE WHEN home_loaded = TRUE AND shop_list = TRUE THEN session_concat END has_home_and_shop_list_loaded_mCVR1 -- AGREGADO 
        ,   CASE WHEN is_transaction = TRUE THEN session_concat end as transaction_CVR1 -- AGREGADO 
    FROM (
        SELECT 
                partition_date as date
            ,   client_id
            ,   session_id
            ,   CAST(ps.global_entity_id as string) as global_entity_id
            ,   platform
            ,   session_sk
            ,   app_version as app_browser
            ,   shop_list_loaded        as shop_list
            ,   shop_details_loaded     as shop_details
            ,   checkout_loaded         as checkout_loaded
            ,   case when totals.orders > 0 then CAST(totals.orders as string) else NULL END as transaction
            ,   session_sk as sessionId
            ,   CONCAT(session_id, client_id, ps.global_entity_id, platform, partition_date) as session_concat
            ,   is_transaction -- AGREGADO
            ,   home_loaded -- AGREGADO
            ,   MAX(visit_type) as visit_type
            ,   MAX(country.country_name) as country
        FROM `peya-bi-tools-pro.il_sessions.fact_perseus_sessions` ps 
        -- AGREGADO 
        INNER JOIN `peya-bi-tools-pro.il_core.dim_country` AS dc 
                ON  (ps.country.country_id = dc.country_id 
                        AND dc.active) 
        WHERE DATE(partition_date) >= DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY)--DATE(partition_date) >= DATE_ADD('{{ next_ds }}', INTERVAL -2 DAY)
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
        )
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
    )
SELECT  date
    ,   platform
    ,   country 
    ,   visit_type
    ,   app_browser
    ,   COUNT(DISTINCT session_sk) as sessions
    ,   COUNT(DISTINCT session_sk) as sessions_vertical
    ,   COUNT(DISTINCT CASE WHEN is_transaction = true then session_sk END) as sessions_with_transactions -- AGREGADO
    ,   COUNT(DISTINCT shop_list_dummy)         as shop_list_dummy
    ,   COUNT(DISTINCT shop_details_dummy)      as shop_details_dummy
    ,   COUNT(DISTINCT checkout_loaded_dummy)   as checkout_loaded_dummy
    ,   COUNT(DISTINCT transaction_dummy)       as transaction_dummy
    ,    COUNT(DISTINCT transaction_CVR1) as transaction_CVR1 -- AGREGADO
    ,   COUNT(DISTINCT transaction_CVR2) as transaction_CVR2
    ,   COUNT(DISTINCT transaction_CVR3) as transaction_CVR3
    ,   COUNT(DISTINCT has_home_loaded_mCVR1) as has_home_loaded_mCVR1 -- AGREGADO
    ,   COUNT(DISTINCT has_home_and_shop_list_loaded_mCVR1) as has_home_and_shop_list_loaded_mCVR1 -- AGREGADO
    ,   COUNT(DISTINCT shop_details_mCVR2)     as shop_details_mCVR2
    ,   COUNT(DISTINCT checkout_loaded_mCVR3)  as checkout_loaded_mCVR3
    ,   COUNT(DISTINCT transaction_mCVR4)      as transaction_mCVR4
FROM raw 
GROUP BY 1,2,3,4,5