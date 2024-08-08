--INSERT `peya-food-and-groceries.automated_tables_reports.fact_sessions_funnel_tribe_agg` NO USAR, ESTA ES LA QAT ORIGINAL


WITH raw AS (
    SELECT
            date
        ,   client_id
        ,   session_id
        ,   global_entity_id
        ,   platform
        ,   app_version
        ,   area_id
        ,   city_id
        ,   user_segment_income
        ,   user_segment_lifecycle
        ,   user_segment_behaviour
        ,   user_segment_qc
        ,   is_plus
        ,   Tribe
        ,   businessType
        ,   shop_list
        ,   shop_details
        ,   product_clicked
        ,   add_to_cart
        ,   cart_clicked
        ,   checkout_loaded
        ,   transaction
        ,   visit_type
        ,   country
        ,   session_concat
        ,   MAX(case when shop_list =1 then session_concat ELSE NULL END)         as  shop_list_dummy
        ,   MAX(case when shop_details =1 then session_concat ELSE NULL END)      as shop_details_dummy
        ,   MAX(case when product_clicked =1 then session_concat ELSE NULL END)   as product_clicked_dummy
        ,   MAX(case when add_to_cart =1 then session_concat ELSE NULL END)       as add_to_cart_dummy
        ,   MAX(case when cart_clicked =1 then session_concat ELSE NULL END)      as cart_clicked_dummy
        ,   MAX(case when checkout_loaded =1 then session_concat ELSE NULL END)   as checkout_loaded_dummy
        ,   MAX(case when transaction =1 then session_concat ELSE NULL END)       as transaction_dummy
        ,   MAX(case when shop_list =1 and transaction=1 then session_concat ELSE NULL END)    as transaction_CVR2
        ,   MAX(case when shop_details =1 and transaction=1 then session_concat ELSE NULL END) as transaction_CVR3
        ,   MAX(case when shop_list =1 and shop_details=1 then session_concat ELSE NULL END)       as shop_details_mCVR2
        ,   MAX(case when shop_details =1 and checkout_loaded=1 then session_concat ELSE NULL END) as checkout_loaded_mCVR3
        ,   MAX(case when shop_details =1 and add_to_cart=1 then session_concat ELSE NULL END)     as add_to_cart_mCVR3a
        ,   MAX(case when add_to_cart =1 and checkout_loaded=1 then session_concat ELSE NULL END)  as checkout_loaded_mCVR3b
        ,   MAX(case when checkout_loaded =1 and transaction=1 then session_concat ELSE NULL END)  as transaction_mCVR4
        ,   MAX(case when cart_clicked =1 and checkout_loaded=1 then session_concat ELSE NULL END)  as checkout_loaded_mCVR3c
    FROM (
        SELECT 
                f.partition_date as date
            ,   f.client_id
            ,   f.session_id
            ,   CAST(f.global_entity_id as STRING) as global_entity_id
            ,   f.platform
            ,   s.app_version
            ,   s.area_id
            ,   f.city.city_id
            ,   u.user_segment_income
            ,   u.user_segment_lifecycle
            ,   u.user_segment_behaviour
            ,   u.user_segment_qc
            ,   du.is_plus
            ,   f.Tribe
            ,   f.businessType       
            ,   f.shop_list_dummy         as shop_list
            ,   f.shop_details_dummy      as shop_details
            ,   f.product_clicked_dummy   as product_clicked
            ,   f.add_to_cart_dummy       as add_to_cart
            ,   f.cart_clicked_dummy      as cart_clicked
            ,   f.checkout_loaded_dummy   as checkout_loaded
            ,   f.transaction_dummy       as transaction
            ,   CAST(CONCAT(f.session_id, f.client_id, f.global_entity_id, f.platform, f.partition_date) as STRING) as session_concat
            ,   MAX(f.visit_type) as visit_type
            ,   MAX(f.country) as country          
        FROM `peya-bi-tools-pro.il_sessions.perseus_fact_sessions_funnel_by_verticals` f
        LEFT JOIN (
                    SELECT  s.session_id
                            ,MAX(s.app_version) app_version
                            ,MAX(s.area.area_id) area_id
                    FROM `peya-bi-tools-pro.il_sessions.fact_perseus_sessions` s
                    WHERE s.partition_date >= DATE_ADD(CURRENT_DATE(), INTERVAL -4 DAY)--partition_date >= DATE_ADD('{{ next_ds }}', INTERVAL -2 DAY)
                    GROUP BY 1
                    )s ON f.session_id = s.session_id
        LEFT JOIN (
                    SELECT user_id
                        ,user_segment_income
                        ,user_segment_lifecycle
                        ,user_segment_behaviour
                        ,user_segment_qc
                        ,from_date
                        ,CASE WHEN to_date IS NULL THEN CURRENT_DATE() ELSE to_date END AS to_date
                    FROM `peya-bi-tools-pro.il_core.dim_user_segmentation` 
                    --WHERE to_date IS NULL --Ultima segmentacion del usuario
                    ) u ON SAFE_CAST(f.user_id AS INT64) = u.user_id
                        AND f.partition_date BETWEEN u.from_date AND u.to_date
        LEFT JOIN `peya-bi-tools-pro.il_core.dim_user` du ON f.user_id = CAST(du.user_id AS STRING)
        WHERE partition_date >= DATE_ADD(CURRENT_DATE(), INTERVAL -4 DAY)--partition_date >= DATE_ADD('{{ next_ds }}', INTERVAL -2 DAY)
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
    )
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
)
SELECT 
        date
    ,   platform
    ,   app_version
    ,   area_id
    ,   city_id
    ,   user_segment_income
    ,   user_segment_lifecycle
    ,   user_segment_behaviour
    ,   user_segment_qc
    ,   is_plus
    ,   Tribe
    ,   COUNT(DISTINCT session_concat) as sessions
    ,   COUNT(DISTINCT 
            (CASE WHEN GREATEST(shop_list
                                ,shop_details
                                ,product_clicked
                                ,add_to_cart
                                ,cart_clicked
                                ,checkout_loaded
                                ,transaction) = 1 THEN session_concat ELSE NULL END))    as sessions_vertical
    ,   COUNT(DISTINCT shop_list_dummy)         as shop_list_dummy
    ,   COUNT(DISTINCT shop_details_dummy)      as shop_details_dummy
    ,   COUNT(DISTINCT product_clicked_dummy)   as product_clicked_dummy
    ,   COUNT(DISTINCT add_to_cart_dummy)       as add_to_cart_dummy
    ,   COUNT(DISTINCT cart_clicked_dummy)      as cart_clicked_dummy
    ,   COUNT(DISTINCT checkout_loaded_dummy)   as checkout_loaded_dummy
    ,   COUNT(DISTINCT transaction_dummy)       as transaction_dummy
    ,   COUNT(DISTINCT transaction_CVR2) as transaction_CVR2
    ,   COUNT(DISTINCT transaction_CVR3) as transaction_CVR3
    ,   COUNT(DISTINCT shop_details_mCVR2)     as shop_details_mCVR2
    ,   COUNT(DISTINCT checkout_loaded_mCVR3)  as checkout_loaded_mCVR3
    ,   COUNT(DISTINCT add_to_cart_mCVR3a)     as add_to_cart_mCVR3a
    ,   COUNT(DISTINCT checkout_loaded_mCVR3b) as checkout_loaded_mCVR3b
    ,   COUNT(DISTINCT transaction_mCVR4)      as transaction_mCVR4
    ,   COUNT(DISTINCT checkout_loaded_mCVR3b) as checkout_loaded_mCVR3c
FROM raw 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11