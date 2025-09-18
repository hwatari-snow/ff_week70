-- セッション設定
ALTER SESSION SET query_tag = 'ff2_challenge';
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- 共通パターンなしのランダムクエリ
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

SELECT COUNT(*) 
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER 
WHERE C_NATIONKEY = 15; --モロッコでした

SELECT C_NAME 
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER 
WHERE C_PHONE = '19-144-468-5416';

-- 第1セット: C_MKTSEGMENTによる単一条件でのカスタマー全列取得パターン
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER WHERE C_MKTSEGMENT = 'BUILDING';
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER WHERE C_MKTSEGMENT = 'AUTOMOBILE';
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER WHERE C_MKTSEGMENT = 'MACHINERY';
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER WHERE C_MKTSEGMENT = 'HOUSEHOLD';

-- 第2セット: C_MKTSEGMENTとC_NATIONKEYによる2条件でのC_NAME, C_NATIONKEY取得パターン
SELECT C_NAME, C_NATIONKEY 
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER 
WHERE C_MKTSEGMENT = 'BUILDING' AND C_NATIONKEY = 21; --ベトナムだった

SELECT C_NAME, C_NATIONKEY 
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER 
WHERE C_MKTSEGMENT = 'MACHINERY' AND C_NATIONKEY = 9; --インドでした


-- ===========================================
-- パフォーマンス・コスト分析クエリ集
-- ===========================================


-- 1. 基本的なクエリ実行履歴の分析
SELECT 
    query_text, 
    COUNT(*) AS count,
    SUM(total_elapsed_time) AS total_elapsed_time
FROM TABLE(snowflake.information_schema.query_history(RESULT_LIMIT => 1000))
WHERE query_tag = 'ff2_challenge' 
    AND query_text NOT ILIKE '%information_schema%'
GROUP BY query_text
ORDER BY count DESC;


-- 2. クエリ時間の平均
SELECT 
    query_text, 
    COUNT(*) AS count,
    SUM(total_elapsed_time) AS total_elapsed_time,
    avg(total_elapsed_time) AS avg_elapsed_time
FROM TABLE(snowflake.information_schema.query_history(RESULT_LIMIT => 1000))
WHERE query_tag = 'ff2_challenge' 
    AND query_text NOT ILIKE '%information_schema%'
GROUP BY query_text
ORDER BY avg_elapsed_time DESC;

SELECT C_MKTSEGMENT,count(*) as count__num FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
group by C_MKTSEGMENT;



-- ===========================================
-- 新機能！クエリ・インサイトのデモンストレーション
-- ===========================================

--Sample1
--顧客テーブルと国名マスタのジョインをやってみる（ジョインキーの指定なし）
SELECT * from
snowflake_sample_data.tpch_sf1.customer
join
snowflake_sample_data.tpch_sf1.nation;


--Sample2
--もっと複雑なクエリで真価を発揮！！
WITH customer_details AS (
    -- 顧客情報にNATION、REGIONを紐付けするサブクエリ
    SELECT 
        c.C_CUSTKEY,
        c.C_NAME as customer_name,
        c.C_ADDRESS as customer_address,
        c.C_PHONE as customer_phone,
        c.C_MKTSEGMENT as market_segment,
        n.N_NAME as nation_name,
        r.R_NAME as region_name,
        r.R_COMMENT as region_comment
    FROM snowflake_sample_data.tpch_sf1.CUSTOMER c
    INNER JOIN snowflake_sample_data.tpch_sf1.NATION n  ON c.C_NATIONKEY = n.N_NATIONKEY
    --JOIN snowflake_sample_data.tpch_sf1.NATION n  --ここを変えてみよう
    INNER JOIN snowflake_sample_data.tpch_sf1.REGION r 
        ON n.N_REGIONKEY = r.R_REGIONKEY
),
order_summary AS (
    -- 注文の基本情報を取得するサブクエリ
    SELECT 
        O_ORDERKEY,
        O_CUSTKEY,
        O_ORDERSTATUS,
        O_TOTALPRICE,
        O_ORDERDATE,
        O_ORDERPRIORITY,
        O_CLERK,
        O_SHIPPRIORITY,
        -- 価格帯の分類
        CASE 
            WHEN O_TOTALPRICE < 50000 THEN '低額注文'
            WHEN O_TOTALPRICE BETWEEN 50000 AND 200000 THEN '中額注文'
            ELSE '高額注文'
        END as price_category
    FROM snowflake_sample_data.tpch_sf1.ORDERS
)
-- メインクエリ：全ての情報を統合
SELECT 
    -- 注文情報
    os.O_ORDERKEY as order_key,
    os.O_ORDERSTATUS as order_status,
    os.O_TOTALPRICE as total_price,
    os.O_ORDERDATE as order_date,
    os.O_ORDERPRIORITY as order_priority,
    os.price_category,
    -- 顧客情報（サブクエリから取得）
    cd.customer_name,
    cd.customer_address,
    cd.customer_phone,
    cd.market_segment,
    -- 地理的情報（サブクエリから取得）
    cd.nation_name,
    cd.region_name,
    cd.region_comment,
    O_ORDERDATE,
    -- リージョン別の価格ランキング（ウィンドウ関数との組み合わせ）
    RANK() OVER (PARTITION BY cd.region_name ORDER BY os.O_TOTALPRICE DESC) as region_price_rank
FROM order_summary os
LEFT JOIN customer_details cd ON os.O_CUSTKEY = cd.C_CUSTKEY

-- 高額注文または特定リージョンの絞り込み
WHERE (
    os.O_TOTALPRICE > 100000 
    OR cd.region_name IN ('AMERICA', 'ASIA', 'EUROPE')
)

ORDER BY 
    cd.region_name,
    os.O_TOTALPRICE DESC,
    os.O_ORDERDATE DESC;

-- 上位1000件に制限
--LIMIT 1000;





'''



SELECT 
    query_text, query_id,query_tag,
    COUNT(*) AS count,
    SUM(total_elapsed_time) AS total_elapsed_time
FROM snowflake.account_usage.query_history
WHERE query_tag = 'ff_challenge' 
    AND query_text NOT ILIKE '%information_schema%'
GROUP BY query_text, query_id, query_tag
ORDER BY count DESC;




SELECT
    *
FROM
    snowflake.account_usage.query_history AS t1
JOIN
    snowflake.account_usage.query_attribution_history AS t2
    ON t1.query_id = t2.query_id
order by t1.start_time desc;

    

SELECT
    t1.query_text,
    sum(credits_attributed_compute) as sum
FROM
    snowflake.account_usage.query_history AS t1
JOIN
    snowflake.account_usage.query_attribution_history AS t2
    ON t1.query_id = t2.query_id
WHERE t1.query_tag = 'ff_challenge' 
GROUP BY
    t1.query_text;

'''
