-- セッション設定
ALTER SESSION SET query_tag = 'ff_challenge';

-- 共通パターンなしのランダムクエリ
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

SELECT COUNT(*) 
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER 
WHERE C_NATIONKEY = 15;

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
WHERE C_MKTSEGMENT = 'BUILDING' AND C_NATIONKEY = 21;

SELECT C_NAME, C_NATIONKEY 
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER 
WHERE C_MKTSEGMENT = 'MACHINERY' AND C_NATIONKEY = 9;

-- ===========================================
-- パフォーマンス・コスト分析クエリ集
-- ===========================================

-- 1. 基本的なクエリ実行履歴の分析
SELECT 
    query_text, 
    COUNT(*) AS count,
    SUM(total_elapsed_time) AS total_elapsed_time
FROM TABLE(snowflake.information_schema.query_history(RESULT_LIMIT => 1000))
WHERE query_tag = 'ff_challenge' 
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
WHERE query_tag = 'ff_challenge' 
    AND query_text NOT ILIKE '%information_schema%'
GROUP BY query_text
ORDER BY avg_elapsed_time DESC;

-- 3. 長時間実行クエリの特定（5秒以上）
SELECT 
    query_id,
    LEFT(query_text, 100) || '...' AS query_preview,
    user_name,
    warehouse_name,
    start_time,
    total_elapsed_time / 1000 AS execution_seconds,
    bytes_scanned,
    rows_produced
FROM TABLE(snowflake.information_schema.query_history(RESULT_LIMIT => 1000))
WHERE query_tag = 'ff_challenge'
    AND total_elapsed_time > 5000  -- 5秒以上
    AND query_text NOT ILIKE '%information_schema%'
ORDER BY total_elapsed_time DESC;

-- 4. 大量データスキャンクエリの特定（1MB以上）
SELECT 
    query_id,
    LEFT(query_text, 100) || '...' AS query_preview,
    user_name,
    warehouse_name,
    start_time,
    total_elapsed_time / 1000 AS execution_seconds,
    bytes_scanned / (1024 * 1024) AS mb_scanned,
    --partitions_scanned,
    --partitions_total,
    rows_produced
FROM TABLE(snowflake.information_schema.query_history(RESULT_LIMIT => 1000))
WHERE query_tag = 'ff_challenge'
    AND bytes_scanned > 1048576  -- 1MB以上
    AND query_text NOT ILIKE '%information_schema%'
ORDER BY bytes_scanned DESC;
