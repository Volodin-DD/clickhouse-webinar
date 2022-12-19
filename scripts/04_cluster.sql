-- изучим топологию кластера
SELECT *
FROM system.clusters;

-- Создадим схему на кластере
CREATE DATABASE ch_webinar_cluster ON CLUSTER ontime_cluster;

-- Создадим физическую таблицу
CREATE TABLE ch_webinar_cluster.uk_price_paid_local ON CLUSTER ontime_cluster
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
    is_new UInt8,
    duration Enum8('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMM(date)
PRIMARY KEY (is_new, duration, type, postcode1, postcode2)
ORDER BY (is_new, duration, type, postcode1, postcode2, addr1, addr2)
SETTINGS index_granularity = 2048;

-- Создадим виртуальную распределённую таблицу
CREATE TABLE ch_webinar_cluster.uk_price_paid ON CLUSTER ontime_cluster
AS ch_webinar_cluster.uk_price_paid_local
ENGINE = Distributed('ontime_cluster', 'ch_webinar_cluster', 'uk_price_paid_local', cityHash64(town));

-- вставим данные в распределённую таблицу
INSERT INTO ch_webinar_cluster.uk_price_paid
WITH
   splitByChar(' ', postcode) AS p
SELECT
    toUInt32(price_string) AS price,
    parseDateTimeBestEffortUS(time) AS date,
    p[1] AS postcode1,
    p[2] AS postcode2,
    transform(a, ['T', 'S', 'D', 'F', 'O'], ['terraced', 'semi-detached', 'detached', 'flat', 'other']) AS type,
    b = 'Y' AS is_new,
    transform(c, ['F', 'L', 'U'], ['freehold', 'leasehold', 'unknown']) AS duration,
    addr1,
    addr2,
    street,
    locality,
    town,
    district,
    county
FROM ch_webinar_local.uk_price_paid_url
SETTINGS max_http_get_redirects=10;

-- шардирование в деле
SELECT
    shardNum() AS shard,
    town,
    count()
FROM ch_webinar_cluster.uk_price_paid
GROUP BY shard, town;

-- распределённые джоины
WITH right AS (
    SELECT
        date,
        count() AS n2
    FROM ch_webinar_cluster.uk_price_paid_local
    GROUP BY date
)

SELECT
    date,
    count() AS n1,
    any(n2) AS n2
FROM ch_webinar_cluster.uk_price_paid
LEFT JOIN right
USING date
GROUP BY date;

WITH right AS (
    SELECT
        town,
        count() AS n2
    FROM ch_webinar_cluster.uk_price_paid_local
    GROUP BY town
)

SELECT
    town,
    count() AS n1,
    any(n2) AS n2
FROM ch_webinar_cluster.uk_price_paid
LEFT JOIN right
USING town
GROUP BY town