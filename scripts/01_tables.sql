-- Создание схемы

CREATE DATABASE ch_webinar_local;

-- Создание таблицы с движком URL
CREATE TABLE ch_webinar_local.uk_price_paid_url (
    uuid_string String,
    price_string String,
    time String,
    postcode String,
    a String,
    b String,
    c String,
    addr1 String,
    addr2 String,
    street String,
    locality String,
    town String,
    district String,
    county String,
    d String,
    e String
)
ENGINE = URL('http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv', 'CSV');

-- Создание таблицы с движком MergeTree
CREATE TABLE ch_webinar_local.uk_price_paid
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
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
PRIMARY KEY (is_new, duration, type, postcode1, postcode2)
ORDER BY (is_new, duration, type, postcode1, postcode2, addr1, addr2)
SETTINGS index_granularity = 8192;

-- Вставка данных в таблицу
INSERT INTO ch_webinar_local.uk_price_paid
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

-- Смотрим на то, как хранятся данные
SELECT *
FROM system.parts
WHERE table == 'uk_price_paid_summing';

-- удалим данные (прямо очистка, не мутация)
TRUNCATE TABLE ch_webinar_local.uk_price_paid;

-- создадим таблицу для суммирования
CREATE TABLE ch_webinar_local.uk_price_paid_summing (
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0)
)
ENGINE = SummingMergeTree()
PARTITION BY toYear(date)
ORDER BY (postcode1, postcode2);

-- создадим материализованное представление к суммированной таблице
CREATE MATERIALIZED VIEW ch_webinar_local.uk_price_paid_summing_mv TO ch_webinar_local.uk_price_paid_summing AS
SELECT
    price,
    date,
    postcode1,
    postcode2,
    type
FROM ch_webinar_local.uk_price_paid;