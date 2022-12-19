-- Комбинатор -If
SELECT
    toYear(date) AS year,
    avgIf(price, type == 'detached') AS detached_price,
    avgIf(price, type != 'detached') AS others_price,
    detached_price / others_price
FROM ch_webinar_local.uk_price_paid_summing
GROUP BY year
ORDER BY year;

-- Комбинатор -Distinct
SELECT
    count(postcode1),
    countDistinct(postcode1),
    uniqExact(postcode1)
FROM ch_webinar_local.uk_price_paid;

-- Комбинатор -State
WITH states AS (
    SELECT
        date,
        sumState(price) AS price_state
    FROM ch_webinar_local.uk_price_paid
    GROUP BY date
)

SELECT
    toStartOfMonth(date) AS month,
    sumMerge(price_state) AS monthly_prices
FROM states
GROUP BY month
ORDER BY month;

-- Оконные функции
WITH states AS (
    SELECT
        date,
        avgState(price) AS price_state
    FROM ch_webinar_local.uk_price_paid
    GROUP BY date
)

SELECT
    date,
    avgMerge(price_state) OVER(ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)AS cumsum
FROM states
ORDER BY date;

-- статистика
SELECT
    toYear(date) AS year,
    stddevPop(price) AS sd_price,
    skewPop(price) AS skewness,
    kurtPop(price) AS kurtosis
FROM ch_webinar_local.uk_price_paid
WHERE type != 'detached'
GROUP BY year;

-- временные ряды
SELECT
    date,
    exponentialMovingAverage(360)(price, toUInt64(date))
FROM ch_webinar_local.uk_price_paid_summing
GROUP BY date;

-- диаграммы
SELECT
    type,
    count() AS n_objects,
    bar(n_objects, 0, 5000000, 20) AS diag
FROM ch_webinar_local.uk_price_paid_summing
GROUP BY types