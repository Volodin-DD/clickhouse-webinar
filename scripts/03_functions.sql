-- массивы
WITH pre AS (
    SELECT
        type,
        groupArray(date) AS dates,
        groupArray(price) AS prices
    FROM ch_webinar_local.uk_price_paid_summing
    GROUP BY type
)

SELECT
    type,
    arraySort((x, y) -> y, prices, dates) AS sorted_prices,
    arrayDifference(sorted_prices),
    length(arrayFilter(x -> x < 10000, prices)) AS n_cheap_objects
FROM pre;

-- строки
SELECT
    postcode1 || ' ' || postcode2 AS full_postcode,
    splitByChar(' ', full_postcode) AS arr_postcode,
    arrayStringConcat(arr_postcode)
FROM ch_webinar_local.uk_price_paid_summing
WHERE match(toString(type), '^detached')
LIMIT 100