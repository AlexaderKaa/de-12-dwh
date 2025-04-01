SELECT DISTINCT
    product->>'product_name' AS product_name
FROM
    outbox,
    jsonb_array_elements(event_value::jsonb->'product_payments') AS product
WHERE
    event_value::jsonb->'product_payments' IS NOT NULL
    AND product->>'product_name' IS NOT NULL;