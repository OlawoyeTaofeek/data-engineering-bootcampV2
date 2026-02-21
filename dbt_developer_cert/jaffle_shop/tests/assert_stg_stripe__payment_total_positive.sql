SELECT 
    order_id, 
    SUM(amount) as total_amount
FROM {{ ref('stg_stripe__payments') }}
GROUP BY 1
HAVING SUM(amount) < 0