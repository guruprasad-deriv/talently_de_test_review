-- Query A: Deposit count by country
-- Dialect: BigQuery standard SQL
-- All countries in client_signup appear even if zero deposits.
-- Sort: zero-deposit countries first, then non-zero countries descending by deposit count.

SELECT cs.country
     , COUNT(fd.deposit_id)  AS deposit_count
  FROM `warehouse.dim_client`  cs
  LEFT JOIN `warehouse.fct_deposit` fd
         ON fd.client_id = cs.client_id
 GROUP BY cs.country
 ORDER BY CASE WHEN COUNT(fd.deposit_id) = 0 THEN 0 ELSE 1 END ASC
        , COUNT(fd.deposit_id) DESC
        , cs.country           ASC
