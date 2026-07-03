-- Query A: Deposit count by country
-- Shows ALL countries in client_signup, including those with zero deposits.
-- Uses LEFT JOIN from client_signup so unmatched rows (zero-deposit countries)
-- still appear with deposit_count = 0.
-- COUNT(d.deposit_id) not COUNT(*) — avoids counting the NULL join rows as 1.

SELECT cs.country
     , COUNT(d.deposit_id)               AS deposit_count
  FROM `deriv-warehouse.trading.client_signup`  cs
  LEFT JOIN `deriv-warehouse.trading.client_deposit` d
         ON d.client_id = cs.client_id
 GROUP BY cs.country
 ORDER BY CASE WHEN COUNT(d.deposit_id) = 0 THEN 0 ELSE 1 END ASC
        , COUNT(d.deposit_id) DESC
        , cs.country ASC
;
