WITH accounts AS (
SELECT 
  acc.account_name,
  acc.size,
  pss.status AS ps_status,
  status_date AS ps_status_date,
  cycle_status,
  cycle_date,
  ROW_NUMBER() OVER (PARTITION BY acc.account_name ORDER BY cycle_date DESC) AS latest_cycle_date_check
FROM `account_arr` acc
LEFT JOIN `account_cycle` cycle ON acc.account_name = cycle.account_name
LEFT JOIN `account_ps` ps ON acc.account_name = ps.account_name
LEFT JOIN `ps_status` pss ON ps.ps_id = pss.ps_id
),

arr_dec_2023 AS (
SELECT
 account_name,
 acc.arr
FROM `account_arr` acc
WHERE month = '2023-12-01'
),

last_approved AS (
SELECT
  account_name,
  MAX(cycle_date) AS last_approved_comp_cycle
FROM `account_cycle` cycle
WHERE cycle_status = 'Approved'
GROUP BY account_name
)

SELECT 
  size,
  accounts.account_name,
  CASE 
    WHEN ps_status IS NULL THEN 'No Implementation'
    WHEN ps_status = 'In Progress' THEN 'In Implementation'
    WHEN ps_status = 'Completed' AND DATE_DIFF(CURRENT_DATE(), ps_status_date, day) >= 365 THEN 'Over 1 year since Implementation'
    WHEN ps_status = 'Completed' AND DATE_DIFF(CURRENT_DATE(), ps_status_date, day) < 365 THEN 'Under 1 year since Implementation'
  END AS implementation_indication,
  last_approved_comp_cycle,
  cycle_status AS last_comp_cycle_status,
  cycle_date AS last_comp_cycle_date,
  arr_dec_2023.arr
FROM accounts
JOIN arr_dec_2023 ON accounts.account_name = arr_dec_2023.account_name
FULL OUTER JOIN last_approved ON accounts.account_name = last_approved.account_name
WHERE latest_cycle_date_check = 1 
ORDER BY account_name
