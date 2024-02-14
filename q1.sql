WITH account_latest AS (
SELECT 
	arr.account_name,
	arr.size,
	pss.status AS ps_status,
  status_date,
  arr.arr,
  cycle_status,
  cycle_date,
  ROW_NUMBER() OVER (PARTITION BY arr.account_name ORDER BY cycle_date DESC) AS latest_cycle_date_check,
FROM `studied-zephyr-393513.hibob_trial.account_arr` arr
 LEFT JOIN `studied-zephyr-393513.hibob_trial.account_cycle` cycle
 ON arr.account_name = cycle.account_name
 LEFT JOIN `studied-zephyr-393513.hibob_trial.account_ps` ps
 ON arr.account_name = ps.account_name
 LEFT JOIN `studied-zephyr-393513.hibob_trial.ps_status` pss
 ON ps.ps_id = pss.ps_id
),

arr_dec_2023 AS (
SELECT
 account_name,
 arr.arr AS arr_dec_2023
FROM `studied-zephyr-393513.hibob_trial.account_arr` arr
WHERE DATE_TRUNC(month, MONTH) = '2023-12-01'
),

account_last_approved AS (
SELECT
  account_name,
  MAX(cycle_date) AS last_approved_comp_cycle
FROM `studied-zephyr-393513.hibob_trial.account_cycle` cycle
WHERE cycle_status = 'Approved'
GROUP BY account_name
)

SELECT 
  size,
  account_latest.account_name,
  CASE 
   WHEN ps_status IS NULL THEN 'No Implementation'
   WHEN ps_status = 'In Progress' THEN 'In Implementation'
   WHEN ps_status = 'Completed' AND DATE_DIFF(CURRENT_DATE(), status_date, day) > 365 THEN 'Over 1 year since Implementation'
   WHEN ps_status = 'Copmleted' AND DATE_DIFF(CURRENT_DATE(), status_date, day) <= 365 THEN 'Under 1 year since Implementation'
   END AS implementation_indication,
  last_approved_comp_cycle,
  cycle_status AS last_comp_cycle_status,
  cycle_date AS last_comp_cycle_date,
  arr_dec_2023
FROM account_latest
 JOIN arr_dec_2023 
 ON account_latest.account_name = arr_dec_2023.account_name
 FULL OUTER JOIN account_last_approved
 ON account_latest.account_name = account_last_approved.account_name
WHERE latest_cycle_date_check = 1 
ORDER BY account_name
