set @date_from = CURDATE() - INTERVAL 1 day;
set @date_to = CURDATE();

SELECT auth_user, DATABASE() as customer, response_key, COUNT(*) AS dv_transactions
FROM doc_verification
WHERE reception_time BETWEEN @date_from AND @date_to
GROUP BY 1,response_key
