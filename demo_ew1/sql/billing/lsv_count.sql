set @date_from = CURDATE() - INTERVAL 1 day;
set @date_to = CURDATE();

SELECT auth_user, channel, result, COUNT(*) AS total_lsv
FROM liveness_verification
WHERE start_time BETWEEN @date_from AND @date_to

GROUP BY auth_user,channel,result