set @date_from = CURDATE() - INTERVAL 1 day;
set @date_to = CURDATE();

SELECT DATABASE() as customer, subkey, COUNT(*) AS face_transactions
FROM img_verification
WHERE img_verification.creation_date BETWEEN @date_from AND @date_to
GROUP BY 1,subkey 