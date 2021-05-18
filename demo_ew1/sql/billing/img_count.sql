set @date_from = CURDATE() - INTERVAL 1 day;
set @date_to = CURDATE();

SELECT DATE_FORMAT(start_time, "%Y/%m/%d") AS exec, auth_user, DATABASE() as customer, response_key, COUNT(*) AS total_img
FROM doc_verification  LEFT JOIN dv_page AS pages
ON doc_verification.id = pages.document_verification_id
WHERE doc_verification.reception_time BETWEEN @date_from AND @date_to
AND 
    (pages.part LIKE 'Processed%'
    OR pages.part IS NULL)
    
GROUP BY 1,auth_user,response_key
