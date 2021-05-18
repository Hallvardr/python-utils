set @date_from = CURDATE() - INTERVAL 1 day;
set @date_to = CURDATE();

SELECT date_format(CURTIME(), '%d/%m/%Y %h:%m') AS time,
date_format(curdate() - INTERVAL 1 day, '%d/%m/%Y %h:%m') AS period_from, date_format(curdate(), '%d/%m/%Y %h:%m') AS period_to,
template_name, document_type, country,
COUNT(case when response_key != 'NoFailedChecks' THEN 1 END) AS Failed,
COUNT(case when response_key = 'NoFailedChecks' THEN 1 END) AS Successful

FROM doc_verification

WHERE template_name NOT LIKE '%ICAO%'
AND reception_time BETWEEN @date_from AND @date_to

GROUP BY template_name;

