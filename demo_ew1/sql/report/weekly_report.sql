set @date_from = CURDATE() - INTERVAL 1 week;
set @date_to = CURDATE();

SELECT date_format(CURTIME(), '%d/%m/%Y %h:%i') AS time,
date_format(curdate() - INTERVAL 1 week, '%d/%m/%Y %h:%i') AS period_from, date_format(CURDATE(), '%d/%m/%Y %h:%i') AS period_to,
template_name, document_type, country,
COUNT(case when response_key != 'NoFailedChecks' THEN 1 END) AS Failed,
COUNT(case when response_key = 'NoFailedChecks' THEN 1 END) AS Successful,
IF(template_name = 'Unknown', 'No', IF(document_type = 'ID Card' OR document_type = 'Passport', 'Yes', IF(template_name = 'Sweden Tax Authority ID Card 2017', 'No',
IF(template_name LIKE '%Finland%', 'No', 'No')))) AS 'ID Proofing'

FROM doc_verification

WHERE template_name NOT LIKE '%ICAO%'
AND reception_time BETWEEN @date_from AND @date_to

GROUP BY template_name, document_type, country;