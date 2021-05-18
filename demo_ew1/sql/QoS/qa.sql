set @date_from = CURDATE() - INTERVAL 1 day;
set @date_to = CURDATE();

select SUBSTRING_INDEX(database(),'_',-1) as tenant, date_format(dv.reception_time, '%d/%m/%Y %H:%i:%s') AS reception_time, qa.transaction_id, qa.channel, response_type, response_key,
template_name, number_images_processed as images, prf_qa_exec_time as qa_time,
CAST(TIMESTAMPDIFF(MICROSECOND, dv.start_time, dv.end_time)/1000 as INT) as total_time, image_result_key,
MAX(case when ch.operation = 'BLACK_WHITE' then score ELSE NULL END) AS 'BLACK_WHITE',
MAX(case when ch.operation = 'HOTSPOT' then score ELSE NULL END) AS 'HOTSPOT',
MAX(case when ch.operation = 'BLUR' then score ELSE NULL END) AS 'BLUR',
MAX(case when ch.operation = 'DARKNESS' then score ELSE NULL END) AS 'DARKNESS'

from doc_verification dv, doc_qa qa, doc_qa_image_result img, doc_qa_checks ch
where dv.transaction_id=qa.transaction_id and qa.id=img.document_qa_id and img.id=ch.doc_qa_image_result_id and qa_result='SILENT'
AND dv.reception_time between @date_from and @date_to

GROUP BY dv.transaction_id, image_result_key
ORDER BY start_time;