set @date_from = CURDATE() - INTERVAL 1 day;
set @date_to = CURDATE();

SELECT dv.template_key, dv.channel, DATABASE() as customer, dv.template_repository_version,
count(distinct dv2.id) as security_count,
sum(a.score) as sum_score,
count(*) as action_count,
count(distinct dv.id) as total_req
FROM
    doc_verification dv
LEFT JOIN doc_verification dv2 on dv.id = dv2.id and dv2.response_key = 'SecurityCheckFailed',
    dv_action a
WHERE
    dv.reception_time between @date_from and @date_to
    and a.document_verification_id=dv.id
    and a.action_category_id in (10, 17)

GROUP BY dv.channel, dv.template_name, dv.template_key, dv.template_repository_version
ORDER BY total_req desc;