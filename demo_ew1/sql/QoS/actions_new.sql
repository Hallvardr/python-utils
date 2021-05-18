set @date_from = CURDATE() - INTERVAL 1 day;
set @date_to = CURDATE();

SELECT MIN(id), MAX(id) INTO @min_id, @max_id
FROM doc_verification
WHERE reception_time between @date_from and @date_to;

SELECT
CONCAT(dv.TEMPLATE_REPOSITORY_VERSION, ", ", dv.template_name, ", ", i.action_name, ", ", dv.channel) AS id_unico,
i.action_category_id, i.threshold,
sum(i.score) AS score_sum,
COUNT(i.score) AS count_sum,
COUNT(case when action_result = 0 THEN 1 END) AS result_OK,
SUM(case when action_result = 0 THEN i.score END) AS sum_score_OK,
COUNT(case when action_result != 0 THEN 1 END) AS result_KO,
SUM(case when action_result != 0 THEN i.score END) AS sum_score_KO,
COUNT(case when response_key = 'NoFailedChecks' THEN 1 END) AS Successful,
COUNT(case when response_key != 'NoFailedChecks' THEN 1 END) AS Failed

FROM
doc_verification dv
    LEFT JOIN dv_action i on dv.id=i.document_verification_id

WHERE
dv.id between @min_id and @max_id
AND i.action_category_id IN (17, 21, 223)
AND response_key NOT LIKE '%MRZ%'

GROUP BY id_unico;