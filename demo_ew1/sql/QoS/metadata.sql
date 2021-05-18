set @date_from = CURDATE() - INTERVAL 1 day;
set @date_to = CURDATE();

SELECT MIN(id), MAX(id)
INTO @min_id, @max_id
FROM doc_verification
WHERE start_time between @date_from and @date_to;

SELECT dv.transaction_id, dv.start_time, dv.channel, p.part, dv.response_key, dv.template_name,
    MAX(CASE WHEN m.page_metadata_key = 'CameraInfo_availableResolutions' THEN m.page_metadata_value ELSE NULL END) AS 'CameraInfo_availableResolutions',
    MAX(CASE WHEN m.page_metadata_key = 'CameraInfo_focusMode' THEN m.page_metadata_value ELSE NULL END) AS 'CameraInfo_focusMode',
    MAX(CASE WHEN m.page_metadata_key = 'CameraInfo_hardwareLevel' THEN m.page_metadata_value ELSE NULL END) AS 'CameraInfo_hardwareLevel',
    MAX(CASE WHEN m.page_metadata_key = 'CameraInfo_previewResolution' THEN m.page_metadata_value ELSE NULL END) AS 'CameraInfo_previewResolution',
    MAX(CASE WHEN m.page_metadata_key = 'CameraInfo_resolution' THEN m.page_metadata_value ELSE NULL END) AS 'CameraInfo_resolution',

    MAX(CASE WHEN m.page_metadata_key = 'Device_info_device_manufacturer' THEN m.page_metadata_value ELSE NULL END) AS 'Device_info_device_manufacturer',
    MAX(CASE WHEN m.page_metadata_key = 'Device_info_device_model' THEN m.page_metadata_value ELSE NULL END) AS 'Device_info_device_model',
    MAX(CASE WHEN m.page_metadata_key = 'Device_info_os_platform' THEN m.page_metadata_value ELSE NULL END) AS 'Device_info_os_platform',
    MAX(CASE WHEN m.page_metadata_key = 'Device_info_os_version' THEN m.page_metadata_value ELSE NULL END) AS 'Device_info_os_version',
    MAX(CASE WHEN m.page_metadata_key = 'Device_info_sdk_version' THEN m.page_metadata_value ELSE NULL END) AS 'Device_info_sdk_version',

    MAX(CASE WHEN m.page_metadata_key = 'Device_info_camera_resolution' THEN m.page_metadata_value ELSE NULL END) AS 'Device_info_camera_resolution',
    MAX(CASE WHEN m.page_metadata_key = 'Device_info_focal_distance' THEN m.page_metadata_value ELSE NULL END) AS 'Device_info_focal_distance',
    MAX(CASE WHEN m.page_metadata_key = 'Device_info_focus_mode' THEN m.page_metadata_value ELSE NULL END) AS 'Device_info_focus_mode',

    MAX(CASE WHEN m.page_metadata_key = 'QA_info_blur_mode' THEN m.page_metadata_value ELSE NULL END) AS 'QA_info_blur_mode',
    MAX(CASE WHEN m.page_metadata_key = 'QA_info_blur_score' THEN m.page_metadata_value ELSE NULL END) AS 'QA_info_blur_score',
    MAX(CASE WHEN m.page_metadata_key = 'QA_info_blur_threshold' THEN m.page_metadata_value ELSE NULL END) AS 'QA_info_blur_threshold',
    MAX(CASE WHEN m.page_metadata_key = 'QA_info_darkness_mode' THEN m.page_metadata_value ELSE NULL END) AS 'QA_info_darkness_mode',
    MAX(CASE WHEN m.page_metadata_key = 'QA_info_darkness_score' THEN m.page_metadata_value ELSE NULL END) AS 'QA_info_darkness_score',
    MAX(CASE WHEN m.page_metadata_key = 'QA_info_darkness_threshold' THEN m.page_metadata_value ELSE NULL END) AS 'QA_info_darkness_threshold',
    MAX(CASE WHEN m.page_metadata_key = 'QA_info_hotspot_mode' THEN m.page_metadata_value ELSE NULL END) AS 'QA_info_hotspot_mode',
    MAX(CASE WHEN m.page_metadata_key = 'QA_info_hotspot_score' THEN m.page_metadata_value ELSE NULL END) AS 'QA_info_hotspot_score',
    MAX(CASE WHEN m.page_metadata_key = 'QA_info_hotspot_threshold' THEN m.page_metadata_value ELSE NULL END) AS 'QA_info_hotspot_threshold',
    MAX(CASE WHEN m.page_metadata_key = 'QA_info_photocopy_detection_mode' THEN m.page_metadata_value ELSE NULL END) AS 'QA_info_photocopy_detection_mode',
    MAX(CASE WHEN m.page_metadata_key = 'QA_info_photocopy_detection_score' THEN m.page_metadata_value ELSE NULL END) AS 'QA_info_photocopy_detection_score',
    MAX(CASE WHEN m.page_metadata_key = 'QA_info_photocopy_detection_threshold' THEN m.page_metadata_value ELSE NULL END) AS 'QA_info_photocopy_detection_threshold',

    MAX(CASE WHEN m.page_metadata_key = 'SDK_config_capture_documents' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_config_capture_documents',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_config_detection_zone_enabled' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_config_detection_zone_enabled',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_config_edge_detection_mode' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_config_edge_detection_mode',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_config_shooter_mode' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_config_shooter_mode',

    MAX(CASE WHEN m.page_metadata_key = 'SDK_process_aspect_ratio_detected' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_process_aspect_ratio_detected',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_process_blur_count' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_process_blur_count',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_process_darkness_count' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_process_darkness_count',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_process_edges_detected_count' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_process_edges_detected_count',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_process_hotspot_count' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_process_hotspot_count',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_process_photocopy_count' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_process_photocopy_count',
	MAX(CASE WHEN m.page_metadata_key = 'SDK_process_processed_frames_count' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_process_processed_frames_count',

    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_capture_time' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_capture_time',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_crop_average_time' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_crop_average_time',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_crop_frames_processed' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_crop_frames_processed',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_crop_max_time' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_crop_max_time',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_crop_min_time' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_crop_min_time',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_edges_detection_average_time' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_edges_detection_average_time',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_edges_detection_frames_processed' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_edges_detection_frames_processed',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_edges_detection_max_time' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_edges_detection_max_time',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_edges_detection_min_time' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_edges_detection_min_time',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_engine_initialization_time' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_engine_initialization_time',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_initialization_time' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_initialization_time',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_iqa_initialization_time' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_iqa_initialization_time',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_qa_checks_average_time' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_qa_checks_average_time',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_qa_checks_frames_processed' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_qa_checks_frames_processed',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_qa_checks_max_time' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_qa_checks_max_time',
    MAX(CASE WHEN m.page_metadata_key = 'SDK_profiling_qa_checks_min_time' THEN m.page_metadata_value ELSE NULL END) AS 'SDK_profiling_qa_checks_min_time'

FROM doc_verification dv
	INNER JOIN dv_page p on p.document_verification_id = dv.id
	INNER JOIN dv_page_metadata m on m.page_id = p.id
    INNER JOIN dv_page_metadata m2 on m2.page_id = m.page_id AND (m2.page_metadata_key = 'Device_info_sdk_version')

WHERE dv.id between @min_id and @max_id

GROUP BY dv.transaction_id, p.part;
