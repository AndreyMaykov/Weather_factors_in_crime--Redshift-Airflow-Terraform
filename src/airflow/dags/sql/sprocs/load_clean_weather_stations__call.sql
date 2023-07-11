CALL wxcr_be.load_clean_weather_stations(
	  '{{params.s3_bucket}}'
	, '{{params.iam_role}}'
);	