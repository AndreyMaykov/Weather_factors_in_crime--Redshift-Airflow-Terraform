CALL wxcr_be.load_clean_crime_incidents(
	  '{{params.s3_bucket}}'
	, '{{params.iam_role}}'
);	