-- deploy model for estimated_citation using Redshift ML
-- feature parameters: citation_count, years_old, reference_count
-- output: multipler
-- put iam role with permission to AWS Sagemaker and S3 bucket which would be used by SageMaker
-- it takes 1 hour to deploy model and will create the function f_estimated_citation_multipler

DROP MODEL IF EXISTS m_estimated_citation
CREATE MODEL m_estimated_citation
FROM
	( SELECT 
			citation_count,  
			(2022 - year) as years_old, 
			reference_count, 
			float4(estimated_citation) / citation_count AS multipler
		FROM work 
		WHERE citation_count != 0
		LIMIT 10000 ) 
TARGET multipler 
FUNCTION f_estimated_citation_multipler
IAM_ROLE '<IAM-ROLE-ARN>' 
PROBLEM_TYPE REGRESSION 
SETTINGS ( 
	S3_BUCKET '<S3-BUCKET-NAME>' ,
	MAX_RUNTIME 3600
	);