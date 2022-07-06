













































SELECT * from aws_s3.query_export_to_s3('select * from outs."FieldsOfStudy_view" where false',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/advanced/FieldsOfStudy/HEADER_FieldsOfStudy.txt', 'us-east-1'),
   options :='format csv, delimiter E''\t'', HEADER true, ESCAPE ''\'', NULL '''''
);


SELECT * from aws_s3.query_export_to_s3('select * from outs."FieldsOfStudy_view"',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/advanced/FieldsOfStudy/FieldsOfStudy.txt', 'us-east-1'),
   options :=' NULL '''' '
);



SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperFieldsOfStudy_view" where false',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/advanced/PaperFieldsOfStudy/HEADER_PaperFieldsOfStudy.txt', 'us-east-1'),
   options :='format csv, delimiter E''\t'', HEADER true, ESCAPE ''\'', NULL '''''
);


SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperFieldsOfStudy_view"',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/advanced/PaperFieldsOfStudy/PaperFieldsOfStudy.txt', 'us-east-1'),
   options :=' NULL '''' '
);



SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperMeSH_view" where false',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/advanced/PaperMeSH/HEADER_PaperMeSH.txt', 'us-east-1'),
   options :='format csv, delimiter E''\t'', HEADER true, ESCAPE ''\'', NULL '''''
);


SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperMeSH_view"',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/advanced/PaperMeSH/PaperMeSH.txt', 'us-east-1'),
   options :=' NULL '''' '
);



SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperRecommendations_view" where false',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/advanced/PaperRecommendations/HEADER_PaperRecommendations.txt', 'us-east-1'),
   options :='format csv, delimiter E''\t'', HEADER true, ESCAPE ''\'', NULL '''''
);


SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperRecommendations_view"',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/advanced/PaperRecommendations/PaperRecommendations.txt', 'us-east-1'),
   options :=' NULL '''' '
);



SELECT * from aws_s3.query_export_to_s3('select * from outs."Affiliations_view" where false',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/Affiliations/HEADER_Affiliations.txt', 'us-east-1'),
   options :='format csv, delimiter E''\t'', HEADER true, ESCAPE ''\'', NULL '''''
);


SELECT * from aws_s3.query_export_to_s3('select * from outs."Affiliations_view"',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/Affiliations/Affiliations.txt', 'us-east-1'),
   options :=' NULL '''' '
);



SELECT * from aws_s3.query_export_to_s3('select * from outs."AuthorExtendedAttributes_view" where false',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/AuthorExtendedAttributes/HEADER_AuthorExtendedAttributes.txt', 'us-east-1'),
   options :='format csv, delimiter E''\t'', HEADER true, ESCAPE ''\'', NULL '''''
);


SELECT * from aws_s3.query_export_to_s3('select * from outs."AuthorExtendedAttributes_view"',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/AuthorExtendedAttributes/AuthorExtendedAttributes.txt', 'us-east-1'),
   options :=' NULL '''' '
);



--SELECT * from aws_s3.query_export_to_s3('select * from outs."Authors_view" where false',
--   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/Authors/HEADER_Authors.txt', 'us-east-1'),
--   options :='format csv, delimiter E''\t'', HEADER true, ESCAPE ''\'', NULL '''''
--);


--SELECT * from aws_s3.query_export_to_s3('select * from outs."Authors_view"',
--   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/Authors/Authors.txt', 'us-east-1'),
--   options :=' NULL '''' '
--);



SELECT * from aws_s3.query_export_to_s3('select * from outs."Journals_view" where false',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/Journals/HEADER_Journals.txt', 'us-east-1'),
   options :='format csv, delimiter E''\t'', HEADER true, ESCAPE ''\'', NULL '''''
);


SELECT * from aws_s3.query_export_to_s3('select * from outs."Journals_view"',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/Journals/Journals.txt', 'us-east-1'),
   options :=' NULL '''' '
);



--SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperAuthorAffiliations_view" where false',
--   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/PaperAuthorAffiliations/HEADER_PaperAuthorAffiliations.txt', 'us-east-1'),
--   options :='format csv, delimiter E''\t'', HEADER true, ESCAPE ''\'', NULL '''''
--);


--SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperAuthorAffiliations_view"',
--   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/PaperAuthorAffiliations/PaperAuthorAffiliations.txt', 'us-east-1'),
--   options :=' NULL '''' '
--);



--SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperExtendedAttributes_view" where false',
--   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/PaperExtendedAttributes/HEADER_PaperExtendedAttributes.txt', 'us-east-1'),
--   options :='format csv, delimiter E''\t'', HEADER true, ESCAPE ''\'', NULL '''''
--);


--SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperExtendedAttributes_view"',
--   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/PaperExtendedAttributes/PaperExtendedAttributes.txt', 'us-east-1'),
--   options :=' NULL '''' '
--);



SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperReferences_view" where false',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/PaperReferences/HEADER_PaperReferences.txt', 'us-east-1'),
   options :='format csv, delimiter E''\t'', HEADER true, ESCAPE ''\'', NULL '''''
);


SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperReferences_view"',
   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/PaperReferences/PaperReferences.txt', 'us-east-1'),
   options :=' NULL '''' '
);



--SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperUrls_view" where false',
--   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/PaperUrls/HEADER_PaperUrls.txt', 'us-east-1'),
--   options :='format csv, delimiter E''\t'', HEADER true, ESCAPE ''\'', NULL '''''
--);


--SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperUrls_view"',
--   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/PaperUrls/PaperUrls.txt', 'us-east-1'),
--   options :=' NULL '''' '
--);



--SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperAbstractsInvertedIndex_view" where false',
--   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/nlp/PaperAbstractsInvertedIndex.txt.', 'us-east-1'),
--   options :='format csv, delimiter E''\t'', HEADER true, ESCAPE ''\'', NULL '''''
--);
--SELECT * from aws_s3.query_export_to_s3('select * from outs."PaperAbstractsInvertedIndex_view"',
--   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/nlp/PaperAbstractsInvertedIndex.txt.', 'us-east-1'),
--   options :='NULL '''''
--);


--SELECT * from aws_s3.query_export_to_s3('select * from outs."Papers_view" where false',
--   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/Papers/HEADER_Papers.txt', 'us-east-1'),
--   options :='format csv, delimiter E''\t'', HEADER true, ESCAPE ''\'', NULL '''''
--);


--SELECT * from aws_s3.query_export_to_s3('select * from outs."Papers_view"',
--   aws_commons.create_s3_uri('openalex-sandbox', 'data_dump_v1/2022-06-13/mag/Papers/Papers.txt', 'us-east-1'),
--   options :=' NULL '''' '
--);


