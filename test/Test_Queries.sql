SELECT *
FROM CM_FULL_SYSTIME_IQ


SELECT *
FROM CM_REPORT_CMSPDAILY_R1_T1
WHERE
TIME_KEY='D091210'
AND UNIT_ID='BNKB012'

SELECT *
FROM CM_REPORT_CMSPWEEK_R1_T1
WHERE
TIME_KEY='W200949'
AND UNIT_ID='BNKB012'


SELECT *
FROM CM_REPDEF_COLUMNS_IQ
WHERE COD_CAMPAIGN='CMSPWEEK'



SELECT *
FROM CM_KPI_IQ
WHERE COD_KPI='SP013CRDC'


SELECT *
FROM CM_CAMPAIGN_HKPI_IQ
WHERE COD_KPI='SP013CRDC'

SELECT *
FROM CM_CAMPAIGN_HKPI_IQ
WHERE SUB_KPI_ROLE='TERM'

