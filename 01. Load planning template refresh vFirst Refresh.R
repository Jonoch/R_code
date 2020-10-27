# 0.1. Open the libraries ---------------------------------
library(tidyverse)
library(odbc)
library(getPass)
# 0.2 Get actuals and course arch ---------------------------------------------------------


# Create db connection
con <- DBI::dbConnect(odbc::odbc(), "cprd", UID = getPass::getPass(), PWD = getPass::getPass())
#con <- DBI::dbConnect(odbc::odbc(), "cqaf", UID = getPass::getPass(), PWD = getPass::getPass())

wd <- rstudioapi::getActiveDocumentContext()$path %>% dirname()
setwd(wd)

extract_planned_load = function(con,
                                #plan_year,
                                plan_ref_year) {
  print (glue::glue("querying planned load for years planned in:{plan_ref_year} ..."))
  #extract actual and load
  planned_ld <- DBI::dbGetQuery(con,
                                statement = paste(
                                  "SELECT
                                  LKAM.REFERENCE_YEAR AS YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END as FIXED_BUCKET,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END AS PLANNING_MODULE,
                                  FS.DESCRIPTION AS FUND_SOURCE,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END AS MONASH_LOAD,
                                  MFAC.DEGREE_FACULTY_NAME AS FACULTY,
                                  DFAC.DEGREE_FACULTY_NAME AS DEGREE_FACULTY,
                                  LKAM.COURSE_LEVEL,
                                  LKAM.SCA_COORD_CAMPUS AS CAMPUS,
                                  LKAM.UNIT_MODE,
                                  CASE WHEN LPD.COURSE_CD IS NULL THEN '* Key attribute bucket'
                                  ELSE LPD.COURSE_CD END AS COURSE_CD,
                                  CASE WHEN LPD.COURSE_CD IS NULL THEN '* Key attribute bucket'
                                  ELSE C_ABBREV.ABBREVIATION END AS COURSE_DESCRIPTION,
                                  CASE WHEN LPD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE LPD.SCA_LOCATION_CD END AS COURSE_LOCATION,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND LPD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END AS DESIGNATED_IND,
                                  LPD.PLAN_YEAR,
                                  LPD.REVIEWED,
                                  CASE WHEN LPD.COMMENCING_IND = 'COMMENCING_LOAD' THEN 'Commencing'
                                  WHEN LPD.COMMENCING_IND = 'RETURNING_LOAD' THEN 'Returning'
                                  END as COM_RET,
                                  SUM(LPD.EFTSL) AS EFTSL ,
                                  CASE WHEN PLAN_YEAR = ", plan_ref_year + 1, " THEN 'Plan Year 1'
                                  WHEN PLAN_YEAR = ", plan_ref_year + 2, " THEN 'Plan Year 2'
                                  WHEN PLAN_YEAR = ", plan_ref_year + 3, " THEN 'Plan Year 3'
                                  WHEN PLAN_YEAR = ", plan_ref_year + 4, " THEN 'Plan Year 4'
                                  WHEN PLAN_YEAR = ", plan_ref_year + 5, " THEN 'Plan Year 5'
                                  END AS MEASURE
                                  FROM
                                  (
                                  SELECT * FROM LPM_CUSTOM.LPM_PLAN_DETAILS
                                  UNPIVOT
                                  (
                                  EFTSL
                                  for COMMENCING_IND in (\"COMMENCING_LOAD\",\"RETURNING_LOAD\")
                                  ) UPVT
                                  )LPD
                                  LEFT JOIN LPM_CUSTOM.LPM_KEY_ATTRIBUTES_MASTER LKAM
                                  ON LPD.KEY_ATTRIBUTES_MASTER_ID = LKAM.KEY_ATTRIBUTES_MASTER_ID
                                  LEFT JOIN SIS_OWNER.FUNDING_SOURCE FS
                                  ON LKAM.FUNDING_SOURCE = FS.FUNDING_SOURCE
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY DFAC
                                  ON LKAM.DEGREE_FACULTY_CD = DFAC.DEGREE_FACULTY_CD
                                  AND LKAM.REFERENCE_YEAR = DFAC.REFERENCE_YEAR
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY MFAC
                                  ON LKAM.OWNING_FACULTY = MFAC.DEGREE_FACULTY_CD
                                  AND LKAM.REFERENCE_YEAR = MFAC.REFERENCE_YEAR
                                  LEFT JOIN
                                  (SELECT CV.COURSE_CD,CV.ABBREVIATION FROM SIS_OWNER.COURSE_VERSION CV
                                  INNER JOIN
                                  (SELECT COURSE_CD,max(VERSION_NUMBER) AS VERSION_NUMBER
                                  FROM   SIS_OWNER.COURSE_VERSION
                                  WHERE START_DT <= TO_DATE('01/01/", plan_ref_year + 5, "','DD/MM/YYYY')
                                  GROUP BY COURSE_CD
                                  ORDER BY COURSE_CD) CV_MAX
                                  ON CV.COURSE_CD = CV_MAX.COURSE_CD
                                  AND CV.VERSION_NUMBER = CV_MAX.VERSION_NUMBER
                                  GROUP BY CV.COURSE_CD,ABBREVIATION
                                  ORDER BY CV.COURSE_CD,ABBREVIATION) C_ABBREV
                                  ON LPD.COURSE_CD = C_ABBREV.COURSE_CD
                                  WHERE LKAM.REFERENCE_YEAR = ", plan_ref_year - 1, " AND LPD.PLAN_YEAR BETWEEN ", plan_ref_year, " AND ", plan_ref_year + 4, "
                                  GROUP BY
                                  LKAM.REFERENCE_YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END ,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END,
                                  FS.DESCRIPTION,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END ,
                                  MFAC.DEGREE_FACULTY_NAME,
                                  DFAC.DEGREE_FACULTY_NAME,
                                  LKAM.COURSE_LEVEL,
                                  LKAM.SCA_COORD_CAMPUS,
                                  LKAM.UNIT_MODE,
                                  CASE WHEN LPD.COURSE_CD IS NULL THEN '* Key attribute bucket'
                                  ELSE LPD.COURSE_CD END,
                                  CASE WHEN LPD.COURSE_CD IS NULL THEN '* Key attribute bucket'
                                  ELSE C_ABBREV.ABBREVIATION END,
                                  CASE WHEN LPD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE LPD.SCA_LOCATION_CD END,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND LPD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END,
                                  LPD.PLAN_YEAR,
                                  LPD.REVIEWED,
                                  LPD.COMMENCING_IND
                                  ------------------------------------------------------------------
                                  
                                  UNION  --- Add previous year plan for current year
                                  SELECT
                                  LKAM.REFERENCE_YEAR AS YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END as FIXED_BUCKET,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END AS PLANNING_MODULE,
                                  FS.DESCRIPTION AS FUND_SOURCE,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END AS MONASH_LOAD,
                                  MFAC.DEGREE_FACULTY_NAME AS FACULTY,
                                  DFAC.DEGREE_FACULTY_NAME AS DEGREE_FACULTY,
                                  LKAM.COURSE_LEVEL,
                                  LKAM.SCA_COORD_CAMPUS AS CAMPUS,
                                  LKAM.UNIT_MODE,
                                  CASE WHEN LPD.COURSE_CD IS NULL THEN '* Key attribute bucket'
                                  ELSE LPD.COURSE_CD END AS COURSE_CD,
                                  CASE WHEN LPD.COURSE_CD IS NULL THEN '* Key attribute bucket'
                                  ELSE C_ABBREV.ABBREVIATION END AS COURSE_DESCRIPTION,
                                  CASE WHEN LPD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE LPD.SCA_LOCATION_CD END AS COURSE_LOCATION,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND LPD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END AS DESIGNATED_IND,
                                  LPD.PLAN_YEAR,
                                  LPD.REVIEWED,
                                  CASE WHEN LPD.COMMENCING_IND = 'COMMENCING_LOAD' THEN 'Commencing'
                                  WHEN LPD.COMMENCING_IND = 'RETURNING_LOAD' THEN 'Returning'
                                  END as COM_RET,
                                  SUM(LPD.EFTSL) AS EFTSL ,
                                  CASE WHEN PLAN_YEAR = ", plan_ref_year, " THEN 'Previous plan for current year'
                                  END AS MEASURE
                                  FROM
                                  (
                                  SELECT * FROM LPM_CUSTOM.LPM_PLAN_DETAILS
                                  UNPIVOT
                                  (
                                  EFTSL
                                  for COMMENCING_IND in (\"COMMENCING_LOAD\",\"RETURNING_LOAD\")
                                  ) UPVT
                                  )LPD
                                  LEFT JOIN LPM_CUSTOM.LPM_KEY_ATTRIBUTES_MASTER LKAM
                                  ON LPD.KEY_ATTRIBUTES_MASTER_ID = LKAM.KEY_ATTRIBUTES_MASTER_ID
                                  LEFT JOIN SIS_OWNER.FUNDING_SOURCE FS
                                  ON LKAM.FUNDING_SOURCE = FS.FUNDING_SOURCE
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY DFAC
                                  ON LKAM.DEGREE_FACULTY_CD = DFAC.DEGREE_FACULTY_CD
                                  AND LKAM.REFERENCE_YEAR = DFAC.REFERENCE_YEAR
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY MFAC
                                  ON LKAM.OWNING_FACULTY = MFAC.DEGREE_FACULTY_CD
                                  AND LKAM.REFERENCE_YEAR = MFAC.REFERENCE_YEAR
                                  LEFT JOIN
                                  (SELECT CV.COURSE_CD,CV.ABBREVIATION FROM SIS_OWNER.COURSE_VERSION CV
                                  INNER JOIN
                                  (SELECT COURSE_CD,max(VERSION_NUMBER) AS VERSION_NUMBER
                                  FROM   SIS_OWNER.COURSE_VERSION
                                  WHERE START_DT <= TO_DATE('01/01/", plan_ref_year + 5, "','DD/MM/YYYY')
                                  GROUP BY COURSE_CD
                                  ORDER BY COURSE_CD) CV_MAX
                                  ON CV.COURSE_CD = CV_MAX.COURSE_CD
                                  AND CV.VERSION_NUMBER = CV_MAX.VERSION_NUMBER
                                  GROUP BY CV.COURSE_CD,ABBREVIATION
                                  ORDER BY CV.COURSE_CD,ABBREVIATION) C_ABBREV
                                  ON LPD.COURSE_CD = C_ABBREV.COURSE_CD
                                  WHERE LKAM.REFERENCE_YEAR = ", plan_ref_year - 1, " AND LPD.PLAN_YEAR = ", plan_ref_year, "
                                  GROUP BY
                                  LKAM.REFERENCE_YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END ,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END,
                                  FS.DESCRIPTION,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END ,
                                  MFAC.DEGREE_FACULTY_NAME,
                                  DFAC.DEGREE_FACULTY_NAME,
                                  LKAM.COURSE_LEVEL,
                                  LKAM.SCA_COORD_CAMPUS,
                                  LKAM.UNIT_MODE,
                                  CASE WHEN LPD.COURSE_CD IS NULL THEN '* Key attribute bucket'
                                  ELSE LPD.COURSE_CD END,
                                  CASE WHEN LPD.COURSE_CD IS NULL THEN '* Key attribute bucket'
                                  ELSE C_ABBREV.ABBREVIATION END,
                                  CASE WHEN LPD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE LPD.SCA_LOCATION_CD END,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND LPD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END,
                                  LPD.PLAN_YEAR,
                                  LPD.REVIEWED,
                                  LPD.COMMENCING_IND
                                  ------------------------------------------------------------------
                                  UNION ALL -- APPEND THE ACTUALS TO THE BOTTOM
                                  select
                                  UNITLOAD.REFERENCE_YEAR AS YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END as FIXED_BUCKET,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END AS PLANNING_MODULE,
                                  CASE
                                  WHEN FS.DESCRIPTION IS NULL THEN 'UNCLASSIFIED'
                                  ELSE FS.DESCRIPTION END AS FUND_SOURCE,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END AS MONASH_LOAD,
                                  MFAC.DEGREE_FACULTY_NAME AS FACULTY,
                                  DFAC.DEGREE_FACULTY_NAME AS DEGREE_FACULTY,
                                  UNITLOAD.COURSE_LEVEL,
                                  UNITLOAD.SCA_COORD_CAMPUS AS CAMPUS,
                                  UNITLOAD.UNIT_MODE,
                                  UNITLOAD.COURSE_CD,
                                  C_ABBREV.ABBREVIATION AS COURSE_DESCRIPTION,
                                  CASE WHEN UNITLOAD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE UNITLOAD.SCA_LOCATION_CD END AS COURSE_LOCATION,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND UNITLOAD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END AS DESIGNATED_IND,
                                  '", plan_ref_year, "' AS PLAN_YEAR,
                                  'Actuals' AS REVIEWED,
                                  CASE WHEN UNITLOAD.COMMENCING_STUDENT_IND = 'Y' THEN 'Commencing'
                                  WHEN UNITLOAD.COMMENCING_STUDENT_IND = 'N' THEN 'Returning'
                                  END as COM_RET,
                                  SUM(EFTSL) AS EFTSL,
                                  'Current Actual Latest' AS MEASURE
                                  FROM LPM_CUSTOM.LPM_UNIT_LOAD UNITLOAD
                                  LEFT JOIN SIS_OWNER.FUNDING_SOURCE FS
                                  ON UNITLOAD.FUNDING_SOURCE = FS.FUNDING_SOURCE
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY DFAC
                                  ON UNITLOAD.DEGREE_FACULTY_ID = DFAC.DEGREE_FACULTY_ID
                                  AND UNITLOAD.REFERENCE_YEAR = UNITLOAD.REFERENCE_YEAR
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY MFAC
                                  ON UNITLOAD.FACULTY_ID = MFAC.DEGREE_FACULTY_ID
                                  AND UNITLOAD.REFERENCE_YEAR = MFAC.REFERENCE_YEAR
                                  LEFT JOIN
                                  (SELECT CV.COURSE_CD,CV.ABBREVIATION FROM SIS_OWNER.COURSE_VERSION CV
                                  INNER JOIN
                                  (SELECT COURSE_CD,max(VERSION_NUMBER) AS VERSION_NUMBER
                                  FROM   SIS_OWNER.COURSE_VERSION
                                  WHERE START_DT <= TO_DATE('01/01/", plan_ref_year + 5, "','DD/MM/YYYY')
                                  GROUP BY COURSE_CD
                                  ORDER BY COURSE_CD) CV_MAX
                                  ON CV.COURSE_CD = CV_MAX.COURSE_CD
                                  AND CV.VERSION_NUMBER = CV_MAX.VERSION_NUMBER
                                  GROUP BY CV.COURSE_CD,ABBREVIATION
                                  ORDER BY CV.COURSE_CD,ABBREVIATION) C_ABBREV
                                  ON UNITLOAD.COURSE_CD = C_ABBREV.COURSE_CD 
                                  WHERE SNAPSHOT_ID = 
                                  (SELECT CTRL.SNAPSHOT_ID
                                  FROM LPM_CUSTOM.LPM_AGGREGATION_CONTROL CTRL
                                  INNER JOIN
                                  (select REFERENCE_YEAR, MAX(SNAPSHOT_DATE) AS MAXDATE
                                  from LPM_CUSTOM.LPM_AGGREGATION_CONTROL
                                  GROUP BY REFERENCE_YEAR) YR
                                  ON YR.REFERENCE_YEAR = CTRL.REFERENCE_YEAR
                                  AND CTRL.SNAPSHOT_DATE = YR.MAXDATE
                                  WHERE CTRL.REFERENCE_YEAR IN (
                                  SELECT ",plan_ref_year," FROM
                                  DUAL))
                                  GROUP BY
                                  UNITLOAD.REFERENCE_YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END ,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END,
                                  FS.DESCRIPTION,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END,
                                  MFAC.DEGREE_FACULTY_NAME,
                                  DFAC.DEGREE_FACULTY_NAME,
                                  UNITLOAD.COURSE_LEVEL,
                                  UNITLOAD.SCA_COORD_CAMPUS,
                                  UNITLOAD.UNIT_MODE,
                                  UNITLOAD.COURSE_CD,
                                  C_ABBREV.ABBREVIATION,
                                  CASE WHEN UNITLOAD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE UNITLOAD.SCA_LOCATION_CD END,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND UNITLOAD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END,
                                  UNITLOAD.COMMENCING_STUDENT_IND
                                  
                                  -----------------------------------------------------
                                  
                                  UNION ALL   -- Append ", plan_ref_year - 1, " EoY Actuals
                                  (select
                                  UNITLOAD.REFERENCE_YEAR AS YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END as FIXED_BUCKET,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END AS PLANNING_MODULE,
                                  CASE
                                  WHEN FS.DESCRIPTION IS NULL THEN 'UNCLASSIFIED'
                                  ELSE FS.DESCRIPTION END AS FUND_SOURCE,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END AS MONASH_LOAD,
                                  MFAC.DEGREE_FACULTY_NAME AS FACULTY,
                                  DFAC.DEGREE_FACULTY_NAME AS DEGREE_FACULTY,
                                  UNITLOAD.COURSE_LEVEL,
                                  UNITLOAD.SCA_COORD_CAMPUS AS CAMPUS,
                                  UNITLOAD.UNIT_MODE,
                                  UNITLOAD.COURSE_CD,
                                  C_ABBREV.ABBREVIATION AS COURSE_DESCRIPTION,
                                  CASE WHEN UNITLOAD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE UNITLOAD.SCA_LOCATION_CD END AS COURSE_LOCATION,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND UNITLOAD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END AS DESIGNATED_IND,
                                  '", plan_ref_year - 1, "' AS PLAN_YEAR,
                                  'Actuals' AS REVIEWED,
                                  CASE WHEN UNITLOAD.COMMENCING_STUDENT_IND = 'Y' THEN 'Commencing'
                                  WHEN UNITLOAD.COMMENCING_STUDENT_IND = 'N' THEN 'Returning'
                                  END as COM_RET,
                                  SUM(EFTSL) AS EFTSL,
                                  'Previous year actual latest' AS MEASURE
                                  FROM LPM_CUSTOM.LPM_UNIT_LOAD UNITLOAD
                                  LEFT JOIN SIS_OWNER.FUNDING_SOURCE FS
                                  ON UNITLOAD.FUNDING_SOURCE = FS.FUNDING_SOURCE
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY DFAC
                                  ON UNITLOAD.DEGREE_FACULTY_ID = DFAC.DEGREE_FACULTY_ID
                                  AND UNITLOAD.REFERENCE_YEAR = UNITLOAD.REFERENCE_YEAR
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY MFAC
                                  ON UNITLOAD.FACULTY_ID = MFAC.DEGREE_FACULTY_ID
                                  AND UNITLOAD.REFERENCE_YEAR = MFAC.REFERENCE_YEAR
                                  LEFT JOIN
                                  (SELECT CV.COURSE_CD,CV.ABBREVIATION FROM SIS_OWNER.COURSE_VERSION CV
                                  INNER JOIN
                                  (SELECT COURSE_CD,max(VERSION_NUMBER) AS VERSION_NUMBER
                                  FROM   SIS_OWNER.COURSE_VERSION
                                  WHERE START_DT <= TO_DATE('01/01/", plan_ref_year + 5, "','DD/MM/YYYY')
                                  GROUP BY COURSE_CD
                                  ORDER BY COURSE_CD) CV_MAX
                                  ON CV.COURSE_CD = CV_MAX.COURSE_CD
                                  AND CV.VERSION_NUMBER = CV_MAX.VERSION_NUMBER
                                  GROUP BY CV.COURSE_CD,ABBREVIATION
                                  ORDER BY CV.COURSE_CD,ABBREVIATION) C_ABBREV
                                  ON UNITLOAD.COURSE_CD = C_ABBREV.COURSE_CD
                                  WHERE SNAPSHOT_ID = 
                                    (SELECT CTRL.SNAPSHOT_ID
                                    FROM LPM_CUSTOM.LPM_AGGREGATION_CONTROL CTRL
                                    INNER JOIN
	                                    (select REFERENCE_YEAR, MAX(SNAPSHOT_DATE) AS MAXDATE
	                                     from LPM_CUSTOM.LPM_AGGREGATION_CONTROL
	                                    GROUP BY REFERENCE_YEAR) YR
                                      ON YR.REFERENCE_YEAR = CTRL.REFERENCE_YEAR
                                      AND CTRL.SNAPSHOT_DATE = YR.MAXDATE
                                      WHERE CTRL.REFERENCE_YEAR IN (
                                  	    SELECT ",plan_ref_year,"-1 FROM
                                  	    DUAL))
                                  GROUP BY
                                  UNITLOAD.REFERENCE_YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END ,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END,
                                  FS.DESCRIPTION,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END,
                                  MFAC.DEGREE_FACULTY_NAME,
                                  DFAC.DEGREE_FACULTY_NAME,
                                  UNITLOAD.COURSE_LEVEL,
                                  UNITLOAD.SCA_COORD_CAMPUS,
                                  UNITLOAD.UNIT_MODE,
                                  UNITLOAD.COURSE_CD,
                                  C_ABBREV.ABBREVIATION,
                                  CASE WHEN UNITLOAD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE UNITLOAD.SCA_LOCATION_CD END,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND UNITLOAD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END,
                                  UNITLOAD.COMMENCING_STUDENT_IND)
                                  
                                  --------------------------------------------------
                                  
                                  UNION ALL   -- Append ", plan_ref_year - 2, " EoY Actuals
                                  (select
                                  UNITLOAD.REFERENCE_YEAR AS YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END as FIXED_BUCKET,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END AS PLANNING_MODULE,
                                  CASE
                                  WHEN FS.DESCRIPTION IS NULL THEN 'UNCLASSIFIED'
                                  ELSE FS.DESCRIPTION END AS FUND_SOURCE,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END AS MONASH_LOAD,
                                  MFAC.DEGREE_FACULTY_NAME AS FACULTY,
                                  DFAC.DEGREE_FACULTY_NAME AS DEGREE_FACULTY,
                                  UNITLOAD.COURSE_LEVEL,
                                  UNITLOAD.SCA_COORD_CAMPUS AS CAMPUS,
                                  UNITLOAD.UNIT_MODE,
                                  UNITLOAD.COURSE_CD,
                                  C_ABBREV.ABBREVIATION AS COURSE_DESCRIPTION,
                                  CASE WHEN UNITLOAD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE UNITLOAD.SCA_LOCATION_CD END AS COURSE_LOCATION,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND UNITLOAD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END AS DESIGNATED_IND,
                                  '", plan_ref_year - 2, "' AS PLAN_YEAR,
                                  'Actuals' AS REVIEWED,
                                  CASE WHEN UNITLOAD.COMMENCING_STUDENT_IND = 'Y' THEN 'Commencing'
                                  WHEN UNITLOAD.COMMENCING_STUDENT_IND = 'N' THEN 'Returning'
                                  END as COM_RET,
                                  SUM(EFTSL) AS EFTSL,
                                  '2 year previous actual latest' AS MEASURE
                                  FROM LPM_CUSTOM.LPM_UNIT_LOAD UNITLOAD
                                  LEFT JOIN SIS_OWNER.FUNDING_SOURCE FS
                                  ON UNITLOAD.FUNDING_SOURCE = FS.FUNDING_SOURCE
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY DFAC
                                  ON UNITLOAD.DEGREE_FACULTY_ID = DFAC.DEGREE_FACULTY_ID
                                  AND UNITLOAD.REFERENCE_YEAR = UNITLOAD.REFERENCE_YEAR
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY MFAC
                                  ON UNITLOAD.FACULTY_ID = MFAC.DEGREE_FACULTY_ID
                                  AND UNITLOAD.REFERENCE_YEAR = MFAC.REFERENCE_YEAR
                                  LEFT JOIN
                                  (SELECT CV.COURSE_CD,CV.ABBREVIATION FROM SIS_OWNER.COURSE_VERSION CV
                                  INNER JOIN
                                  (SELECT COURSE_CD,max(VERSION_NUMBER) AS VERSION_NUMBER
                                  FROM   SIS_OWNER.COURSE_VERSION
                                  WHERE START_DT <= TO_DATE('01/01/", plan_ref_year + 5, "','DD/MM/YYYY')
                                  GROUP BY COURSE_CD
                                  ORDER BY COURSE_CD) CV_MAX
                                  ON CV.COURSE_CD = CV_MAX.COURSE_CD
                                  AND CV.VERSION_NUMBER = CV_MAX.VERSION_NUMBER
                                  GROUP BY CV.COURSE_CD,ABBREVIATION
                                  ORDER BY CV.COURSE_CD,ABBREVIATION) C_ABBREV
                                  ON UNITLOAD.COURSE_CD = C_ABBREV.COURSE_CD
                                  WHERE SNAPSHOT_ID = 
                                    (SELECT CTRL.SNAPSHOT_ID
                                    FROM LPM_CUSTOM.LPM_AGGREGATION_CONTROL CTRL
                                    INNER JOIN
	                                    (select REFERENCE_YEAR, MAX(SNAPSHOT_DATE) AS MAXDATE
	                                     from LPM_CUSTOM.LPM_AGGREGATION_CONTROL
	                                    GROUP BY REFERENCE_YEAR) YR
                                      ON YR.REFERENCE_YEAR = CTRL.REFERENCE_YEAR
                                      AND CTRL.SNAPSHOT_DATE = YR.MAXDATE
                                      WHERE CTRL.REFERENCE_YEAR IN (
                                  	    SELECT ",plan_ref_year,"- 2 FROM
                                  	    DUAL))
                                  GROUP BY
                                  UNITLOAD.REFERENCE_YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END ,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END,
                                  FS.DESCRIPTION,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END,
                                  MFAC.DEGREE_FACULTY_NAME,
                                  DFAC.DEGREE_FACULTY_NAME,
                                  UNITLOAD.COURSE_LEVEL,
                                  UNITLOAD.SCA_COORD_CAMPUS,
                                  UNITLOAD.UNIT_MODE,
                                  UNITLOAD.COURSE_CD,
                                  C_ABBREV.ABBREVIATION,
                                  CASE WHEN UNITLOAD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE UNITLOAD.SCA_LOCATION_CD END,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND UNITLOAD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END,
                                  UNITLOAD.COMMENCING_STUDENT_IND)
                                  
                                  --------------------------------------------------
                                  
                                  UNION ALL   -- Append ", plan_ref_year - 3, " EoY Actuals
                                  (select
                                  UNITLOAD.REFERENCE_YEAR AS YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END as FIXED_BUCKET,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END AS PLANNING_MODULE,
                                  CASE
                                  WHEN FS.DESCRIPTION IS NULL THEN 'UNCLASSIFIED'
                                  ELSE FS.DESCRIPTION END AS FUND_SOURCE,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END AS MONASH_LOAD,
                                  MFAC.DEGREE_FACULTY_NAME AS FACULTY,
                                  DFAC.DEGREE_FACULTY_NAME AS DEGREE_FACULTY,
                                  UNITLOAD.COURSE_LEVEL,
                                  UNITLOAD.SCA_COORD_CAMPUS AS CAMPUS,
                                  UNITLOAD.UNIT_MODE,
                                  UNITLOAD.COURSE_CD,
                                  C_ABBREV.ABBREVIATION AS COURSE_DESCRIPTION,
                                  CASE WHEN UNITLOAD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE UNITLOAD.SCA_LOCATION_CD END AS COURSE_LOCATION,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND UNITLOAD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END AS DESIGNATED_IND,
                                  '", plan_ref_year - 3, "' AS PLAN_YEAR,
                                  'Actuals' AS REVIEWED,
                                  CASE WHEN UNITLOAD.COMMENCING_STUDENT_IND = 'Y' THEN 'Commencing'
                                  WHEN UNITLOAD.COMMENCING_STUDENT_IND = 'N' THEN 'Returning'
                                  END as COM_RET,
                                  SUM(EFTSL) AS EFTSL,
                                  '3 year previous actual latest' AS MEASURE
                                  FROM LPM_CUSTOM.LPM_UNIT_LOAD UNITLOAD
                                  LEFT JOIN SIS_OWNER.FUNDING_SOURCE FS
                                  ON UNITLOAD.FUNDING_SOURCE = FS.FUNDING_SOURCE
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY DFAC
                                  ON UNITLOAD.DEGREE_FACULTY_ID = DFAC.DEGREE_FACULTY_ID
                                  AND UNITLOAD.REFERENCE_YEAR = UNITLOAD.REFERENCE_YEAR
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY MFAC
                                  ON UNITLOAD.FACULTY_ID = MFAC.DEGREE_FACULTY_ID
                                  AND UNITLOAD.REFERENCE_YEAR = MFAC.REFERENCE_YEAR
                                  LEFT JOIN
                                  (SELECT CV.COURSE_CD,CV.ABBREVIATION FROM SIS_OWNER.COURSE_VERSION CV
                                  INNER JOIN
                                  (SELECT COURSE_CD,max(VERSION_NUMBER) AS VERSION_NUMBER
                                  FROM   SIS_OWNER.COURSE_VERSION
                                  WHERE START_DT <= TO_DATE('01/01/", plan_ref_year + 5, "','DD/MM/YYYY')
                                  GROUP BY COURSE_CD
                                  ORDER BY COURSE_CD) CV_MAX
                                  ON CV.COURSE_CD = CV_MAX.COURSE_CD
                                  AND CV.VERSION_NUMBER = CV_MAX.VERSION_NUMBER
                                  GROUP BY CV.COURSE_CD,ABBREVIATION
                                  ORDER BY CV.COURSE_CD,ABBREVIATION) C_ABBREV
                                  ON UNITLOAD.COURSE_CD = C_ABBREV.COURSE_CD
                                  WHERE SNAPSHOT_ID = 
                                    (SELECT CTRL.SNAPSHOT_ID
                                    FROM LPM_CUSTOM.LPM_AGGREGATION_CONTROL CTRL
                                    INNER JOIN
	                                    (select REFERENCE_YEAR, MAX(SNAPSHOT_DATE) AS MAXDATE
	                                     from LPM_CUSTOM.LPM_AGGREGATION_CONTROL
	                                    GROUP BY REFERENCE_YEAR) YR
                                      ON YR.REFERENCE_YEAR = CTRL.REFERENCE_YEAR
                                      AND CTRL.SNAPSHOT_DATE = YR.MAXDATE
                                      WHERE CTRL.REFERENCE_YEAR IN (
                                  	    SELECT ",plan_ref_year,"-3 FROM
                                  	    DUAL))
                                  GROUP BY
                                  UNITLOAD.REFERENCE_YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END ,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END,
                                  FS.DESCRIPTION,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END,
                                  MFAC.DEGREE_FACULTY_NAME,
                                  DFAC.DEGREE_FACULTY_NAME,
                                  UNITLOAD.COURSE_LEVEL,
                                  UNITLOAD.SCA_COORD_CAMPUS,
                                  UNITLOAD.UNIT_MODE,
                                  UNITLOAD.COURSE_CD,
                                  C_ABBREV.ABBREVIATION,
                                  CASE WHEN UNITLOAD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE UNITLOAD.SCA_LOCATION_CD END,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND UNITLOAD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END,
                                  UNITLOAD.COMMENCING_STUDENT_IND)
                                  --------------------------------------------------
                                  UNION ALL --Add PIT from last year
                                  (select
                                  UNITLOAD.REFERENCE_YEAR AS YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END as FIXED_BUCKET,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END AS PLANNING_MODULE,
                                  CASE
                                  WHEN FS.DESCRIPTION IS NULL THEN 'UNCLASSIFIED'
                                  ELSE FS.DESCRIPTION END AS FUND_SOURCE,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END AS MONASH_LOAD,
                                  MFAC.DEGREE_FACULTY_NAME AS FACULTY,
                                  DFAC.DEGREE_FACULTY_NAME AS DEGREE_FACULTY,
                                  UNITLOAD.COURSE_LEVEL,
                                  UNITLOAD.SCA_COORD_CAMPUS AS CAMPUS,
                                  UNITLOAD.UNIT_MODE,
                                  UNITLOAD.COURSE_CD,
                                  C_ABBREV.ABBREVIATION AS COURSE_DESCRIPTION,
                                  CASE WHEN UNITLOAD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE UNITLOAD.SCA_LOCATION_CD END AS COURSE_LOCATION,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND UNITLOAD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END AS DESIGNATED_IND,
                                  '", plan_ref_year - 1, "' AS PLAN_YEAR,
                                  'Actuals' AS REVIEWED,
                                  CASE WHEN UNITLOAD.COMMENCING_STUDENT_IND = 'Y' THEN 'Commencing'
                                  WHEN UNITLOAD.COMMENCING_STUDENT_IND = 'N' THEN 'Returning'
                                  END as COM_RET,
                                  SUM(EFTSL) AS EFTSL,
                                  'Last year PIT actuals'  AS MEASURE
                                  FROM LPM_CUSTOM.LPM_UNIT_LOAD UNITLOAD
                                  LEFT JOIN SIS_OWNER.FUNDING_SOURCE FS
                                  ON UNITLOAD.FUNDING_SOURCE = FS.FUNDING_SOURCE
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY DFAC
                                  ON UNITLOAD.DEGREE_FACULTY_ID = DFAC.DEGREE_FACULTY_ID
                                  AND UNITLOAD.REFERENCE_YEAR = UNITLOAD.REFERENCE_YEAR
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY MFAC
                                  ON UNITLOAD.FACULTY_ID = MFAC.DEGREE_FACULTY_ID
                                  AND UNITLOAD.REFERENCE_YEAR = MFAC.REFERENCE_YEAR
                                  LEFT JOIN
                                  (SELECT CV.COURSE_CD,CV.ABBREVIATION FROM SIS_OWNER.COURSE_VERSION CV
                                  INNER JOIN
                                  (SELECT COURSE_CD,max(VERSION_NUMBER) AS VERSION_NUMBER
                                  FROM   SIS_OWNER.COURSE_VERSION
                                  WHERE START_DT <= TO_DATE('01/01/", plan_ref_year + 5, "','DD/MM/YYYY')
                                  GROUP BY COURSE_CD
                                  ORDER BY COURSE_CD) CV_MAX
                                  ON CV.COURSE_CD = CV_MAX.COURSE_CD
                                  AND CV.VERSION_NUMBER = CV_MAX.VERSION_NUMBER
                                  GROUP BY CV.COURSE_CD,ABBREVIATION
                                  ORDER BY CV.COURSE_CD,ABBREVIATION) C_ABBREV
                                  ON UNITLOAD.COURSE_CD = C_ABBREV.COURSE_CD
                                  WHERE SNAPSHOT_ID =
                                  (SELECT MIN(SNAPSHOT_ID)
                                  FROM LPM_CUSTOM.LPM_AGGREGATION_CONTROL CTRL
                                  WHERE TO_CHAR(SNAPSHOT_DATE, 'DDMM') = TO_CHAR(SYSDATE, 'DDMM')
                                  AND REFERENCE_YEAR = ", plan_ref_year - 1, ")
                                  GROUP BY
                                  UNITLOAD.REFERENCE_YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END ,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END,
                                  FS.DESCRIPTION,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END,
                                  MFAC.DEGREE_FACULTY_NAME,
                                  DFAC.DEGREE_FACULTY_NAME,
                                  UNITLOAD.COURSE_LEVEL,
                                  UNITLOAD.SCA_COORD_CAMPUS,
                                  UNITLOAD.UNIT_MODE,
                                  UNITLOAD.COURSE_CD,
                                  C_ABBREV.ABBREVIATION,
                                  CASE WHEN UNITLOAD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE UNITLOAD.SCA_LOCATION_CD END,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND UNITLOAD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END,
                                  UNITLOAD.COMMENCING_STUDENT_IND)
                                  --------------------------------------------------
                                  UNION --- Add planning forecast for current year
                                  SELECT
                                  LKAM.REFERENCE_YEAR AS YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END as FIXED_BUCKET,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END AS PLANNING_MODULE,
                                  FS.DESCRIPTION AS FUND_SOURCE,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END AS MONASH_LOAD,
                                  MFAC.DEGREE_FACULTY_NAME AS FACULTY,
                                  DFAC.DEGREE_FACULTY_NAME AS DEGREE_FACULTY,
                                  LKAM.COURSE_LEVEL,
                                  LKAM.SCA_COORD_CAMPUS AS CAMPUS,
                                  LKAM.UNIT_MODE,
                                  CASE WHEN LPD.COURSE_CD IS NULL THEN '* Key attribute bucket'
                                  ELSE LPD.COURSE_CD END AS COURSE_CD,
                                  CASE WHEN LPD.COURSE_CD IS NULL THEN '* Key attribute bucket'
                                  ELSE C_ABBREV.ABBREVIATION END AS COURSE_DESCRIPTION,
                                  CASE WHEN LPD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE LPD.SCA_LOCATION_CD END AS COURSE_LOCATION,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND LPD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END AS DESIGNATED_IND,
                                  '", plan_ref_year, "' AS PLAN_YEAR,
                                  'Planning Forecast' AS REVIEWED,
                                  CASE WHEN LPD.COMMENCING_IND = 'COMMENCING_FORECAST_LOAD' THEN 'Commencing'
                                  WHEN LPD.COMMENCING_IND = 'RETURNING_FORECAST_LOAD' THEN 'Returning'
                                  END as COM_RET,
                                  SUM(LPD.EFTSL) AS EFTSL ,
                                  'Planning Forecast' AS MEASURE
                                  FROM
                                  (
                                  SELECT * FROM LPM_CUSTOM.LPM_PLAN_FORECAST
                                  UNPIVOT
                                  (
                                  EFTSL
                                  for COMMENCING_IND in (\"COMMENCING_FORECAST_LOAD\",\"RETURNING_FORECAST_LOAD\")
                                  ) UPVT
                                  )LPD
                                  LEFT JOIN LPM_CUSTOM.LPM_KEY_ATTRIBUTES_MASTER LKAM
                                  ON LPD.KEY_ATTRIBUTES_MASTER_ID = LKAM.KEY_ATTRIBUTES_MASTER_ID
                                  LEFT JOIN SIS_OWNER.FUNDING_SOURCE FS
                                  ON LKAM.FUNDING_SOURCE = FS.FUNDING_SOURCE
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY DFAC
                                  ON LKAM.DEGREE_FACULTY_CD = DFAC.DEGREE_FACULTY_CD
                                  AND LKAM.REFERENCE_YEAR = DFAC.REFERENCE_YEAR
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY MFAC
                                  ON LKAM.OWNING_FACULTY = MFAC.DEGREE_FACULTY_CD
                                  AND LKAM.REFERENCE_YEAR = MFAC.REFERENCE_YEAR
                                  LEFT JOIN
                                  (SELECT CV.COURSE_CD,CV.ABBREVIATION FROM SIS_OWNER.COURSE_VERSION CV
                                  INNER JOIN
                                  (SELECT COURSE_CD,max(VERSION_NUMBER) AS VERSION_NUMBER
                                  FROM   SIS_OWNER.COURSE_VERSION
                                  WHERE START_DT <= TO_DATE('01/01/2024','DD/MM/YYYY')
                                  GROUP BY COURSE_CD
                                  ORDER BY COURSE_CD) CV_MAX
                                  ON CV.COURSE_CD = CV_MAX.COURSE_CD
                                  AND CV.VERSION_NUMBER = CV_MAX.VERSION_NUMBER
                                  GROUP BY CV.COURSE_CD,ABBREVIATION
                                  ORDER BY CV.COURSE_CD,ABBREVIATION) C_ABBREV
                                  ON LPD.COURSE_CD = C_ABBREV.COURSE_CD
                                  GROUP BY
                                  LKAM.REFERENCE_YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END ,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END,
                                  FS.DESCRIPTION,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END ,
                                  MFAC.DEGREE_FACULTY_NAME,
                                  DFAC.DEGREE_FACULTY_NAME,
                                  LKAM.COURSE_LEVEL,
                                  LKAM.SCA_COORD_CAMPUS,
                                  LKAM.UNIT_MODE,
                                  CASE WHEN LPD.COURSE_CD IS NULL THEN '* Key attribute bucket'
                                  ELSE LPD.COURSE_CD END,
                                  CASE WHEN LPD.COURSE_CD IS NULL THEN '* Key attribute bucket'
                                  ELSE C_ABBREV.ABBREVIATION END,
                                  CASE WHEN LPD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE LPD.SCA_LOCATION_CD END,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND LKAM.COURSE_LEVEL = 'UG' AND LPD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
                                  'A0001','A0501','A0502','D0001','D0501','D0502') AND LPD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END,
                                  LPD.COMMENCING_IND
                                  
                                  --------------------------------------------------
                                  UNION -- Append Latest Monitoring Forecast
                                  SELECT
                                  MFORECAST.REFERENCE_YEAR AS YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END as FIXED_BUCKET,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END AS PLANNING_MODULE,
                                  FS.DESCRIPTION AS FUND_SOURCE,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END AS MONASH_LOAD,
                                  MFAC.DEGREE_FACULTY_NAME AS FACULTY,
                                  DFAC.DEGREE_FACULTY_NAME AS DEGREE_FACULTY,
                                  MFORECAST.COURSE_LEVEL,
                                  MFORECAST.SCA_COORD_CAMPUS AS CAMPUS,
                                  MFORECAST.UNIT_MODE,
                                  '* Key attribute bucket' AS COURSE_CD,
                                  '* Key attribute bucket' AS COURSE_DESCRIPTION,
                                  'NA' AS COURSE_LOCATION,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND MFORECAST.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND MFORECAST.COURSE_LEVEL = 'UG') THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END AS DESIGNATED_IND,
                                  '", plan_ref_year, "' AS PLAN_YEAR,
                                  'Monitoring Forecast' AS REVIEWED,
                                  CASE WHEN MFORECAST.COMMENCING_STUDENT_IND = 'Y' THEN 'Commencing'
                                  WHEN MFORECAST.COMMENCING_STUDENT_IND = 'N' THEN 'Returning'
                                  END as COM_RET,
                                  SUM(MFORECAST.REPORTED_FORECAST_LOAD) AS EFTSL ,
                                  'Monitoring Forecast' AS MEASURE
                                  FROM LPM_CUSTOM.LPM_FORECAST_LOAD MFORECAST
                                  LEFT JOIN SIS_OWNER.FUNDING_SOURCE FS
                                  ON MFORECAST.FUNDING_SOURCE = FS.FUNDING_SOURCE
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY DFAC
                                  ON MFORECAST.DEGREE_FACULTY_ID = DFAC.DEGREE_FACULTY_ID
                                  AND MFORECAST.REFERENCE_YEAR = DFAC.REFERENCE_YEAR
                                  LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY MFAC
                                  ON MFORECAST.FACULTY_ID = MFAC.DEGREE_FACULTY_ID
                                  AND MFORECAST.REFERENCE_YEAR = MFAC.REFERENCE_YEAR
                                  WHERE MFORECAST.FORECAST_CONTROL_ID = (
                                  SELECT MAX(FORECAST_CONTROL_ID)
                                  FROM LPM_CUSTOM.LPM_FORECAST_CONTROL)
                                  GROUP BY
                                  MFORECAST.REFERENCE_YEAR,
                                  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
                                  THEN 'Yes'
                                  ELSE 'No fixed bucket' END ,
                                  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
                                  WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
                                  WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
                                  ELSE 'DEMAND Planning' END,
                                  FS.DESCRIPTION,
                                  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
                                  ELSE 'Other Monash' END ,
                                  MFAC.DEGREE_FACULTY_NAME,
                                  DFAC.DEGREE_FACULTY_NAME,
                                  MFORECAST.COURSE_LEVEL,
                                  MFORECAST.SCA_COORD_CAMPUS,
                                  MFORECAST.UNIT_MODE,
                                  CASE WHEN (FS.DESCRIPTION = 'CSP' AND MFORECAST.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
                                  WHEN (FS.DESCRIPTION = 'CSP' AND MFORECAST.COURSE_LEVEL = 'UG') THEN 'CSP Undesignated bucket level UG'
                                  WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END,
                                  MFORECAST.COMMENCING_STUDENT_IND
                                  
                                  ----------------------------------------------------------------
                                  UNION ALL ------------- CURRENT YEAR LEVEL DATA
                                  select UNITLOAD.REFERENCE_YEAR AS YEAR,
  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
       THEN 'Yes'
       ELSE 'No fixed bucket' END as FIXED_BUCKET,
  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
       WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
       WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
       ELSE 'DEMAND Planning' END AS PLANNING_MODULE,
	CASE
		WHEN FS.DESCRIPTION IS NULL THEN 'UNCLASSIFIED'
		ELSE FS.DESCRIPTION END AS FUND_SOURCE,
	CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
	     ELSE 'Other Monash' END AS MONASH_LOAD,
	MFAC.DEGREE_FACULTY_NAME AS FACULTY,
	DFAC.DEGREE_FACULTY_NAME AS DEGREE_FACULTY,
	UNITLOAD.COURSE_LEVEL,
	UNITLOAD.SCA_COORD_CAMPUS AS CAMPUS,
	UNITLOAD.UNIT_MODE,
	UNITLOAD.COURSE_CD,
	C_ABBREV.ABBREVIATION AS COURSE_DESCRIPTION,
	CASE WHEN UNITLOAD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE UNITLOAD.SCA_LOCATION_CD END AS COURSE_LOCATION,
	CASE WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
	           'A0001','A0501','A0502','D0001','D0501','D0502') AND UNITLOAD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
	     WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END AS DESIGNATED_IND,
	'", plan_ref_year, "' AS PLAN_YEAR,
	'Actuals' AS REVIEWED,
	CASE WHEN UNITLOAD.COMMENCING_STUDENT_IND = 'Y' THEN 'Commencing'
	     WHEN UNITLOAD.COMMENCING_STUDENT_IND = 'N' THEN 'Returning'
	END as COM_RET,
	SUM(EFTSL) AS EFTSL,
	CASE WHEN (NOMINATED_YEAR_LEVEL = 1 OR NOMINATED_YEAR_LEVEL IS NULL) THEN 'CURRENT_YEAR_YEAR_1'
	WHEN (NOMINATED_YEAR_LEVEL = 2) THEN 'CURRENT_YEAR_YEAR_2'
	WHEN (NOMINATED_YEAR_LEVEL = 3) THEN 'CURRENT_YEAR_YEAR_3'
	WHEN (NOMINATED_YEAR_LEVEL = 4) THEN 'CURRENT_YEAR_YEAR_4'
	WHEN (NOMINATED_YEAR_LEVEL >= 5) THEN 'CURRENT_YEAR_YEAR_5+'  END AS MEASURE
FROM LPM_CUSTOM.LPM_UNIT_LOAD UNITLOAD
LEFT JOIN SIS_OWNER.FUNDING_SOURCE FS
	ON UNITLOAD.FUNDING_SOURCE = FS.FUNDING_SOURCE
LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY DFAC
	ON UNITLOAD.DEGREE_FACULTY_ID = DFAC.DEGREE_FACULTY_ID
	AND UNITLOAD.REFERENCE_YEAR = UNITLOAD.REFERENCE_YEAR
LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY MFAC
	ON UNITLOAD.FACULTY_ID = MFAC.DEGREE_FACULTY_ID
	AND UNITLOAD.REFERENCE_YEAR = MFAC.REFERENCE_YEAR
LEFT JOIN
		(SELECT CV.COURSE_CD,CV.ABBREVIATION FROM SIS_OWNER.COURSE_VERSION CV
			INNER JOIN
				(SELECT COURSE_CD,max(VERSION_NUMBER) AS VERSION_NUMBER
              FROM   SIS_OWNER.COURSE_VERSION
              WHERE START_DT <= TO_DATE('01/01/", plan_ref_year + 5, "','DD/MM/YYYY')
           				GROUP BY COURSE_CD
           				ORDER BY COURSE_CD) CV_MAX
				ON CV.COURSE_CD = CV_MAX.COURSE_CD
				AND CV.VERSION_NUMBER = CV_MAX.VERSION_NUMBER
		GROUP BY CV.COURSE_CD,ABBREVIATION
		ORDER BY CV.COURSE_CD,ABBREVIATION) C_ABBREV
ON UNITLOAD.COURSE_CD = C_ABBREV.COURSE_CD
WHERE SNAPSHOT_ID = 
    (SELECT CTRL.SNAPSHOT_ID
    FROM LPM_CUSTOM.LPM_AGGREGATION_CONTROL CTRL
    INNER JOIN
	 (select REFERENCE_YEAR, MAX(SNAPSHOT_DATE) AS MAXDATE
	 from LPM_CUSTOM.LPM_AGGREGATION_CONTROL
	 GROUP BY REFERENCE_YEAR) YR
    ON YR.REFERENCE_YEAR = CTRL.REFERENCE_YEAR
    AND CTRL.SNAPSHOT_DATE = YR.MAXDATE
    WHERE CTRL.REFERENCE_YEAR IN (
      SELECT ",plan_ref_year," FROM
      DUAL))
GROUP BY
  UNITLOAD.REFERENCE_YEAR,
  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
       THEN 'Yes'
       ELSE 'No fixed bucket' END ,
  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
       WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
       WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
       ELSE 'DEMAND Planning' END,
  FS.DESCRIPTION,
  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
	     ELSE 'Other Monash' END,
  MFAC.DEGREE_FACULTY_NAME,
	DFAC.DEGREE_FACULTY_NAME,
	UNITLOAD.COURSE_LEVEL,
	UNITLOAD.SCA_COORD_CAMPUS,
	UNITLOAD.UNIT_MODE,
	UNITLOAD.COURSE_CD,
	C_ABBREV.ABBREVIATION,
	UNITLOAD.NOMINATED_YEAR_LEVEL,
	CASE WHEN UNITLOAD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE UNITLOAD.SCA_LOCATION_CD END,
	CASE WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
	           'A0001','A0501','A0502','D0001','D0501','D0502') AND UNITLOAD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
	     WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END,
	CASE WHEN (NOMINATED_YEAR_LEVEL = 1 OR NOMINATED_YEAR_LEVEL IS NULL) THEN 'CURRENT_YEAR_YEAR_1'
	WHEN (NOMINATED_YEAR_LEVEL = 2) THEN 'CURRENT_YEAR_YEAR_2'
	WHEN (NOMINATED_YEAR_LEVEL = 3) THEN 'CURRENT_YEAR_YEAR_3'
	WHEN (NOMINATED_YEAR_LEVEL = 4) THEN 'CURRENT_YEAR_YEAR_4'
	WHEN (NOMINATED_YEAR_LEVEL >= 5) THEN 'CURRENT_YEAR_YEAR_5+' END,
	UNITLOAD.COMMENCING_STUDENT_IND
----------------------------------------------------
UNION -----APPEND PREVIOUS YEAR LEVEL DATA
select
UNITLOAD.REFERENCE_YEAR AS YEAR,
  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
       THEN 'Yes'
       ELSE 'No fixed bucket' END as FIXED_BUCKET,
  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
       WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
       WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
       ELSE 'DEMAND Planning' END AS PLANNING_MODULE,
	CASE
		WHEN FS.DESCRIPTION IS NULL THEN 'UNCLASSIFIED'
		ELSE FS.DESCRIPTION END AS FUND_SOURCE,
	CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
	     ELSE 'Other Monash' END AS MONASH_LOAD,
	MFAC.DEGREE_FACULTY_NAME AS FACULTY,
	DFAC.DEGREE_FACULTY_NAME AS DEGREE_FACULTY,
	UNITLOAD.COURSE_LEVEL,
	UNITLOAD.SCA_COORD_CAMPUS AS CAMPUS,
	UNITLOAD.UNIT_MODE,
	UNITLOAD.COURSE_CD,
	C_ABBREV.ABBREVIATION AS COURSE_DESCRIPTION,
	CASE WHEN UNITLOAD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE UNITLOAD.SCA_LOCATION_CD END AS COURSE_LOCATION,
	CASE WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
	           'A0001','A0501','A0502','D0001','D0501','D0502') AND UNITLOAD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
	     WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END AS DESIGNATED_IND,
	'", plan_ref_year, "' AS PLAN_YEAR,
	'Actuals' AS REVIEWED,
	CASE WHEN UNITLOAD.COMMENCING_STUDENT_IND = 'Y' THEN 'Commencing'
	     WHEN UNITLOAD.COMMENCING_STUDENT_IND = 'N' THEN 'Returning'
	END as COM_RET,
	SUM(EFTSL) AS EFTSL,
	CASE WHEN (NOMINATED_YEAR_LEVEL = 1 OR NOMINATED_YEAR_LEVEL IS NULL) THEN 'PREVIOUS_YEAR_YEAR_1'
	WHEN (NOMINATED_YEAR_LEVEL = 2) THEN 'PREVIOUS_YEAR_YEAR_2'
	WHEN (NOMINATED_YEAR_LEVEL = 3) THEN 'PREVIOUS_YEAR_YEAR_3'
	WHEN (NOMINATED_YEAR_LEVEL = 4) THEN 'PREVIOUS_YEAR_YEAR_4'
	WHEN (NOMINATED_YEAR_LEVEL >= 5) THEN 'PREVIOUS_YEAR_YEAR_5+'
	END AS MEASURE
FROM LPM_CUSTOM.LPM_UNIT_LOAD UNITLOAD
LEFT JOIN SIS_OWNER.FUNDING_SOURCE FS
	ON UNITLOAD.FUNDING_SOURCE = FS.FUNDING_SOURCE
LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY DFAC
	ON UNITLOAD.DEGREE_FACULTY_ID = DFAC.DEGREE_FACULTY_ID
	AND UNITLOAD.REFERENCE_YEAR = UNITLOAD.REFERENCE_YEAR
LEFT JOIN LPM_CUSTOM.LPM_DEGREE_FACULTY MFAC
	ON UNITLOAD.FACULTY_ID = MFAC.DEGREE_FACULTY_ID
	AND UNITLOAD.REFERENCE_YEAR = MFAC.REFERENCE_YEAR
LEFT JOIN
		(SELECT CV.COURSE_CD,CV.ABBREVIATION FROM SIS_OWNER.COURSE_VERSION CV
			INNER JOIN
				(SELECT COURSE_CD,max(VERSION_NUMBER) AS VERSION_NUMBER
              FROM   SIS_OWNER.COURSE_VERSION
              WHERE START_DT <= TO_DATE('01/01/", plan_ref_year + 5, "','DD/MM/YYYY')
           				GROUP BY COURSE_CD
           				ORDER BY COURSE_CD) CV_MAX
				ON CV.COURSE_CD = CV_MAX.COURSE_CD
				AND CV.VERSION_NUMBER = CV_MAX.VERSION_NUMBER
		GROUP BY CV.COURSE_CD,ABBREVIATION
		ORDER BY CV.COURSE_CD,ABBREVIATION) C_ABBREV
ON UNITLOAD.COURSE_CD = C_ABBREV.COURSE_CD
WHERE SNAPSHOT_ID = (SELECT CTRL.SNAPSHOT_ID
FROM LPM_CUSTOM.LPM_AGGREGATION_CONTROL CTRL
INNER JOIN
	(select REFERENCE_YEAR, MAX(SNAPSHOT_DATE) AS MAXDATE
	from LPM_CUSTOM.LPM_AGGREGATION_CONTROL
	GROUP BY REFERENCE_YEAR) YR
 ON YR.REFERENCE_YEAR = CTRL.REFERENCE_YEAR
 AND CTRL.SNAPSHOT_DATE = YR.MAXDATE
 WHERE CTRL.REFERENCE_YEAR IN (
 	SELECT ",plan_ref_year,"-1
 	FROM DUAL))
GROUP BY
  UNITLOAD.REFERENCE_YEAR,
  CASE WHEN FS.DESCRIPTION IN('CSP','RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL')
       THEN 'Yes'
       ELSE 'No fixed bucket' END ,
  CASE WHEN FS.DESCRIPTION IN('CSP') THEN 'CSP Planning'
       WHEN FS.DESCRIPTION IN('INTERNATIONAL: OFFSHORE PARTNERS') THEN 'Offshore Partner Planning'
       WHEN FS.DESCRIPTION IN('RESEARCH TRAINING PROGRAM - DOMESTIC','RESEARCH TRAINING PROGRAM - INTERNATIONAL') THEN 'RTP Planning'
       ELSE 'DEMAND Planning' END,
  FS.DESCRIPTION,
  CASE WHEN DFAC.MONASH_UNI_IND = 'Y' THEN 'Monash Uni'
	     ELSE 'Other Monash' END,
  MFAC.DEGREE_FACULTY_NAME,
	DFAC.DEGREE_FACULTY_NAME,
	UNITLOAD.COURSE_LEVEL,
	UNITLOAD.SCA_COORD_CAMPUS,
	UNITLOAD.UNIT_MODE,
	UNITLOAD.COURSE_CD,
	C_ABBREV.ABBREVIATION,
	UNITLOAD.NOMINATED_YEAR_LEVEL,
	CASE WHEN UNITLOAD.SCA_LOCATION_CD IS  NULL THEN 'NA' ELSE UNITLOAD.SCA_LOCATION_CD END,
	CASE WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'GPG') THEN 'CSP Designated GPG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('4531','4532','M6011','M6018')) THEN 'CSP Designated Medical UG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')) THEN 'CSP Designated Sub-bachelor UG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD IS NULL) THEN 'CSP Undesignated bucket level UG'
	     WHEN (FS.DESCRIPTION = 'CSP' AND UNITLOAD.COURSE_LEVEL = 'UG' AND UNITLOAD.COURSE_CD NOT IN('4531','4532','M6011','M6018',
	           'A0001','A0501','A0502','D0001','D0501','D0502') AND UNITLOAD.COURSE_CD IS NOT NULL) THEN  'CSP Undesignated course code level UG'
	     WHEN (FS.DESCRIPTION <> 'CSP') THEN 'Non-CSP' END,
	CASE WHEN (NOMINATED_YEAR_LEVEL = 1 OR NOMINATED_YEAR_LEVEL IS NULL) THEN 'PREVIOUS_YEAR_YEAR_1'
	WHEN (NOMINATED_YEAR_LEVEL = 2) THEN 'PREVIOUS_YEAR_YEAR_2'
	WHEN (NOMINATED_YEAR_LEVEL = 3) THEN 'PREVIOUS_YEAR_YEAR_3'
	WHEN (NOMINATED_YEAR_LEVEL = 4) THEN 'PREVIOUS_YEAR_YEAR_4'
	WHEN (NOMINATED_YEAR_LEVEL >= 5) THEN 'PREVIOUS_YEAR_YEAR_5+' END,
	UNITLOAD.COMMENCING_STUDENT_IND")
  )

}
Curr_Year <- as.numeric(format(Sys.Date(), "%Y"))

# Used for testing purposes, replace 2019 with Curr_Year
#PlanLoad_data <- extract_planned_load(con,2019)
PlanLoad_data <- extract_planned_load(con,Curr_Year)

# Manipulate to match standard output

Load_Planning_Temp_Refresh <- PlanLoad_data %>% 
  replace_na(list(FUND_SOURCE = "UNCLASSIFIED", DESIGNATED_IND = "Non-CSP")) %>% 
  group_by(FUND_SOURCE,FACULTY,DEGREE_FACULTY,COURSE_LEVEL,CAMPUS,UNIT_MODE,COURSE_CD,COURSE_DESCRIPTION,COM_RET,DESIGNATED_IND,MEASURE) %>%
  summarise(Load = sum(EFTSL)) %>%
  pivot_wider(id_cols = c(FUND_SOURCE,FACULTY,DEGREE_FACULTY,COURSE_LEVEL,CAMPUS,UNIT_MODE,COURSE_CD,COURSE_DESCRIPTION,COM_RET,DESIGNATED_IND), names_from = MEASURE, values_from = Load) %>%
  mutate_if(is.numeric, ~replace(., is.na(.), 0)) %>% 
  filter_at(vars(-(FUND_SOURCE:DESIGNATED_IND)), any_vars(. != 0)) %>%
  mutate(`Plan Year 5` = `Plan Year 4`) %>% 
  dplyr::select(FUND_SOURCE:DESIGNATED_IND,
         `3 year previous actual latest`, `2 year previous actual latest`,`Last year PIT actuals`,
         PREVIOUS_YEAR_YEAR_1,PREVIOUS_YEAR_YEAR_2,PREVIOUS_YEAR_YEAR_3,PREVIOUS_YEAR_YEAR_4,`PREVIOUS_YEAR_YEAR_5+`,`Previous year actual latest`,
         CURRENT_YEAR_YEAR_1, CURRENT_YEAR_YEAR_2, CURRENT_YEAR_YEAR_3, CURRENT_YEAR_YEAR_4, `CURRENT_YEAR_YEAR_5+`,`Current Actual Latest`, 
         #`Monitoring Forecast`, removed for 2020
         #`Planning Forecast`, removed for 2020
         `Previous plan for current year`, `Plan Year 1`, `Plan Year 2`, `Plan Year 3`, `Plan Year 4`, `Plan Year 5`)

# create Total UNIT_MODE row
Total_rows <- Load_Planning_Temp_Refresh %>% 
  group_by(FUND_SOURCE,FACULTY,DEGREE_FACULTY,COURSE_LEVEL,CAMPUS,COURSE_CD,COURSE_DESCRIPTION,COM_RET,DESIGNATED_IND) %>% 
  summarise_if(is.numeric, funs(sum)) %>%
  mutate(UNIT_MODE = "TOTAL")


#Union total row to data
LP_template_UnitMode_totals <- Load_Planning_Temp_Refresh %>% 
  union(Total_rows) %>% 
  arrange(FUND_SOURCE,FACULTY,DEGREE_FACULTY,COURSE_LEVEL,CAMPUS,COURSE_CD,COM_RET,UNIT_MODE)

#Aggregate HDR load
LP_aggregate_HDR <- LP_template_UnitMode_totals %>%
  filter(COURSE_LEVEL == "HDR") %>% 
  group_by(FUND_SOURCE,FACULTY,DEGREE_FACULTY,COURSE_LEVEL,CAMPUS,UNIT_MODE,
           COM_RET,DESIGNATED_IND) %>% 
  summarise_if(is.numeric, sum) %>%
  ungroup() %>% 
  mutate(COURSE_CD = "* Key attribute bucket") %>% 
  mutate(COURSE_DESCRIPTION = '* Key attribute bucket')

#Union Aggregate HDR rows to data
LP_template_refresh <- LP_template_UnitMode_totals %>%
  filter(!((COURSE_LEVEL == "HDR") & (COURSE_CD == "* Key attribute bucket"))) %>% 
  union(LP_aggregate_HDR) %>% 
  arrange(FUND_SOURCE,FACULTY,DEGREE_FACULTY,COURSE_LEVEL,CAMPUS,COURSE_CD,COM_RET,UNIT_MODE)
  



write_csv(LP_template_UnitMode_totals, "Load_Planning_template_refresh 2019.csv")
