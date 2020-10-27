# The following script aims to largely automate the data manipulation process of 
# building in the designated/non-designated, which is one of the final steps 
# of the load forecasting process. Please ensure the latest METIS txt file is in 
# S:\ADM-UPS\UPS\Kronos Load Monitoring\Automation\METIS File Correction\METIS File
#1. Call libraries -------------
library(tidyverse)
library(readxl)
library(getPass)
library(odbc)

#Read in METIS file and Change the file name to match the new METIS file created
METIS_original <- read_delim('../METIS File/KRONOS_METIS_Forecast_24092019.txt',
                           delim = "\t", guess_max = min(3000,9999)) %>%
  mutate_at(vars(TEACHING_FACULTY,TR_ORG_UNIT_CD),funs(as.character))


#2. Connect to CPRD --------------------
con <- DBI::dbConnect(odbc::odbc(), 'cprd', UID = getPass::getPass(), 
                     PWD = getPass::getPass())

#3. Query current Designated Actuals -----------------------
actual_medical_load <- DBI::dbGetQuery(con, 
statement = paste("SELECT
	TEACHING_FACULTY,
	TR_ORG_UNIT_CD,
	DEGREE_FACULTY_NAME AS DEGREE_FACULTY,
	FS.DESCRIPTION AS FUND_SOURCE_DESCRIPTION,
	LUL.COURSE_LEVEL AS COURSE_LEVEL,
	LUL.SCA_COORD_CAMPUS AS SCA_COORD_CAMPUS,
	LUL.SUA_LOCATION_CD AS SUA_LOCATION_CODE,
	LUL.UNIT_MODE AS UNIT_MODE,
	LUL.COMMENCING_STUDENT_IND AS COMMENCING_STUDENT_IND,
	--LCT.CLUSTER_CD AS OFFICIAL_CLUSTER,
  CLUSTER_SUBGROUP AS MONASH_CLUSTER,
  SUM(EFTSL) as MEDICAL_TAUGHT_LOAD,
  (SELECT TO_CHAR
    (SYSDATE, 'MM-DD-YYYY')
     FROM DUAL) as REPDATE
FROM LPM_CUSTOM.LPM_UNIT_LOAD LUL
left join LPM_CUSTOM.LPM_CLUSTER_TRANSLATION LCT on
LUL.CLUSTER_TRANSLATION_ID = LCT.CLUSTER_TRANSLATION_ID
left join LPM_CUSTOM.LPM_DEGREE_FACULTY LDF ON
LDF.DEGREE_FACULTY_ID = LUL.DEGREE_FACULTY_ID and
LDF.REFERENCE_YEAR = LUL.REFERENCE_YEAR
left join SIS_OWNER.FUNDING_SOURCE FS on
FS.FUNDING_SOURCE = LUL.FUNDING_SOURCE
WHERE SNAPSHOT_ID = (SELECT MAX(snapshot_id)
FROM LPM_CUSTOM.LPM_AGGREGATION_CONTROL
WHERE REFERENCE_YEAR = (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL))
AND COURSE_CD IN('4531','4532','M6011','M6018')
AND LUL.FUNDING_SOURCE = 'DETYA'
GROUP BY TEACHING_FACULTY, TR_ORG_UNIT_CD, DEGREE_FACULTY_NAME,FS.DESCRIPTION,LUL.COURSE_LEVEL,
   LUL.SCA_COORD_CAMPUS,LUL.SUA_LOCATION_CD,LUL.UNIT_MODE,LUL.COMMENCING_STUDENT_IND,LCT.CLUSTER_CD,CLUSTER_SUBGROUP
ORDER BY TEACHING_FACULTY, TR_ORG_UNIT_CD, DEGREE_FACULTY_NAME,FS.DESCRIPTION,LUL.COURSE_LEVEL,
   LUL.SCA_COORD_CAMPUS,LUL.SUA_LOCATION_CD,LUL.UNIT_MODE,LUL.COMMENCING_STUDENT_IND,LCT.CLUSTER_CD,CLUSTER_SUBGROUP"))

actual_subbach_load <- DBI::dbGetQuery(con,
                                       statement = paste("SELECT
	TEACHING_FACULTY,
	TR_ORG_UNIT_CD,
	DEGREE_FACULTY_NAME AS DEGREE_FACULTY,
	FS.DESCRIPTION AS FUND_SOURCE_DESCRIPTION,
	LUL.COURSE_LEVEL AS COURSE_LEVEL,
	LUL.SCA_COORD_CAMPUS AS SCA_COORD_CAMPUS,
	LUL.SUA_LOCATION_CD AS SUA_LOCATION_CODE,
	LUL.UNIT_MODE AS UNIT_MODE,
	LUL.COMMENCING_STUDENT_IND AS COMMENCING_STUDENT_IND,
	--LCT.CLUSTER_CD as OFFICIAL_CLUSTER,
  CLUSTER_SUBGROUP as MONASH_CLUSTER,
  SUM(EFTSL) as SUBBACH_TAUGHT_LOAD,
 	(SELECT TO_CHAR
    (SYSDATE, 'MM-DD-YYYY')
     FROM DUAL) as REPDATE
FROM LPM_CUSTOM.LPM_UNIT_LOAD LUL
left join LPM_CUSTOM.LPM_CLUSTER_TRANSLATION LCT on
LUL.CLUSTER_TRANSLATION_ID = LCT.CLUSTER_TRANSLATION_ID
left join LPM_CUSTOM.LPM_DEGREE_FACULTY LDF ON
LDF.DEGREE_FACULTY_ID = LUL.DEGREE_FACULTY_ID and
LDF.REFERENCE_YEAR = LUL.REFERENCE_YEAR
left join SIS_OWNER.FUNDING_SOURCE FS on
FS.FUNDING_SOURCE = LUL.FUNDING_SOURCE
WHERE SNAPSHOT_ID = (SELECT MAX(snapshot_id)
FROM LPM_CUSTOM.LPM_AGGREGATION_CONTROL
WHERE REFERENCE_YEAR = (SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL))
AND COURSE_CD IN('A0001','A0501','A0502','D0001','D0501','D0502')
AND LUL.FUNDING_SOURCE = 'DETYA'
GROUP BY TEACHING_FACULTY, TR_ORG_UNIT_CD, DEGREE_FACULTY_NAME,FS.DESCRIPTION,LUL.COURSE_LEVEL,
   LUL.SCA_COORD_CAMPUS,LUL.SUA_LOCATION_CD,LUL.UNIT_MODE,LUL.COMMENCING_STUDENT_IND,LCT.CLUSTER_CD,CLUSTER_SUBGROUP
ORDER BY TEACHING_FACULTY, TR_ORG_UNIT_CD, DEGREE_FACULTY_NAME,FS.DESCRIPTION,LUL.COURSE_LEVEL,
   LUL.SCA_COORD_CAMPUS,LUL.SUA_LOCATION_CD,LUL.UNIT_MODE,LUL.COMMENCING_STUDENT_IND,LCT.CLUSTER_CD,CLUSTER_SUBGROUP"))

Designated_load_actual <- actual_medical_load %>% 
  full_join(actual_subbach_load)

# Check if correct data date,number of rows and total
# This should return the current date
actual_medical_load$REPDATE
# This should also return the current date
actual_subbach_load$REPDATE[1:4]
# This should return the number of buckets with Medical load (as of 2020 n=4)
actual_medical_load %>% tally()
# This should return the number of buckets with Subbach load (as of 2020 n=141)
actual_subbach_load %>% tally()
# This checks if the tables joined correctly. N rows should be the above summed.
Designated_load_actual %>%  tally()
# This checks the totals and should match the sums of the SQL.
Designated_load_actual %>% summarise_if(is.numeric,sum,na.rm = TRUE)


METIS_des <- METIS_original %>% 
  left_join(select(Designated_load_actual, -REPDATE), by = c("TEACHING_FACULTY", "TR_ORG_UNIT_CD", 
                                           "DEGREE_FACULTY", 
                                           "FUND_SOURCE_DESCRIPTION", 
                                           "COURSE_LEVEL", "SCA_COORD_CAMPUS", 
                                           "SUA_LOCATION_CODE", "UNIT_MODE", 
                                           "COMMENCING_STUDENT_IND", 
                                           "MONASH_CLUSTER"))

# 4. Build in Designated/non-designated columns
METIS_working <- METIS_des %>% 
  mutate(DESIG_MED_UG = case_when(is.na(MEDICAL_TAUGHT_LOAD) ~ NA_real_,
                                  REPORTED_FORECAST_TAUGHT_LOAD >= MEDICAL_TAUGHT_LOAD ~ MEDICAL_TAUGHT_LOAD,
                                  REPORTED_FORECAST_TAUGHT_LOAD < MEDICAL_TAUGHT_LOAD ~ REPORTED_FORECAST_TAUGHT_LOAD
                                   )) %>% 
  mutate(SUBBACH =  case_when(is.na(SUBBACH_TAUGHT_LOAD) ~ NA_real_,
                              REPORTED_FORECAST_TAUGHT_LOAD >= SUBBACH_TAUGHT_LOAD ~ SUBBACH_TAUGHT_LOAD,
                              REPORTED_FORECAST_TAUGHT_LOAD < SUBBACH_TAUGHT_LOAD ~ REPORTED_FORECAST_TAUGHT_LOAD
                              )) %>% 
  mutate(NON_DESIGNATED = case_when(is.na(MEDICAL_TAUGHT_LOAD) &
                                      is.na(SUBBACH_TAUGHT_LOAD) ~ REPORTED_FORECAST_TAUGHT_LOAD,
                                    !is.na(MEDICAL_TAUGHT_LOAD) ~ REPORTED_FORECAST_TAUGHT_LOAD - DESIG_MED_UG,
                                    !is.na(SUBBACH_TAUGHT_LOAD) ~ REPORTED_FORECAST_TAUGHT_LOAD - SUBBACH
                                    ))
#Check mutates were successful, METIS_check should equal REPORTED_FORECAST_TAUGHT_LOAD
METIS_working %>% summarise_at(c("REPORTED_FORECAST_TAUGHT_LOAD","DESIG_MED_UG","SUBBACH","NON_DESIGNATED"),sum,na.rm = TRUE)  %>% 
  mutate(METIS_check = DESIG_MED_UG + SUBBACH + NON_DESIGNATED) 


METIS_long <- select(METIS_working,-REPORTED_FORECAST_TAUGHT_LOAD, 
                     -MEDICAL_TAUGHT_LOAD,
                     -SUBBACH_TAUGHT_LOAD) %>% 
  pivot_longer(cols = c("DESIG_MED_UG","SUBBACH","NON_DESIGNATED"),
               names_to = "Desig_Grouping",
               values_to = "REPORTED_FORECAST_TAUGHT_LOAD",
               values_drop_na = TRUE)

#Check values still match the METIS working file
METIS_working %>% summarise_at(c("REPORTED_FORECAST_TAUGHT_LOAD","DESIG_MED_UG","SUBBACH","NON_DESIGNATED"),sum,na.rm = TRUE)  %>% 
  mutate(METIS_check = DESIG_MED_UG + SUBBACH + NON_DESIGNATED)
#The breakdowns in the following chunk should match that of above
METIS_long %>% 
  group_by(Desig_Grouping) %>% 
  summarise(Forecast = sum(REPORTED_FORECAST_TAUGHT_LOAD))
METIS_long %>% summarise(REPORTED_FORECAST_TAUGHT_LOAD = sum(REPORTED_FORECAST_TAUGHT_LOAD))

#Correct the file for CSP GPG load and blank entries
METIS_final <- METIS_long %>%
  # Blanks in SUA_LOCATION_CODE == SCA_COORD_CAMPUS
  mutate(SUA_LOCATION_CODE = if_else(is.na(SUA_LOCATION_CODE),
                                     SCA_COORD_CAMPUS,SUA_LOCATION_CODE)) %>% 
  # Blanks in FUND_SOURCE == UNALLOCATED
  mutate(FUND_SOURCE = if_else(is.na(FUND_SOURCE),
                               'UNALLOCATED',FUND_SOURCE)) %>%
  # Blanks in FUND_SOURCE_DESCRIPTION == UNALLOCATED
  mutate(FUND_SOURCE_DESCRIPTION = if_else(is.na(FUND_SOURCE_DESCRIPTION),
                               'UNALLOCATED',FUND_SOURCE_DESCRIPTION)) %>% 
  # Assign Designated_GPG to all CSP GPG buckets
  mutate(Desig_Grouping = if_else((FUND_SOURCE_DESCRIPTION ==  'CSP' & 
                                     COURSE_LEVEL == 'GPG'), 
                                  'DESIGNATED_GPG',Desig_Grouping))

#Final Check
METIS_original %>% summarise(Forecast_total = sum(REPORTED_FORECAST_TAUGHT_LOAD))
METIS_long %>% 
  group_by(Desig_Grouping) %>% 
  summarise(Forecast = sum(REPORTED_FORECAST_TAUGHT_LOAD))
METIS_original %>% filter(FUND_SOURCE_DESCRIPTION ==  'CSP' & 
                            COURSE_LEVEL == 'GPG') %>% 
  summarise(DESIGNATED_GPG = sum(REPORTED_FORECAST_TAUGHT_LOAD))
# The DESIG_MED_UG, SUBBACH and Designated_DPD should match. 
# Non Designated should equal: Non Designated - Designated GPG
METIS_final %>% group_by(Desig_Grouping) %>% 
  summarise(Forecast_final = sum(REPORTED_FORECAST_TAUGHT_LOAD))

# Export CSV. Corrections will need to be made to the teaching faculty and
# Department columns in the final file. Change the file date to the
# date of the forecast if required (this should be automated)
reference_date <- METIS_final$CURR_YR_FORECAST_DATE[1]
write_csv(METIS_final,paste("METIS",reference_date,"w Des_Ind - blank fix required.csv"),
          na = "")
  
  




