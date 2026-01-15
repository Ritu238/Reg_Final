CREATE OR REPLACE PROCEDURE DISHA_L2_CURATED.BASE_MODEL_DATA.NET_CHARGES_TEST_PROCEDURE()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
    -- Logging Variables
	PROC_NAME VARCHAR(500):=''DISHA_L2_CURATED.BASE_MODEL_DATA.NET_CHARGES_test_PROCEDURE'';
	TBL_NAME VARCHAR(500):=''DISHA_L2_CURATED.BASE_MODEL_DATA.NET_CHARGES_test'';
    TOTAL_ROWS_IN_PREFINAL INTEGER DEFAULT 0;
    ROWS_INSERTED INTEGER DEFAULT 0;
    ROWS_UPDATED INTEGER DEFAULT 0;
    ROWS_UNCHANGED INTEGER DEFAULT 0;
    DUPLICATE_COUNT INTEGER;
    DUPLICATE_DETAILS VARCHAR(2000);
    CURR_TIME TIMESTAMP := TO_VARCHAR(CURRENT_TIMESTAMP());
    ERROR_MESSAGE VARCHAR;
    FINAL_OUTPUT VARCHAR;
	
	-- Variables for dynamic column handling
    HASH_COLUMN_LIST VARCHAR;
    INSERT_COLUMN_LIST VARCHAR;
    INSERT_VALUE_LIST VARCHAR;
    
    -- SQL statements to execute
    MERGE_STATEMENT VARCHAR;
    INSERT_STATEMENT VARCHAR;
	
BEGIN

CREATE TABLE IF NOT EXISTS DISHA_L2_CURATED.BASE_MODEL_DATA.NET_CHARGES_test
	(OLD_LOAN_NUMBER	VARCHAR(20),
	NEW_LOAN_NUMBER	VARCHAR(15),
	ADMINISTRATIVE_FEES	FLOAT,
	PROCESSING_FEES	FLOAT,
	IMD_SFDC	FLOAT,
	IMD_FC	FLOAT,
	SERVICE_CHARGE_PDD	FLOAT,
	BAJAJ_ALLIANZ_LIFE_INSURANCE	FLOAT,
	BAJAJ_ALLIANZ_GENERAL_INSURANCE	FLOAT,
	BHARTI_AXA_LIFE_INSURANCE	FLOAT,
	CARE_PROTECTION_PLAN	FLOAT,
	CERSAI_REGISTRATION_CHARGES	FLOAT,
	EQUITABLE_MORTGAGE_FEES	FLOAT,
	HDFC_LIFE_INSURANCE_COMPANY_LIMITED	FLOAT,
	ICICI_LOMBARD_GENERAL_INSURANCE	FLOAT,
	ICICI_PRUDENTIAL_LIFE_INSURANCE	FLOAT,
	KOTAK_LIFE_INSURANCE	FLOAT,
	RCU_CHARGES	FLOAT,
	SHRIRAM_PROPERTY_INSURANCE	FLOAT,
	STAMPING_CHARGES	FLOAT,
	VALUATION_CHARGES	FLOAT,
	LEGAL_CHARGES	FLOAT,
	FUTURE_GENERALI_INSURANCE	FLOAT,
	ICICI_LOMBARD_GIC_LIMITED	FLOAT,
	ODV_CHARGES	FLOAT,
	AF_PF_SCPDD_SUM_INC_IMD	FLOAT,
	TECH_LEGAL_SUM	FLOAT,
	FIRST_DISBURSAL_DATE	DATE,
	LOAN_STATUS	VARCHAR(10),
	DATA_SOURCE	VARCHAR(6),
	START_TIMESTAMP	TIMESTAMP,
	END_TIMESTAMP	TIMESTAMP,
	CURRENT_FLAG	BOOLEAN,
	ACTIVATION_FLAG	NUMBER(1,0),
	LOAN_AMOUNT FLOAT,
	ROW_HASH VARCHAR(255) AS SHA2(UPPER(ARRAY_TO_STRING([OLD_LOAN_NUMBER ,NEW_LOAN_NUMBER ,ADMINISTRATIVE_FEES ,PROCESSING_FEES ,IMD_SFDC ,IMD_FC ,SERVICE_CHARGE_PDD ,BAJAJ_ALLIANZ_LIFE_INSURANCE ,BAJAJ_ALLIANZ_GENERAL_INSURANCE ,BHARTI_AXA_LIFE_INSURANCE ,CARE_PROTECTION_PLAN ,CERSAI_REGISTRATION_CHARGES ,EQUITABLE_MORTGAGE_FEES ,HDFC_LIFE_INSURANCE_COMPANY_LIMITED ,ICICI_LOMBARD_GENERAL_INSURANCE ,ICICI_PRUDENTIAL_LIFE_INSURANCE ,KOTAK_LIFE_INSURANCE ,RCU_CHARGES ,SHRIRAM_PROPERTY_INSURANCE ,STAMPING_CHARGES ,VALUATION_CHARGES ,LEGAL_CHARGES ,FUTURE_GENERALI_INSURANCE ,ICICI_LOMBARD_GIC_LIMITED ,ODV_CHARGES ,AF_PF_SCPDD_SUM_INC_IMD ,TECH_LEGAL_SUM ,FIRST_DISBURSAL_DATE ,LOAN_STATUS,LOAN_AMOUNT], ''|'')), 256) 
	);	
	---------------- Create Temp Table --------------------
CREATE or replace temporary table disha_l2_curated.base_model_data.NET_CHARGES_test_prefinal 
	(OLD_LOAN_NUMBER VARCHAR(20),
	NEW_LOAN_NUMBER	VARCHAR(15),
	ADMINISTRATIVE_FEES	FLOAT,
	PROCESSING_FEES	FLOAT,
	IMD_SFDC	FLOAT,
	IMD_FC	FLOAT,
	SERVICE_CHARGE_PDD	FLOAT,
	BAJAJ_ALLIANZ_LIFE_INSURANCE	FLOAT,
	BAJAJ_ALLIANZ_GENERAL_INSURANCE	FLOAT,
	BHARTI_AXA_LIFE_INSURANCE	FLOAT,
	CARE_PROTECTION_PLAN	FLOAT,
	CERSAI_REGISTRATION_CHARGES	FLOAT,
	EQUITABLE_MORTGAGE_FEES	FLOAT,
	HDFC_LIFE_INSURANCE_COMPANY_LIMITED	FLOAT,
	ICICI_LOMBARD_GENERAL_INSURANCE	FLOAT,
	ICICI_PRUDENTIAL_LIFE_INSURANCE	FLOAT,
	KOTAK_LIFE_INSURANCE	FLOAT,
	RCU_CHARGES	FLOAT,
	SHRIRAM_PROPERTY_INSURANCE	FLOAT,
	STAMPING_CHARGES	FLOAT,
	VALUATION_CHARGES	FLOAT,
	LEGAL_CHARGES	FLOAT,
	FUTURE_GENERALI_INSURANCE	FLOAT,
	ICICI_LOMBARD_GIC_LIMITED	FLOAT,
	ODV_CHARGES	FLOAT,
	AF_PF_SCPDD_SUM_INC_IMD	FLOAT,
	TECH_LEGAL_SUM	FLOAT,
	FIRST_DISBURSAL_DATE	DATE,
	LOAN_STATUS	VARCHAR(10),
	LOAN_AMOUNT FLOAT);
	
INSERT INTO disha_l2_curated.base_model_data.NET_CHARGES_test_prefinal  
with base as (
select cast(cod_acct_no_old as varchar(20)) as old_loan_number, cast(trim(cod_acct_no_v1) as varchar(15)) as new_loan_number , 
administrative_fee/1.18 as administrative_fees, processing_fees/1.18 as processing_fees, imd_amount__c as imd_sfdc, sum(imd_fc) as imd_fc
,sum( case when Service_Charge_oracle is null then 0 else Service_Charge_oracle/1.18 end) as Service_Charge_pdd
,sum( case when Bajaj_Allianz_oracle is null then 0 else Bajaj_Allianz_oracle/1.18 end) as Bajaj_Allianz_Life_Insurance
,sum( case when BAJAJ_ALLIANZ_GENERAL_oracle is null then 0 else BAJAJ_ALLIANZ_GENERAL_oracle/1.18 end) as BAJAJ_ALLIANZ_GENERAL_Insurance
,sum( case when Bharti_Axa_oracle is null then 0 else Bharti_Axa_oracle/1.18 end) as Bharti_Axa_Life_Insurance
,sum( case when Care_health_oracle is null then 0 else Care_health_oracle/1.18 end) as Care_protection_plan
,sum( case when Cersai_oracle is null then 0 else Cersai_oracle/1.18 end) as Cersai_registration_charges
,sum( case when Equitable_oracle is null then 0 else Equitable_oracle/1.18 end) as Equitable_Mortgage_fees
,sum( case when HDFC_oracle is null then 0 else HDFC_oracle/1.18 end) as HDFC_Life_Insurance_company_limited
,sum( case when ICICI_Lombard_oracle is null then 0 else ICICI_Lombard_oracle/1.18 end) as ICICI_Lombard_General_Insurance
,sum( case when ICICI_prud_oracle is null then 0 else ICICI_prud_oracle/1.18 end) as ICICI_prudential_life_insurance
,sum( case when Kotak_oracle is null then 0 else Kotak_oracle/1.18 end) as Kotak_life_insurance
,sum( case when RCU_oracle is null then 0 else RCU_oracle/1.18 end) as RCU_charges
,sum( case when Shriram_oracle is null then 0 else Shriram_oracle/1.18 end) as Shriram_property_insurance
,sum( case when Stamping_oracle is null then 0 else Stamping_oracle/1.18 end) as Stamping_charges
,sum( case when Valuation_oracle is null then 0 else Valuation_oracle/1.18 end) as Valuation_charges
,sum( case when Legal_oracle is null then 0 else Legal_oracle/1.18 end) as Legal_charges
,sum( case when Future_generali_oracle is null then 0 else Future_generali_oracle/1.18 end) as FUTURE_GENERALI_INSURANCE
,sum( case when ICICI_Lombard_GIC_oracle is null then 0 else ICICI_Lombard_GIC_oracle/1.18 end) as ICICI_LOMBARD_GIC_LIMITED
,sum( case when ODV_oracle is null then 0 else ODV_oracle/1.18 end) as ODV_CHARGES

from (
select cod_acct_no_old, cod_acct_no_v1, administrative_fee, processing_fees, imd_fc, imd_amount__c
/* case when cod_sc_v1 in (441,1441) then charges end as Adm_fee_oracle */
/* ,case when cod_sc_v1 in (1106, 106) then charges end as PF_oracle */
,case when cod_sc_v1 in (263) then charges end as Bajaj_Allianz_oracle
,case when cod_sc_v1 in (454) then charges end as BAJAJ_ALLIANZ_GENERAL_oracle 
,case when cod_sc_v1 in (443) then charges end as Bharti_Axa_oracle
,case when cod_sc_v1 in (446) then charges end as Care_health_oracle
,case when cod_sc_v1 in (1167, 167) then charges end as Cersai_oracle
,case when cod_sc_v1 in (1253, 253) then charges end as Equitable_oracle
,case when cod_sc_v1 in (254) then charges end as HDFC_oracle
,case when cod_sc_v1 in (439) then charges end as ICICI_Lombard_oracle
,case when cod_sc_v1 in (442) then charges end as ICICI_prud_oracle
,case when cod_sc_v1 in (150) then charges end as Kotak_oracle
,case when cod_sc_v1 in (1258, 258) then charges end as RCU_oracle
,case when cod_sc_v1 in (1153, 153) then charges end as Service_Charge_oracle 
,case when cod_sc_v1 in (160) then charges end as Shriram_oracle
,case when cod_sc_v1 in (1120, 120) then charges end as Stamping_oracle
,case when cod_sc_v1 in (1122, 122, 267) then charges end as Valuation_oracle
,case when cod_sc_v1 in (1123, 123) then charges end as Legal_oracle
,case when cod_sc_v1 in (151) then charges end as Future_generali_oracle
,case when cod_sc_v1 in (433) then charges end as ICICI_Lombard_GIC_oracle
,case when cod_sc_v1 in (451, 1451) then charges end as ODV_oracle
  from (
select b.cod_acct_no_old, administrative_fee, processing_fees,  a3.*, lac.imd_amount__c from (
select cod_acct_no_v1,cod_sc_v1,sum(case when fee_amount is null then 0 else fee_amount end+
case when gst is null then 0 else gst end+
case when amt_sc is null then 0 else amt_sc end+
case when amt_gst is null then 0 else amt_gst end+
case when arrear_amt is null then 0 else arrear_amt end) as charges ,
/* sum(case when fee_amount is null then 0 else fee_amount end+ */
/* case when gst is null then 0 else gst end+ */
/* case when arrear_amt is null then 0 else arrear_amt end) as charges_deducted, */
sum(case when amt_sc is null then 0 else amt_sc end) as imd_fc

from (

select * from
(
select a1.*,a2.amt_sc, trim(coalesce(a1.nam_sc, a2.nam_sc, a3.nam_sc)) as nam_sc_v1, coalesce(a1.cod_sc, a2.cod_sc, a3.cod_arrear_charge) as cod_sc_v1, a2.amt_gst, a3.arrear_amt,

trim(coalesce(a1.cod_acct_no, a2.cod_acct_no, a3.cod_acct_no)) as cod_acct_no_v1 from 
(select a.cod_acct_no, sum(fee_amount) as fee_amount, sum(b.gst) as gst, a.nam_sc, a.cod_sc from
(
SELECT distinct a.cod_acct_no, a.amt_dedn_tcy as fee_amount, a.ctr_srl, b.cod_sc,b.nam_sc , a.dat_post
FROM 
(select * from DISHA_L1_HARMONIZED.flex_lms.ln_disb_dedn_detl_hist where  activation_flag = 1  )a  
 left join (select * from DISHA_L1_HARMONIZED.flex_lms.ba_sc_code where  activation_flag = 1)b on a.cod_sc_or_prem=b.cod_sc 
 where a.cod_sc_or_prem in (443      ,446      ,167      ,1167               ,253      ,254      ,439      ,442      ,150   ,1123    ,123      
,1258    ,258      ,1153    ,153      ,160      ,1253     ,1120    ,120      ,122        ,1122, 454, 263, 267, 151, 433, 451, 1451
)
  and b.flg_mnt_status=''A'')a
  
  left join 
 
(SELECT distinct cod_acct_no,amt_dedn_tcy as GST,ctr_srl,cod_sc_or_prem,dat_post FROM 
(select * from DISHA_L1_HARMONIZED.flex_lms.ln_disb_dedn_detl_hist where  activation_flag = 1) WHERE cod_sc_or_prem = 555 and ctr_srl in (
SELECT ctr_srl-1 FROM (select * from DISHA_L1_HARMONIZED.flex_lms.ln_disb_dedn_detl_hist where  activation_flag = 1) WHERE cod_sc_or_prem IN (443      ,446      ,167      ,1167    ,253      ,254        ,439      ,442      ,150      ,1123    ,123      
,1258    ,258      ,1153    ,153      ,160      ,1253      ,1120    ,120      ,122       ,1122, 454, 263, 267, 151, 433, 451, 1451)) )b

on a.cod_acct_no=b.cod_acct_no and a.ctr_srl-1=b.ctr_srl  and a.dat_post=b.dat_post group by a.cod_acct_no , a.nam_sc, a.cod_sc  )a1

full outer join 

(select a3.cod_acct_no, a3.nam_sc, a3.cod_sc, sum(a3.amt_sc) as amt_sc, sum(a3.amt_gst) as amt_gst from (
select distinct d.cod_acct_no,b.nam_sc,e.cod_sc, e.amt_sc,e.amt_gst from 
(select * from DISHA_L1_HARMONIZED.flex_lms.LN_IMD_PAYMENT_REGISTER where  activation_flag = 1  )d 
left join (select * from DISHA_L1_HARMONIZED.flex_lms.LN_IMD_PAYMENT_SC_DETAILS where  activation_flag = 1)e on d.cod_receipt_no=e.cod_receipt_no 
 left join (select * from DISHA_L1_HARMONIZED.flex_lms.ba_sc_code where  activation_flag = 1)b on e.cod_sc=b.cod_sc where b.flg_mnt_status = ''A''
)a3 group by a3.cod_acct_no, a3.nam_sc, a3.cod_sc
)a2

on a1.cod_acct_no=a2.cod_acct_no and a1.cod_sc=a2.cod_sc


full outer join 

(select cod_acct_no, sum(amt_arrears_assessed) - sum(amt_arrears_waived) as arrear_amt, cod_arrear_charge, nam_sc from 
(select * from DISHA_L1_HARMONIZED.flex_lms.ln_arrears_table where  activation_flag = 1 )a 
left join (select * from DISHA_L1_HARMONIZED.flex_lms.ba_sc_code where  activation_flag = 1)b 
on b.cod_sc = a.cod_arrear_charge 
where a.cod_arrear_charge in (443      ,446      ,167      ,1167   ,253      ,254      ,439      ,442      ,150      ,1123    ,123      
,1258    ,258      ,1153    ,153      ,160      ,1253      ,1120    ,120        ,122      ,1122, 454, 263, 267, 151, 433, 451, 1451) 
and b.flg_mnt_status = ''A'' group by cod_acct_no,
cod_arrear_charge, nam_sc )a3

on a3.cod_arrear_charge = a1.cod_sc and a3.cod_acct_no = a1.cod_acct_no 


)a 

) group by  cod_acct_no_v1,cod_sc_v1 
)a3 
left join (select * from DISHA_L1_HARMONIZED.flex_lms.ln_x_old_new_acct_xref where activation_flag = 1)b on a3.cod_acct_no_v1 = b.cod_acct_no_new 
left join (select trim(loan_application_id__c) as loan_application_id__c, cast(imd_amount__c as float) as imd_amount__c from DISHA_L1_HARMONIZED.sfdc_los.loan_application__c where lower(ISDELETED) = ''false''
and source_type__c <> ''Legacy Loan''
			and (first_name__c <> ''LoanAppFirstname'' or  first_name__c is null) and upper(stage__c) = ''DISBURSEMENT'' and  activation_flag = 1) lac
			
on lac.loan_application_id__c = a3.cod_acct_no_v1  
left join (select * from DISHA_L2_CURATED.HISTORICAL_LOAD.BOOKING_REPORT where extracted_date = current_date - 1)d on d.new_loan_account_number = a3.cod_acct_no_v1 where a3.cod_acct_no_v1 is not null

) 
) group by 1,2,3, 4, 5
),


af_pf_scpdd_sum as (
select new_loan_number, sum(administrative_fees+ processing_fees+ service_charge_pdd) as af_pf_scpdd_sum_inc_IMD, sum(ODV_Charges+ Valuation_Charges+ Legal_Charges) as Tech_legal_sum from base group by 1 
),


ln_acct_dtls as (
select trim(cod_acct_no) as cod_acct_no, date(dat_first_disb) as first_disbursal_date, cast(case when cod_acct_stat in (10,8) then ''Active'' when cod_acct_stat in (11,1) then ''Closed'' else cast(cod_acct_stat as varchar) end as varchar(10)) as loan_status ,AMT_FACE_VALUE as Loan_Amount
from DISHA_L1_HARMONIZED.flex_lms.ln_acct_dtls where  activation_flag = 1  
)

select a.*, c.af_pf_scpdd_sum_inc_IMD, c.Tech_legal_sum, b.first_disbursal_date, b.loan_status,b.Loan_Amount
from base a 
left join ln_acct_dtls b on b.cod_acct_no = a.new_loan_number
left join af_pf_scpdd_sum c on c.new_loan_number = a.new_loan_number ;

	SELECT COUNT(*) INTO :DUPLICATE_COUNT
  FROM (
    SELECT NEW_LOAN_NUMBER, COUNT(*) AS RECORD_COUNT
    FROM DISHA_L2_CURATED.BASE_MODEL_DATA.NET_CHARGES_test_PREFINAL
    GROUP BY NEW_LOAN_NUMBER
    HAVING COUNT(*) > 1
  );

	
	       -- Hash column list for SHA2 calculation
    SELECT LISTAGG(''S.'' || COLUMN_NAME, '','') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    INTO :HASH_COLUMN_LIST
    FROM DISHA_L2_CURATED.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ''BASE_MODEL_DATA''
    AND TABLE_NAME = ''NET_CHARGES_TEST_PREFINAL''
    AND TABLE_CATALOG = ''DISHA_L2_CURATED'';
    
    -- Insert column list (all columns including metadata columns)
    SELECT LISTAGG(COLUMN_NAME, '','')  WITHIN GROUP (ORDER BY ORDINAL_POSITION)|| '',DATA_SOURCE,START_TIMESTAMP,END_TIMESTAMP,CURRENT_FLAG,ACTIVATION_FLAG''
    INTO :INSERT_COLUMN_LIST
    FROM DISHA_L2_CURATED.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ''BASE_MODEL_DATA''
    AND TABLE_NAME = ''NET_CHARGES_TEST_PREFINAL''
    AND TABLE_CATALOG = ''DISHA_L2_CURATED'';
    
    -- Insert value list (dynamically mapping source columns)
    SELECT LISTAGG(''S.'' || COLUMN_NAME, '','')  WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    INTO :INSERT_VALUE_LIST
    FROM DISHA_L2_CURATED.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ''BASE_MODEL_DATA''
    AND TABLE_NAME = ''NET_CHARGES_TEST_PREFINAL''
    AND TABLE_CATALOG = ''DISHA_L2_CURATED'';   

    SELECT COUNT(*) INTO :TOTAL_ROWS_IN_PREFINAL
    FROM DISHA_L2_CURATED.BASE_MODEL_DATA.NET_CHARGES_TEST_PREFINAL ;
 ----IF TOTAL ROW COUNT 0 , Return Detailed Error
	IF (TOTAL_ROWS_IN_PREFINAL = 0) THEN
					ERROR_MESSAGE := ''PREFINAL QUERY EXECUTED SUCCESSFULLY BUT RETURNED 0 RECORDS. PLEASE VERIFY SOURCE DATA AND JOIN CONDITIONS.'';
					
					INSERT INTO DISHA_MART_ANALYTICS.AUDIT_INFO.DAILY_COUNT_RECON
					(PROCEDURE_NAME, TABLE_NAME, EXECUTION_DATE, PREFINAL_CNT, INSERTED_CNT, UPDATED_CNT, UNCHANGED_CNT,STATUS,START_TIME,END_TIME,REMARKS)
					SELECT :PROC_NAME,:TBL_NAME,DATE(:CURR_TIME),:TOTAL_ROWS_IN_PREFINAL,:ROWS_INSERTED,:ROWS_UPDATED,:ROWS_UNCHANGED,''FAIL'',:CURR_TIME,CURRENT_TIMESTAMP,:ERROR_MESSAGE;                
					
					RETURN ''PREFINAL_FAILED: '' || ERROR_MESSAGE;
	END IF;
   -- If duplicates exist, return detailed error
    IF (DUPLICATE_COUNT > 0) THEN
          SELECT CONCAT(
            ''===== DATA LOADING FAILED =====
'',
            ''Total Rows in Prefinal: '', :TOTAL_ROWS_IN_PREFINAL, ''
'',
            ''Duplicate Rows Found: '', :DUPLICATE_COUNT, ''
'',
            ''ERROR: Unable to load data due to duplicate entries for NEW_LOAN_NUMBER.'',''
''
        ) INTO :FINAL_OUTPUT;
		
		
		INSERT INTO DISHA_MART_ANALYTICS.AUDIT_INFO.DAILY_COUNT_RECON
		(PROCEDURE_NAME, TABLE_NAME, EXECUTION_DATE, PREFINAL_CNT, INSERTED_CNT, UPDATED_CNT, UNCHANGED_CNT,STATUS,START_TIME,END_TIME,REMARKS)
		SELECT :PROC_NAME,:TBL_NAME,DATE(:CURR_TIME),:TOTAL_ROWS_IN_PREFINAL,:ROWS_INSERTED,:ROWS_UPDATED,:ROWS_UNCHANGED,''FAIL'',:CURR_TIME,CURRENT_TIMESTAMP,:FINAL_OUTPUT;
		
		
        RETURN FINAL_OUTPUT;
    END IF;
	

        -- Perform MERGE operation with tracking
    MERGE_STATEMENT :=  CONCAT(
    ''MERGE INTO DISHA_L2_CURATED.BASE_MODEL_DATA.NET_CHARGES_test T
    USING DISHA_L2_CURATED.BASE_MODEL_DATA.NET_CHARGES_test_PREFINAL S
    ON T.NEW_LOAN_NUMBER = S.NEW_LOAN_NUMBER 
		AND T.CURRENT_FLAG = TRUE
    WHEN MATCHED 
    AND SHA2(UPPER(ARRAY_TO_STRING(['', 
    HASH_COLUMN_LIST, 
    ''], ''''|'''')), 256) <> T.ROW_HASH
    THEN UPDATE SET 
        CURRENT_FLAG = FALSE,
        END_TIMESTAMP ='''''',CURR_TIME,'''''',
		T.ACTIVATION_FLAG=0
    WHEN NOT MATCHED THEN
    INSERT ('', 
    INSERT_COLUMN_LIST, ''
    )
    VALUES('', 
    INSERT_VALUE_LIST,'',''''ORACLE'''','''''',CURR_TIME,'''''',NULL,TRUE,1);'');	
	
	--RETURN MERGE_STATEMENT;
	EXECUTE IMMEDIATE :MERGE_STATEMENT;

        -- Count rows affected
         ROWS_INSERTED := (SELECT COUNT(*) FROM DISHA_L2_CURATED.BASE_MODEL_DATA.NET_CHARGES_test WHERE CURRENT_FLAG=1 and START_TIMESTAMP=:CURR_TIME);
         ROWS_UPDATED := (SELECT COUNT(*) FROM DISHA_L2_CURATED.BASE_MODEL_DATA.NET_CHARGES_test WHERE CURRENT_FLAG=0 and END_TIMESTAMP=:CURR_TIME AND ACTIVATION_FLAG=0);
         ROWS_UNCHANGED := :TOTAL_ROWS_IN_PREFINAL - (:ROWS_INSERTED + :ROWS_UPDATED);
		 
		INSERT_STATEMENT:= 
	CONCAT(''INSERT INTO DISHA_L2_CURATED.BASE_MODEL_DATA.NET_CHARGES_test('',
			INSERT_COLUMN_LIST,'')
			SELECT '',
			INSERT_VALUE_LIST,'',''''ORACLE'''','''''',CURR_TIME,'''''',NULL,TRUE,1
			FROM DISHA_L2_CURATED.BASE_MODEL_DATA.NET_CHARGES_test_PREFINAL S
			JOIN DISHA_L2_CURATED.BASE_MODEL_DATA.NET_CHARGES_test T
				ON T.NEW_LOAN_NUMBER = S.NEW_LOAN_NUMBER
				AND T.CURRENT_FLAG = FALSE
				AND T.END_TIMESTAMP ='''''',CURR_TIME,'''''';'');
	--RETURN INSERT_STATEMENT;		
	EXECUTE IMMEDIATE :INSERT_STATEMENT;	
	
	--DROP TEMP TABLE
	--DROP TABLE IF EXISTS DISHA_L2_CURATED.BASE_MODEL_DATA.NET_CHARGES_test_PREFINAL;
	
	
          SELECT CONCAT(
            ''===== DATA LOADING REPORT =====
'',
            ''Process Timestamp: '', :CURR_TIME, ''
'',
            ''Total Rows in Prefinal: '', :TOTAL_ROWS_IN_PREFINAL, ''
'',
            ''Rows Inserted: '', :ROWS_INSERTED, ''
'',
            ''Rows Updated: '', :ROWS_UPDATED, ''
'',
            ''Rows Unchanged: '', :ROWS_UNCHANGED, ''

'',
            ''

'',
            ''Status: SUCCESSFULLY LOADED DATA INTO NET_CHARGES_test''
        ) INTO :FINAL_OUTPUT;
		
		--INSERT AUDIT INFO INTO TABLE
		/*DELETE FROM DISHA_MART_ANALYTICS.AUDIT_INFO.DAILY_COUNT_RECON 
		WHERE  PROCEDURE_NAME=:PROC_NAME
		AND	   EXECUTION_DATE=DATE(:CURR_TIME);*/
		
		INSERT INTO DISHA_MART_ANALYTICS.AUDIT_INFO.DAILY_COUNT_RECON
		(PROCEDURE_NAME, TABLE_NAME, EXECUTION_DATE, PREFINAL_CNT, INSERTED_CNT, UPDATED_CNT, UNCHANGED_CNT,STATUS,START_TIME,END_TIME, REMARKS)
		SELECT :PROC_NAME,:TBL_NAME,DATE(:CURR_TIME),:TOTAL_ROWS_IN_PREFINAL,:ROWS_INSERTED,:ROWS_UPDATED,:ROWS_UNCHANGED,''PASS'',:CURR_TIME,CURRENT_TIMESTAMP, :FINAL_OUTPUT;
		
        RETURN FINAL_OUTPUT;
    ----For Any other exception
	EXCEPTION
        WHEN OTHER THEN
            ERROR_MESSAGE := ''ERROR IN SCD TYPE2 LOAD: '' || SQLERRM || '' (ERROR CODE: '' || sqlcode || '')'';
                        
		INSERT INTO DISHA_MART_ANALYTICS.AUDIT_INFO.DAILY_COUNT_RECON
		(PROCEDURE_NAME, TABLE_NAME, EXECUTION_DATE, PREFINAL_CNT, INSERTED_CNT, UPDATED_CNT, UNCHANGED_CNT,STATUS,START_TIME,END_TIME,REMARKS)
		SELECT :PROC_NAME,:TBL_NAME,DATE(:CURR_TIME),:TOTAL_ROWS_IN_PREFINAL,:ROWS_INSERTED,:ROWS_UPDATED,:ROWS_UNCHANGED,''FAIL'',:CURR_TIME,CURRENT_TIMESTAMP,:ERROR_MESSAGE;	
		
		RETURN ''DATA LOADING FAILED, '' || ERROR_MESSAGE;

      
END';