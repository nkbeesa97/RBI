#!/bin/bash  
source ${MINICONDA_ACTIVATE} ${MINICONDA_ENV}
#set -vx
#--------------------------------------------------------------------------------------
# File      : load_am_tv_squared_xtr.ksh
#
#
# Use       : exec_sf.ksh EXTRACTS NA load_am_tv_squared_xtr.ksh 13
#	      exec_sf.ksh EXTRACTS NA load_am_tv_squared_xtr.ksh [the date number where any records greater than 12 days ago from the current date will be deleted from the table]
#
#
# AUDIT TRAIL
# Modified By
# =======================
# Date         Person            Ver   Description
# -----------  ----------------  ----  ------------------------------------------------
# 08/13/2020   Brendan Koller    1.0   Initial development
# 03/08/2021   Bhushan Newalkar	 2.0   Fixing logical bugs
# 08/15/2021   Priscilla Thomas  3.0   Add Trim to Customer Number for client list
# 10/06/2021   Hannah Zhu        4.0   default value 'Undefined' as BOOKED_SYSCODE_DESC when it's null
# 09/01/2021   Tharini S         5.0   Removed acct_key and current_flg='Y' and added eff_end_dt ='2099-12-31' and sf_use_flg = 1
# 09/20/2021   Priscilla Thomas  6.0   SRDATA-15793: Update Partner Factor Logic Zip Level
#--------------------------------------------------------------------------------------

if [ $# -ne 1 ]
then
	echo
	echo
	echo $0 requires parameters.
	echo Usage:  $0 \<V_MAX_NUMBER_HISTORY\>
	echo
	echo
	exit 1
fi

V_MAX_NUMBER_HISTORY=${1}
V_DATE=$(date '+%Y-%m-%d')


# TD_DATABASE_RBI_EDS="${ENVR}_RBI.EDSV"
TD_DATABASE_RBI_EDS="TDSYNC"



echo "TD_DATABASE_RBI_EDS = ${TD_DATABASE_RBI_EDS}"


python << ACTUAL_EOF
#!/usr/bin/python3

import os
import sys
from dotenv import *
from np_arg_load import *
from npcommon import *
from np_fastload import *
from np_fexport import *

def main(argv):
  global args
  load_dotenv(find_dotenv())
  args = loadArgs(argv)
  #--NP-SQL-EXTRACT-SPOT1
  FormatOptions.width = 1000
  #--.OS echo
  #--.OS echo date
  #--.OS echo
  #--.OS echo;
  #/*=======================================================================================================*/
  #-- Step 0: Delete any old data that is no longer necessary to hold in the extract table
  #/*=======================================================================================================*/
  #--- For this extract table, we will hold 12 weeks worth of data (a quarter's worth). Any data older than 12 weeks will be removed
  #-- Use BIG ETL Warehouse
  executeSql([], [
    (f"""USE WAREHOUSE ${SF_WH_AM_ETL_BIG}""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  executeSql([], [
    (f"""CREATE OR REPLACE temporary TABLE VT_XTR_ETL_PRCS_DT_DELETE AS
(
select ETL_PRCS_DT FROM (
select ETL_PRCS_DT
, rank() OVER( ORDER BY ETL_PRCS_DT DESC ) as RNK
FROM ${TD_DATABASE_AM_XTR}.AM_TV_SQUARED_EXTRACT
GROUP BY ETL_PRCS_DT ) A
WHERE RNK >= ${V_MAX_NUMBER_HISTORY}
)""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-- CREATE_MULTISET_OPTION - Remove MULTISET create table option
  #-- CREATE_TABLE_TYPE_OPTION - Replace VOLATILE with TEMPORARY
  #-- CTAS_WITH_DATA - Remove WITH DATA clause
  #-- TABLE_COMMIT_OPTIONS - Remove ON COMMIT PRESERVE ROWS
  #-- TABLE_INDEX - Remove table index options
  #/*=======================================================================================================*/
  #-- Step 1: Create a set table to hold unique customer ID's (codes are from a spreadhseet from Frequence)
  #/*=======================================================================================================*/
  #--- Step 1-1: Retrieve the Client Data
  executeSql([], [
    (f"""CREATE OR REPLACE temporary TABLE TMP_CLIENT_LIST AS (
select e.cust_key AS Customer_Key
	  ,c.cust_nm AS Client_Name
	  ,TRIM(c.cust_nbr) AS cust_nbr
	  ,e.eclipse_regn_nm
	  ,(c.RBI_SRC_CD||'-'|| SUBSTR(TRIM(c.CUST_NBR), 1, 10)) AS Client_ID
	  ,e.cust_id as cust_id
	from (select 
			cust_key
			,cust_id
			,cust_nm
			,TRIM(cust_nbr) AS cust_nbr
      ,case
			when eclipse_regn_nm='STLOUIS' then 'SAINTLOUIS'
		else eclipse_regn_nm
		end as eclipse_regn_nm
       from ${TD_DATABASE_AM_BI}.AM_EDA_CUSTOMER_DIM
       ) e
join ${TD_DATABASE_MEDIA_EDS}.RDM_CUSTOMER c
on TRIM(e.cust_nbr)=TRIM(c.cust_nbr)
and e.eclipse_regn_nm=c.rbi_src
and c.eff_end_dt='2099-12-31'
join (select distinct el.cstm_trfc_id,acc.acct_id,acc.acct_nm
	from  ${TD_DATABASE_RBI_EDS}.RBI_SF_ACCOUNT acc
	join ${TD_DATABASE_RBI_EDS}.RBI_SF_TRAFFIC_ID_ELEMENT el
	on acc.acct_id = el.cstm_acct_id
	and el.current_flg ='Y'
	and cstm_trfc_typ ='Account'
	and (acc.eff_end_dt = '2099-12-31' or acc.eff_end_dt='2099/12/31')
	and acc.sf_use_flg = 1
	join (select distinct acct_id
	      from ${TD_DATABASE_RBI_EDS}.RBI_SF_OPPORTUNITY
	where src_del_flg=0 AND UPPER(DESC_TV_SQUARED) LIKE '%#SITETRAFFICLIFT%' )o
	on acc.acct_id=o.acct_id
)sf
on TRIM(sf.cstm_trfc_id)=TRIM(e.cust_nbr)
)""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-- CREATE_MULTISET_OPTION - Remove MULTISET create table option
  #-- CREATE_TABLE_TYPE_OPTION - Replace VOLATILE with TEMPORARY
  #-- CTAS_WITH_DATA - Remove WITH DATA clause
  #-- TABLE_COMMIT_OPTIONS - Remove ON COMMIT PRESERVE ROWS
  #-- TABLE_INDEX - Remove table index options
  #-- UPPER_WHERECLAUSE - In where clause, if an equal expression is detected - UPPER is appended to both sides to make it case sensitive
  #-- COLLECT STATISTICS COLUMN (Customer_Key, Client_ID) ON TMP_CLIENT_LIST;
  #-- COLLECT_STATS - Comment out COLLECT STATISTICS. requires further review.
  #--- Step 1-2: Return last week dates
  executeSql([], [
    (f"""CREATE OR REPLACE temporary TABLE TMP_DATES AS (
select distinct B.CLNDR_WEEK_START_DT ,B.CLNDR_WEEK_END_DT  from ${TD_DATABASE_AM_BI}.AM_CALENDAR_DIM A
			JOIN ${TD_DATABASE_AM_BI}.AM_CALENDAR_DIM B
			ON A.CLNDR_WEEK_START_DT - INTERVAL '1  DAY'=B.CLNDR_WEEK_END_DT
			where A.CLNDR_DT =CURRENT_DATE
)""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-- CREATE_MULTISET_OPTION - Remove MULTISET create table option
  #-- CREATE_TABLE_TYPE_OPTION - Replace VOLATILE with TEMPORARY
  #-- CTAS_WITH_DATA - Remove WITH DATA clause
  #-- EXPR_INTERVAL - Moving QUALIFIER as a part of STRING LITERAL
  #-- SEL_STATEMENT - Replace SEL with SELECT
  #-- TABLE_COMMIT_OPTIONS - Remove ON COMMIT PRESERVE ROWS
  #-- TABLE_INDEX - Remove table index options
  #-- COLLECT STATISTICS COLUMN (CLNDR_WEEK_START_DT, CLNDR_WEEK_END_DT) ON TMP_DATES;
  #-- COLLECT_STATS - Comment out COLLECT STATISTICS. requires further review.
  #/*=======================================================================================================*/
  #-- Step 2: Get AM_AD_EVENT_FACT data for the dates and client list
  #/*=======================================================================================================*/
  #--DROP TABLE VT_AD_EVENT_FACTOR;
  executeSql([], [
    (f"""CREATE OR REPLACE temporary TABLE VT_AD_EVENT_FACTOR AS (
	SELECT
		 AD_EVNT_KEY
		,F.CUST_KEY
		,RUH.DMA_CD_KEY
		,O.CTRC_NBR AS CMPGN_ID
		,F.STN_KEY
		,S2.SYSCODE_KEY AS SOLD_SYSCODE_KEY
		,F.SPOT_KEY
		,F.SPOT_RATE_NBR
		,F.ECLIPSE_REGN_NM
		,F.AD_EVNT_START_DT
		,F.AD_EVNT_START_TS
		,F.AD_EVNT_END_TS
		,F.AD_EVNT_DUR_CNT
		,F.AD_EVNT_START_DT - 1 AS AD_EVNT_START_DT_MINUS_ONE
		,F.AD_EVNT_START_DT + 1 AS AD_EVNT_START_DT_PLUS_ONE
		,EXTRACT(HOUR FROM F.AD_EVNT_START_TS) AS HOUR_OF_DAY
		,(HOUR_OF_DAY + 24 - 1) % 24 AS HOUR_OF_DAY_MINUS_ONE
		,(HOUR_OF_DAY + 1) % 24 AS HOUR_OF_DAY_PLUS_ONE
	FROM
	(select
		AD_EVNT_KEY,
		AD_EVNT_START_DT,
		ORD_NBR,
		CUST_KEY,
		SPOT_KEY,
		SPOT_RATE_NBR,
		AD_EVNT_DUR_CNT,
		STN_KEY,
		SYSCODE,
		ECLIPSE_REGN_NM,
		AD_EVNT_START_TS,
		AD_EVNT_END_TS,
		REGN_KEY
	 FROM ${TD_DATABASE_AM_BI}.AM_AD_EVENT_FACT
	 WHERE AD_EVNT_START_DT BETWEEN (SELECT CLNDR_WEEK_START_DT FROM TMP_DATES) AND (SELECT CLNDR_WEEK_END_DT FROM TMP_DATES)
	) F
	JOIN TMP_CLIENT_LIST c_list
	  ON c_list.Customer_Key = F.CUST_KEY
	JOIN ${TD_DATABASE_AM_BI}.AM_ORDER_EDA_FACT O
	  ON F.ORD_NBR=O.ORD_NBR
	 AND F.ECLIPSE_REGN_NM=O.ECLIPSE_REGN_NM
	JOIN ${TD_DATABASE_AM_BI}.AM_REGION_EDA_DIM RE
	  ON F.REGN_KEY = RE.REGION_KEY
	  AND F.ECLIPSE_REGN_NM = RE.ECLIPSE_REGN_NM
	JOIN ${TD_DATABASE_AM_BI}.AM_SYSCODE_DIM S
	  ON S.SYSCODE=F.SYSCODE
	 AND F.AD_EVNT_START_DT BETWEEN S.EFF_START_DT AND S.EFF_END_DT
	JOIN ${TD_DATABASE_AM_BI}.AM_SYSCODE_DIM S2
	  ON S2.SYSCODE=COALESCE(RE.SYSCODE,F.SYSCODE)
	 AND F.AD_EVNT_START_DT BETWEEN S2.EFF_START_DT AND S2.EFF_END_DT
	JOIN ${TD_DATABASE_AM_BI}.AM_RETAIL_UNIT_HIERARCHY_DIM RUH
	  ON S.SYSCODE_KEY  = RUH.SYSCODE_KEY
)""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-- COL_ALIAS_IN_EXPR - Review column alias reference
  #-- CREATE_TABLE_TYPE_OPTION - Replace VOLATILE with TEMPORARY
  #-- CTAS_WITH_DATA - Remove WITH DATA clause
  #-- EXPR_FORMAT - Convert expression FORMAT/CAST_AS_FORMAT to TO_CHAR/TO_DATE
  #-- EXPR_MOD - Change expression MOD to %
  #-- SEL_STATEMENT - Replace SEL with SELECT
  #-- TABLE_COMMIT_OPTIONS - Remove ON COMMIT PRESERVE ROWS
  #-- TABLE_INDEX - Remove table index options
  #/*=======================================================================================================*/
  #-- Step 3: Get AM_PROGRAM_TUNING_EVENT_FACT data for the dates
  #/*=======================================================================================================*/
  #-- Not Needed

  #/*=======================================================================================================*/
  #-- Step 4: Get O&O impressions data for the dates and client list from PATEF
  #/*=======================================================================================================*/
  executeSql([], [
    (f"""CREATE OR REPLACE temporary TABLE VT_OWN_IMPS AS(
	SELECT
		 PATEF.CUST_KEY
		,T.CTRC_NBR AS CMPGN_ID
		,PATEF.ECLIPSE_REGN_NM
		,PATEF.DMA_CD_KEY
		,PATEF.STN_KEY
		,AD_EVENT.SOLD_SYSCODE_KEY AS SYSCODE_KEY
		,PATEF.PRGM_KEY
		,PATEF.SPOT_KEY
		,PATEF.SPOT_RATE_NBR
		,PATEF.AD_EVNT_START_DT
		,PATEF.AD_EVNT_START_TS
		,PATEF.AD_EVNT_KEY
		,PATEF.AD_EVNT_DUR_CNT
		,Sum(Cast(ad_tuning_evnt_dur_cnt AS FLOAT))/30 AS IMPS
	FROM ${TD_DATABASE_AM_BI}.AM_PROGRAM_AD_TUNING_EVENT_FACT PATEF
	JOIN TMP_CLIENT_LIST c_list
	  	ON c_list.Customer_Key = PATEF.CUST_KEY
	JOIN VT_AD_EVENT_FACTOR AD_EVENT
		ON PATEF.AD_EVNT_KEY = AD_EVENT.AD_EVNT_KEY
		AND PATEF.STN_KEY = AD_EVENT.STN_KEY
		AND PATEF.AD_EVNT_START_DT = AD_EVENT.AD_EVNT_START_DT
	JOIN ${TD_DATABASE_AM_BI}.AM_ORDER_EDA_FACT T
		ON T.ORD_NBR=PATEF.ORD_NBR
		AND T.ECLIPSE_REGN_NM =PATEF.ECLIPSE_REGN_NM
	WHERE PATEF.AD_EVNT_START_DT BETWEEN (SELECT CLNDR_WEEK_START_DT FROM TMP_DATES)  AND (SELECT CLNDR_WEEK_END_DT FROM TMP_DATES)
	GROUP BY
		 PATEF.CUST_KEY
		,T.CTRC_NBR
		,PATEF.ECLIPSE_REGN_NM
		,PATEF.DMA_CD_KEY
		,PATEF.STN_KEY
		,SOLD_SYSCODE_KEY
		,PATEF.PRGM_KEY
		,PATEF.SPOT_KEY
		,PATEF.SPOT_RATE_NBR
		,PATEF.AD_EVNT_START_DT
		,PATEF.AD_EVNT_START_TS
		,PATEF.AD_EVNT_DUR_CNT
		,PATEF.AD_EVNT_KEY
)""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return

  #/*=======================================================================================================*/
  #-- Step 5: AEF data for Partner Factor syscodes
  #/*=======================================================================================================*/
  print("Step 5.1: Calculate-PARTNER-FACTOR-IMP")
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE SPOT
        AS
      	SELECT
		    AD_EVNT_KEY
		  , c_list.Client_Name AS CUST_NM
		  , c_list.CUST_ID     AS CUST_ID 
		  , c_list.CUST_NBR    AS CUST_NBR
		  , O.CTRC_NBR
		  , O.ORD_NBR
		  , O.ECLIPSE_REGN_NM
		  , H.DMA_CD_KEY
		  , Coalesce(R.SYSCODE, AD.SYSCODE) AS S_SYSCODE
		  , AD.SYSCODE AS A_SYSCODE
		  , AD.SPOT_KEY
		  , AD.NTWRK_KEY
		  , AD.STN_KEY
		  , AD.AD_EVNT_START_DT
		  , AD.AD_EVNT_START_TS
		  , AD.AD_EVNT_END_TS
		  , AD.SPOT_RATE_NBR
		  , AD.AD_EVNT_DUR_CNT
	    FROM ${TD_DATABASE_AM_BI}.AM_AD_EVENT_FACT AS AD
		JOIN TMP_CLIENT_LIST c_list
	      ON c_list.Customer_Key = AD.CUST_KEY
	    JOIN ${TD_DATABASE_AM_BI}.AM_ORDER_EDA_FACT O
		    ON AD.ORD_NBR = O.ORD_NBR
		   AND AD.ECLIPSE_REGN_NM = O.ECLIPSE_REGN_NM
	    JOIN ${TD_DATABASE_AM_BI}.AM_SYSCODE_DIM S
		    ON AD.SYSCODE = S.SYSCODE
		   AND AD.AD_EVNT_START_DT BETWEEN S.EFF_START_DT AND S.EFF_END_DT
		   AND Coalesce(S.SYSCODE_TYP_NM,'UNKNOWN') <> 'ADVANCED ADVERTISING'	-- EXCLUDING AA PLACEHOLDER SPOTS
	    JOIN ${TD_DATABASE_AM_BI}.AM_RETAIL_UNIT_HIERARCHY_DIM AS H
        ON S.SYSCODE_KEY = H.SYSCODE_KEY
	    LEFT JOIN ${TD_DATABASE_AM_BI}.AM_REGION_EDA_DIM AS R
		    ON AD.REGN_KEY = R.REGION_KEY
		   AND AD.ECLIPSE_REGN_NM = R.ECLIPSE_REGN_NM
	   WHERE VRFY_FLG = 'Y'
	     AND AD.AD_EVNT_START_DT BETWEEN (SELECT CLNDR_WEEK_START_DT FROM TMP_DATES) AND (SELECT CLNDR_WEEK_END_DT FROM TMP_DATES)""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
#-----5.2-----
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE ZF
      AS SELECT DISTINCT
	        BRDCST_QTR_START_SKEY   AS r_qtr,
	        ZIP_KEY                 AS zip_cd,
	        DONOR_ZIP_KEY           AS zip_donor,
	        SOLD_SYSCODE            AS s_syscode,
	        AIRED_SYSCODE           AS a_syscode,
	        DSYS                    AS da_syscode,
	        ZIP_FACTOR
        FROM ${TD_DATABASE_AM_EDS}.AM_PARTNER_FACTOR_ZIPWEIGHT zw
        QUALIFY Row_Number() Over(PARTITION BY ZIP_KEY, DONOR_ZIP_KEY, SOLD_SYSCODE, AIRED_SYSCODE, DSYS, ZIP_FACTOR ORDER BY BRDCST_QTR_START_SKEY DESC) = 1""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
#-----5.3-----    
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE TT_SUBS AS
    SELECT s.sbsc_guid_key, 
           z.zip_cd, 
           s.eff_start_dt, 
           s.eff_end_dt
    FROM ${TD_DATABASE_AM_BI}.AM_SUBSCRIBER_DIM s
     LEFT JOIN ${TD_DATABASE_AM_BI}.AM_ZIP_DIM z
         ON s.zip_key = z.zip_key	-- Zip Key could be -99 (missing)
      WHERE s.eff_start_dt <=  (SELECT CLNDR_WEEK_END_DT FROM TMP_DATES)  and s.eff_end_dt >= (SELECT CLNDR_WEEK_START_DT FROM TMP_DATES)
   QUALIFY ROW_NUMBER() OVER(PARTITION BY s.sbsc_guid_key ORDER BY s.eff_end_dt DESC) = 1""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
#-----5.4-----    
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE TT_LIN
      AS
        SELECT 
			L.AD_EVNT_KEY
			, C.CUST_NBR
			, C.CTRC_NBR
			, L.ORD_NBR
			, L.ECLIPSE_REGN_NM
			, L.SYSCODE_DMA_CD_KEY AS DMA_CD_KEY
			, AD.SYSCODE
			, AD.REGN_KEY
			, S.ZIP_CD
			, L.SPOT_KEY
			, L.NTWRK_KEY
			, L.STN_KEY
			, L.PRGM_KEY
			, L.AD_EVNT_START_DT AS IMP_DT
			, Sum(Cast(AD_TUNING_EVNT_DUR_CNT AS FLOAT))/30 AS IMP
        FROM ${TD_DATABASE_AM_BI}.AM_PROGRAM_AD_TUNING_EVENT_FACT AS L
	    JOIN (SELECT DISTINCT CUST_ID, CUST_NBR, CTRC_NBR, ORD_NBR, ECLIPSE_REGN_NM FROM SPOT) AS C 
			ON L.ORD_NBR = C.ORD_NBR 
			AND L.ECLIPSE_REGN_NM = C.ECLIPSE_REGN_NM
		JOIN TMP_CLIENT_LIST c_list
	        ON c_list.Cust_ID = C.CUST_ID  
		JOIN ${TD_DATABASE_AM_BI}.AM_AD_EVENT_NETWORK_FACT AS AD
			ON L.AD_EVNT_KEY = AD.AD_EVNT_KEY
			AND L.NTWRK_KEY = AD.NTWRK_KEY
			AND L.AD_EVNT_START_DT = AD.AD_EVNT_START_DT
		LEFT JOIN TT_SUBS S 
			ON L.SBSC_GUID_KEY = S.SBSC_GUID_KEY 
		WHERE L.AD_EVNT_START_DT BETWEEN (SELECT CLNDR_WEEK_START_DT FROM TMP_DATES) AND (SELECT CLNDR_WEEK_END_DT FROM TMP_DATES)
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-----5.5-----
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE TT_CMPGN_VIEWERS
      AS
		SELECT
			  AD_EVNT_KEY
			, CUST_NBR
			, CTRC_NBR
			, ORD_NBR
			, L.ECLIPSE_REGN_NM
			, DMA_CD_KEY
			, R.SYSCODE S_SYSCODE	-- SOLD
			, L.SYSCODE A_SYSCODE	-- AIRED
			, ZIP_CD
			, SPOT_KEY
			, NTWRK_KEY
			, STN_KEY
			, PRGM_KEY
			, IMP_DT
			, Sum(IMP) AS IMP
		FROM TT_LIN L
		LEFT JOIN ${TD_DATABASE_AM_BI}.AM_REGION_EDA_DIM AS R
			ON L.REGN_KEY = R.REGION_KEY
			AND L.ECLIPSE_REGN_NM = R.ECLIPSE_REGN_NM
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
#-----5.6-----    
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE SPOT_IMP AS
	  SELECT
		  S.CUST_NM
    	, S.CUST_NBR
		, S.CTRC_NBR
		, S.ORD_NBR
		, S.ECLIPSE_REGN_NM
		, Coalesce(CVR.DMA_CD_KEY, S.DMA_CD_KEY) AS DMA_CD_KEY
		, S.AD_EVNT_START_DT
		, S.S_SYSCODE
		, S.A_SYSCODE
		, CASE WHEN I.ORD_NBR IS NOT NULL THEN 1 ELSE 0 END AS IMP_FL
		, Coalesce(CVR.ZIP_CD, I.ZIP_CD,'N/A') AS ZIP_CD	-- Zip info has to come from viewership
		, S.SPOT_KEY
		, S.NTWRK_KEY
		, S.STN_KEY
		, Coalesce(CVR.PRGM_KEY,-99) AS PRGM_KEY	-- Program info has to come from viewership		
		, S.AD_EVNT_START_TS
		, S.AD_EVNT_END_TS
		, S.AD_EVNT_KEY -- Spot at Syscode level, could duplicate across zips and programs
		, S.SPOT_RATE_NBR
		, S.AD_EVNT_DUR_CNT
		, Sum(Coalesce(CVR.IMP,0)) AS IMPS
	  FROM SPOT S
	  LEFT JOIN (SELECT DISTINCT ORD_NBR, ECLIPSE_REGN_NM, S_SYSCODE, A_SYSCODE, ZIP_CD, IMP_DT FROM TT_CMPGN_VIEWERS WHERE IMP > 0) I	-- Determine if we have imp data on a syscode/day
		  ON S.ORD_NBR = I.ORD_NBR
		  AND S.ECLIPSE_REGN_NM = I.ECLIPSE_REGN_NM
		  AND S.S_SYSCODE = I.S_SYSCODE
		  AND S.A_SYSCODE = I.A_SYSCODE
		  AND S.AD_EVNT_START_DT = I.IMP_DT
	  LEFT JOIN TT_CMPGN_VIEWERS CVR	-- Get actual imp data per spot (ad event)
		  ON S.AD_EVNT_KEY = CVR.AD_EVNT_KEY
		  AND S.STN_KEY = CVR.STN_KEY
		  AND I.ZIP_CD = CVR.ZIP_CD
	  WHERE S.A_SYSCODE IS NOT NULL	-- Can't estimate where Aired Syscode is null
	  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return 
  #-----5.7-----          
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE O_SPOTS
    AS
      SELECT ORD_NBR, ECLIPSE_REGN_NM, S_SYSCODE, A_SYSCODE, ZIP_CD, Count(DISTINCT AD_EVNT_KEY) AS ZIP_SPOTS
		  FROM SPOT_IMP
		  WHERE IMPS > 0 	-- HAS IMP DATA
		  GROUP BY 1,2,3,4,5""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return 
#-----5.8-----
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE P_SPOTS
    AS
    SELECT DISTINCT ORD_NBR, ECLIPSE_REGN_NM, S_SYSCODE, A_SYSCODE
		  FROM SPOT_IMP
		  WHERE IMP_FL = 0	-- HAS NO IMP DATA
    """,
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return   
#-----5.9-----
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE ZIP_DONOR 
    AS (
		SELECT DISTINCT
			P.ORD_NBR,
			P.ECLIPSE_REGN_NM,
			P.S_SYSCODE,
			P.A_SYSCODE,
			O.S_SYSCODE AS DS_SYSCODE,
			O.A_SYSCODE AS DA_SYSCODE,
			O.ZIP_CD AS D_ZIP,
			O.ZIP_SPOTS
		FROM P_SPOTS P 
		JOIN O_SPOTS O 
			ON P.ORD_NBR = O.ORD_NBR 
			AND P.ECLIPSE_REGN_NM = O.ECLIPSE_REGN_NM
		JOIN ZF 
			ON ZF.S_SYSCODE = P.S_SYSCODE 
			AND ZF.A_SYSCODE = P.A_SYSCODE 
			AND ZF.DA_SYSCODE = O.A_SYSCODE 
			AND ZF.ZIP_DONOR = O.ZIP_CD
    )""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return   
#-----5.10-----
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE SYS_DONOR
    AS
	    SELECT 
		    ORD_NBR,
		    ECLIPSE_REGN_NM,
		    S_SYSCODE,
		    A_SYSCODE,
		    DS_SYSCODE,
		    DA_SYSCODE,
		    WT,
		  Row_Number() Over (PARTITION BY ORD_NBR, ECLIPSE_REGN_NM, S_SYSCODE, A_SYSCODE ORDER BY WT DESC) AS D_SYS_RNK
	    FROM
	    (
		    SELECT ORD_NBR,	ECLIPSE_REGN_NM, S_SYSCODE, A_SYSCODE, DS_SYSCODE, DA_SYSCODE, Sum(ZIP_SPOTS) AS WT
		    FROM ZIP_DONOR
		    GROUP BY 1,2,3,4,5,6
	    ) T""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return  
#-----5.11-----
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE
        DONOR_SYS_ZIP AS (
		    SELECT DISTINCT
			    ORD_NBR, ECLIPSE_REGN_NM, 
			    SD.S_SYSCODE, SD.A_SYSCODE, ZF.ZIP_CD, 
			    SD.DS_SYSCODE, SD.DA_SYSCODE, ZF.ZIP_DONOR AS D_ZIP_CD, ZF.ZIP_FACTOR AS FACTOR
		    FROM SYS_DONOR SD
		    JOIN ZF ON SD.S_SYSCODE = ZF.S_SYSCODE AND SD.A_SYSCODE = ZF.A_SYSCODE 
			    AND SD.D_SYS_RNK = 1 AND SD.DA_SYSCODE = ZF.DA_SYSCODE)""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return  
#-----5.12-----
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE
      PARTNER_SPOT_ZIP AS
	    SELECT 
		    SI.ORD_NBR,
		    SI.ECLIPSE_REGN_NM,
		    SI.DMA_CD_KEY,
		    SI.SPOT_KEY,
		    SI.NTWRK_KEY,
		    SI.STN_KEY,
		    SI.AD_EVNT_START_DT,
		    SI.AD_EVNT_START_TS,
		    SI.AD_EVNT_END_TS,
		    SI.S_SYSCODE,
		    SI.A_SYSCODE,	
		    SI.AD_EVNT_KEY,	-- Duplicate to zips based on donor assignment
		    DSZ.DS_SYSCODE,
		    DSZ.DA_SYSCODE,
		    DSZ.ZIP_CD,
		    DSZ.D_ZIP_CD,
		    SI.PRGM_KEY,
		    SI.SPOT_RATE_NBR,
		    SI.AD_EVNT_DUR_CNT,
	  	    DSZ.FACTOR
	    FROM SPOT_IMP SI
	    JOIN DONOR_SYS_ZIP DSZ
		    ON SI.ORD_NBR = DSZ.ORD_NBR
		    AND SI.ECLIPSE_REGN_NM = DSZ.ECLIPSE_REGN_NM
		    AND SI.S_SYSCODE = DSZ.S_SYSCODE
		    AND SI.A_SYSCODE = DSZ.A_SYSCODE
	    WHERE SI.IMP_FL = 0
	      AND AD_EVNT_START_DT BETWEEN (SELECT CLNDR_WEEK_START_DT FROM TMP_DATES) AND (SELECT CLNDR_WEEK_END_DT FROM TMP_DATES)""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return  
#----Program Key Logic is removed for TV Squared---
#-----5.14----- 
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE PARTNER_SPOT_ZIP_PRGM 
    AS (
	    SELECT 
		    ORD_NBR,
		    ECLIPSE_REGN_NM,
		    PSZ.DMA_CD_KEY,
		    SPOT_KEY,
		    NTWRK_KEY,
		    PSZ.STN_KEY,
		    AD_EVNT_START_DT,
		    AD_EVNT_START_TS,
		    AD_EVNT_END_TS,
		    S_SYSCODE,
		    PSZ.A_SYSCODE,	
		    AD_EVNT_KEY,
		    DS_SYSCODE,
		    DA_SYSCODE,
		    PSZ.ZIP_CD,
		    D_ZIP_CD,
		    FACTOR,
		    PRGM_KEY,
		    SPOT_RATE_NBR,
		    AD_EVNT_DUR_CNT,
			AD_EVNT_START_TS AS AD_TUNING_EVNT_START_TS,
            AD_EVNT_END_TS AS AD_TUNING_EVNT_END_TS,
		    ${TD_DATABASE_AM_BI}.Duration(AD_TUNING_EVNT_START_TS, AD_TUNING_EVNT_END_TS) AS AD_OVERLAP_SEC
	    FROM PARTNER_SPOT_ZIP PSZ
	    QUALIFY Row_Number() Over(PARTITION BY AD_EVNT_KEY, NTWRK_KEY, PSZ.ZIP_CD ORDER BY AD_OVERLAP_SEC DESC) = 1	-- If an ad played between two programs
  )""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return      
  #-----5.19-----     
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE TT_PARTNER_IMPS 
      AS (
	      WITH DONOR_IMPS AS (
		      SELECT CUST_NM, CUST_NBR, SI.ECLIPSE_REGN_NM, CTRC_NBR, SI.ORD_NBR, SI.A_SYSCODE, ZIP_CD, Count(DISTINCT AD_EVNT_KEY) SPOTS, Sum(IMPS) IMPS
		      FROM SPOT_IMP SI
		      WHERE IMP_FL = 1 -- HAS IMP DATA -> O&O
      		GROUP BY 1,2,3,4,5,6,7
	        )
	    SELECT 
		    D.CUST_NM,
		    D.CUST_NBR,
		    P.AD_EVNT_START_DT AS AD_EVENT_START_DT,
		    P.AD_EVNT_START_TS,		    
		    D.CTRC_NBR AS CMPGN_ID,
		    P.ORD_NBR,
		    P.ECLIPSE_REGN_NM,
		    P.SPOT_KEY,
		    P.NTWRK_KEY,
		    P.STN_KEY,
		    P.PRGM_KEY,
		    P.DMA_CD_KEY,
		    P.S_SYSCODE,
		    P.A_SYSCODE,
		    P.DS_SYSCODE,
		    P.DA_SYSCODE,
		    P.ZIP_CD,
		    P.D_ZIP_CD,
		    D.SPOTS,
		    P.FACTOR,
		    D.IMPS,
		    P.SPOT_RATE_NBR,	
		    P.AD_EVNT_DUR_CNT,
		    Count(DISTINCT P.AD_EVNT_KEY) AS P_SPOTS,	-- ACTUAL AIRED SPOTS IN PARTNER ZONE
		    D.IMPS * P.FACTOR * (P_SPOTS / Cast(D.SPOTS AS FLOAT)) AS P_IMPS
	    FROM PARTNER_SPOT_ZIP_PRGM P 
	    JOIN DONOR_IMPS D
		    ON P.ORD_NBR = D.ORD_NBR
		    AND P.ECLIPSE_REGN_NM = D.ECLIPSE_REGN_NM
		    AND P.DA_SYSCODE = D.A_SYSCODE	
		    AND P.D_ZIP_CD = TRY_TO_NUMBER(D.ZIP_CD)
    	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
    )""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-----5.20-----     
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE TT_PARTNER_IMPS_DONE 
	 AS
	 SELECT 
		CUST_NM,
		CUST_NBR,
		AD_EVENT_START_DT,
		AD_EVNT_START_TS,
		CMPGN_ID,
		ORD_NBR,
		ECLIPSE_REGN_NM,
		SPOT_KEY,
		NTWRK_KEY,
		STN_KEY,
		PRGM_KEY,
		DMA_CD_KEY,
		S_SYSCODE,
		A_SYSCODE,
		DS_SYSCODE,
		DA_SYSCODE,
		ZIP_CD,
        D_ZIP_CD,
        SPOTS,
        P_SPOTS,
        FACTOR,
		SPOT_RATE_NBR,
		AD_EVNT_DUR_CNT,
		IMPS,
        P_IMPS
	FROM TT_PARTNER_IMPS 
 	QUALIFY Row_Number() Over(PARTITION BY CUST_NM, CUST_NBR, AD_EVENT_START_DT, AD_EVNT_START_TS, CMPGN_ID, ORD_NBR, ECLIPSE_REGN_NM, SPOT_KEY, NTWRK_KEY, PRGM_KEY, DMA_CD_KEY, A_SYSCODE,
		DS_SYSCODE, DA_SYSCODE,	ZIP_CD,D_ZIP_CD, SPOTS, P_SPOTS, FACTOR, IMPS, P_IMPS ORDER BY SPOTS DESC) = 1""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #/*=======================================================================================================*/
  #-- Step 6: Loading O&O and Partner syscode impressions data into extract table
  #/*=======================================================================================================*/
  executeSql([], [
    (f"""CREATE OR REPLACE TEMPORARY TABLE TMP_PULL_SPOT_INFO AS (
 		SELECT PRGM_KEY, replace(rtrim((LISTAGG(cast(PRGM_GENRE_CD as varchar(25)), '|') within group(order by PRGM_GENRE_CD) ::varchar(10000)), '|'), '| ', '|') as Genre, PRGM_NM
 		from ( select distinct a.PRGM_KEY, b.PRGM_GENRE_CD,PRGM_NM
 		FROM VT_OWN_IMPS                                       a
 		JOIN ${TD_DATABASE_AM_BI}.AM_PROGRAM_GENRE_DIM         b
 		 on a.PRGM_KEY = b.PRGM_KEY
 		JOIN ${TD_DATABASE_AM_BI}.AM_PROGRAM_DIM               x
 		 on a.PRGM_KEY = x.PRGM_KEY)A
 		GROUP BY 1,3
 )""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  executeSql([], [
    (f"""begin transaction""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-- TRANS_CONTROL - Convert BT to BEGIN TRANSACTION
  executeSql([], [
    (f"""DELETE FROM ${TD_DATABASE_AM_XTR}.AM_TV_SQUARED_EXTRACT WHERE ETL_PRCS_DT IN (SELECT ETL_PRCS_DT FROM VT_XTR_ETL_PRCS_DT_DELETE)""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  executeSql([], [
    (f"""DELETE FROM ${TD_DATABASE_AM_XTR}.AM_TV_SQUARED_EXTRACT WHERE UPPER(ETL_PRCS_DT) = UPPER('${V_DATE}')""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-- UPPER_WHERECLAUSE - In where clause, if an equal expression is detected - UPPER is appended to both sides to make it case sensitive
  executeSql([], [
    (f"""INSERT INTO ${TD_DATABASE_AM_XTR}.AM_TV_SQUARED_EXTRACT
 (
 CAMPAIGN_ID,
 CLIENT_ID,
 STATION,
 AD_DATE,
 AD_TIME,
 AD_SECONDS,
 AD_COST,
 AUDIENCE_IMPRESSIONS,
 CREATIVE_NM,
 MARKET_NM,
 BOOKED_SYSCODE,
 BOOKED_SYSCODE_DESC,
 GROSS_CPM,
 PROGRAM,
 GENRE,
 ETL_PRCS_DT,
 LD_SEQ_NBR,
 CRE_TS
 )
		SELECT
			OWN.CMPGN_ID AS CAMPAIGN_ID,
			CUST.CLIENT_ID,
			STN_NM.STN_NORM_NM AS STATION,
			OWN.AD_EVNT_START_DT AS AD_DATE,
			CAST(OWN.AD_EVNT_START_TS as Time) as AD_TIME,
			OWN.AD_EVNT_DUR_CNT,
			OWN.SPOT_RATE_NBR as AD_Cost,
			OWN.IMPS AS AUDIENCE_IMPRESSIONS,
			SPOT.SPOT_TTL AS CREATIVE_NM,
			MRKT.NIELSEN_NM AS MARKET_NM,
			SYS.SYSCODE AS BOOKED_SYSCODE,
			COALESCE(SYS.SYSCODE_DESC, 'Undefined') AS BOOKED_SYSCODE_DESC,
			CASE
				WHEN IMPS = 0 THEN 0
				ELSE OWN.SPOT_RATE_NBR/(OWN.IMPS*1000)
			END  AS GROSS_CPM,
			PRGM.PRGM_NM AS PROGRAM,
			GENRE,
			CURRENT_DATE,
			${DAILY_LD_NBR},
			CURRENT_TIMESTAMP(0)
		FROM TMP_CLIENT_LIST CUST
		JOIN VT_OWN_IMPS OWN
			ON OWN.CUST_KEY = CUST.Customer_Key
		JOIN ${TD_DATABASE_AM_BI}.AM_STATION_NAME_OWNER_DIM STN_NM
			ON STN_NM.STN_KEY = OWN.STN_KEY
			AND STN_NM.EFF_END_DT = '2099-12-31'
		JOIN ${TD_DATABASE_AM_BI}.AM_EDA_SPOT_DIM SPOT
			ON OWN.SPOT_KEY = SPOT.SPOT_KEY
			AND CUST.Customer_Key = SPOT.CUST_KEY
		JOIN ${TD_DATABASE_AM_BI}.AM_PROGRAM_DIM PRGM
			ON OWN.PRGM_KEY = PRGM.PRGM_KEY
		JOIN TMP_PULL_SPOT_INFO PRGM_GNR
			ON OWN.PRGM_KEY = PRGM_GNR.PRGM_KEY
		JOIN ${TD_DATABASE_AM_BI}.AM_SYSCODE_DIM SYS
			ON OWN.SYSCODE_KEY = SYS.SYSCODE_KEY
		JOIN ${TD_DATABASE_AM_BI}.AM_DMA_DIM MRKT
			ON OWN.DMA_CD_KEY = MRKT.DMA_CD_KEY""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  executeSql([], [
    (f"""INSERT INTO ${TD_DATABASE_AM_XTR}.AM_TV_SQUARED_EXTRACT
 (
 CAMPAIGN_ID,
 CLIENT_ID,
 STATION,
 AD_DATE,
 AD_TIME,
 AD_SECONDS,
 AD_COST,
 AUDIENCE_IMPRESSIONS,
 CREATIVE_NM,
 MARKET_NM,
 BOOKED_SYSCODE,
 BOOKED_SYSCODE_DESC,
 GROSS_CPM,
 PROGRAM,
 GENRE,
 ETL_PRCS_DT,
 LD_SEQ_NBR,
 CRE_TS
 )
 	 SELECT
		 PID.CMPGN_ID AS CAMPAIGN_ID,
		 CUST.CLIENT_ID,
		 STN_NM.STN_NORM_NM AS STATION,
		 PID.AD_EVENT_START_DT AS AD_DATE,
		 CAST(PID.AD_EVNT_START_TS as Time) as AD_TIME,
		 PID.AD_EVNT_DUR_CNT,
		 PID.SPOT_RATE_NBR as AD_Cost,
		 ROUND(COALESCE(PID.P_IMPS,0),6) AS AUDIENCE_IMPRESSIONS,
		 SPOT.SPOT_TTL AS CREATIVE_NM,
		 MRKT.NIELSEN_NM AS MARKET_NM,
		 SYS.SYSCODE AS BOOKED_SYSCODE,
		 COALESCE(SYS.SYSCODE_DESC, 'Undefined') AS BOOKED_SYSCODE_DESC,
		 CASE
		   WHEN COALESCE(PID.P_IMPS,0) = 0 THEN 0
		   ELSE TO_NUMBER(PID.SPOT_RATE_NBR/(PID.P_IMPS*1000),15,6)
		 END  AS GROSS_CPM,
		 'Unknown' AS PROGRAM,
		 'Unknown' AS GENRE,
		 CURRENT_DATE,
		 ${DAILY_LD_NBR},
		 CURRENT_TIMESTAMP(0)
		FROM TMP_CLIENT_LIST CUST
		JOIN TT_PARTNER_IMPS_DONE PID
			ON PID.CUST_NBR = CUST.CUST_NBR
		JOIN ${TD_DATABASE_AM_BI}.AM_STATION_NAME_OWNER_DIM STN_NM
			ON STN_NM.STN_KEY = PID.STN_KEY
			AND STN_NM.EFF_END_DT = '2099-12-31'
		JOIN ${TD_DATABASE_AM_BI}.AM_EDA_SPOT_DIM SPOT
			ON PID.SPOT_KEY = SPOT.SPOT_KEY
			AND CUST.Customer_Key = SPOT.CUST_KEY
		JOIN ${TD_DATABASE_AM_BI}.AM_SYSCODE_DIM SYS
			ON PID.S_SYSCODE = SYS.SYSCODE
            AND PID.AD_EVENT_START_DT BETWEEN SYS.EFF_START_DT AND SYS.EFF_END_DT
		JOIN ${TD_DATABASE_AM_BI}.AM_DMA_DIM MRKT
			ON PID.DMA_CD_KEY = MRKT.DMA_CD_KEY""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return

  executeSql([], [
    (f"""commit""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-- TRANS_CONTROL - Convert END TRANSACTION/ET to COMMIT
  #-------------------------
  executeSql([], [
    (f"""DROP TABLE if exists VT_XTR_ETL_PRCS_DT_DELETE""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-- DROP_TBL: Add if exists in drop table
  executeSql([], [
    (f"""DROP TABLE if exists TMP_CLIENT_LIST""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-- DROP_TBL: Add if exists in drop table
  executeSql([], [
    (f"""DROP TABLE if exists TMP_DATES""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-- DROP_TBL: Add if exists in drop table
  executeSql([], [
    (f"""DROP TABLE if exists SPOT""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-- DROP_TBL: Add if exists in drop table
  executeSql([], [
    (f"""DROP TABLE if exists ZF""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  executeSql([], [
    (f"""DROP TABLE if exists TT_SUBS""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  executeSql([], [
    (f"""DROP TABLE if exists TT_LIN""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  executeSql([], [
    (f"""DROP TABLE if exists TT_CMPGN_VIEWERS""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  executeSql([], [
    (f"""DROP TABLE if exists SPOT_IMP""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  executeSql([], [
    (f"""DROP TABLE if exists O_SPOTS""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  executeSql([], [
    (f"""DROP TABLE if exists P_SPOTS""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return     
  executeSql([], [
    (f"""DROP TABLE if exists ZIP_DONOR""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return   
  executeSql([], [
    (f"""DROP TABLE if exists SYS_DONOR""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return   
  executeSql([], [
    (f"""DROP TABLE if exists DONOR_SYS_ZIP""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return   
  executeSql([], [
    (f"""DROP TABLE if exists PARTNER_SPOT_ZIP""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return   
  executeSql([], [
    (f"""DROP TABLE if exists PRGM_VIEWS""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return                   
  executeSql([], [
    (f"""DROP TABLE if exists PARTNER_SPOT_ZIP_PRGM""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return 
  executeSql([], [
    (f"""DROP TABLE if exists TT_PARTNER_IMPS""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return 
  executeSql([], [
    (f"""DROP TABLE if exists TT_PARTNER_IMPS_DONE""",
    [])
  ])             
  #-- DROP_TBL: Add if exists in drop table
  executeSql([], [
    (f"""DROP TABLE if exists VT_OWN_IMPS""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-- DROP_TBL: Add if exists in drop table
  executeSql([], [
    (f"""DROP TABLE if exists TMP_PULL_SPOT_INFO""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  executeSql([], [
    (f"""DROP TABLE if exists TMP_PULL_SPOT_INFO_PARTNER""",
    [])
  ])
  if (Action.errorLevel > 1):
    print("MaxErrorLevel reached, exiting...")
    return
  #-- DROP_TBL: Add if exists in drop table
  return
  #--NP-SQL-EXTRACT-SPOT1
  #--NP-SQL-EXTRACT-FILE-END

if __name__ == '__main__':
  main(sys.argv[1:])
  cleanup()
  done()


ACTUAL_EOF

rc=$?

if (( $rc != 0 ));
then
	echo
	echo "Error: Process Failed!!!"
	echo `date`
	echo
	exit 1
fi

echo "Process Completed Successfully!!!"
echo `date`
