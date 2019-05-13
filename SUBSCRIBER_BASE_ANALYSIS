CREATE OR REPLACE PROCEDURE CDRANALYSIS_MTNMFS.MFS_BUILDSUBSBYDAY_THREDDED(RUNDATE DATE)
    IS
        SQL_MAIN    VARCHAR2(20000);
        SQL_MAIN_TH    CLOB;
        IRECS       NUMBER;
        VDEBUG      VARCHAR2(10);
        DTRUNSTRING VARCHAR2(20);
        STATUS      VARCHAR2(200);
        DTPOSTFIX   VARCHAR(10);
        TS_NAME     VARCHAR2(100);
        COUNTER_COLS NUMBER;
        
        CURSOR SERV IS
         SELECT 
            'CAST(CASE WHEN TRANSFER_SUBTYPE = {P}'||TRANSFER_SUBTYPE||'{P} AND SERVICE_NAME = {P}'||SERVICE_NAME||'{P} THEN TO_CHAR(TRUNC(DT),{P}YYYYMMDD{P})||{P}_{P}||REPLACE(REPLACE(replace(transfer_subtype||service_name,{P}INTERNAL{P},{P}{P}),{P}_{P},{P}{P}),{P}UNDEFINED{P},{P}{P}) ELSE NULL END AS VARCHAR2(80)) '||'FIRST_'||SUBSTR(REPLACE(REPLACE(REPLACE(TRANSFER_SUBTYPE||SERVICE_NAME,'INTERNAL',''),'_',''),'UNDEFINED',''),1,30)||',' FIRST_VAL,
            'CAST(CASE WHEN TRANSFER_SUBTYPE = {P}'||TRANSFER_SUBTYPE||'{P} AND SERVICE_NAME = {P}'||SERVICE_NAME||'{P} THEN TO_CHAR(TRUNC(DT),{P}YYYYMMDD{P})||{P}_{P}||REPLACE(REPLACE(replace(transfer_subtype||service_name,{P}INTERNAL{P},{P}{P}),{P}_{P},{P}{P}),{P}UNDEFINED{P},{P}{P}) ELSE NULL END AS VARCHAR2(80)) '||'LAST_'||SUBSTR(REPLACE(REPLACE(REPLACE(TRANSFER_SUBTYPE||SERVICE_NAME,'INTERNAL',''),'_',''),'UNDEFINED',''),1,30)||',' LAST_VAL
        FROM LKP_SERVICE_SUBTYPE
        WHERE UPPER(TRANSFER_SUBTYPE) <> 'UNDEFINED'
        ORDER BY 1;
        
        CURSOR SERV_DEL IS
        
        SELECT * FROM 
        (SELECT
            CASE WHEN SQL1.COLUMN_NAME LIKE '%FIRST%' THEN SQL1.COLUMN_NAME ELSE NULL END VAL1,
            CASE WHEN SQL1.COLUMN_NAME LIKE '%FIRST%' THEN REPLACE(SQL1.COLUMN_NAME,'FIRST_','LAST_') ELSE NULL END VAL2,
            CASE WHEN SQL1.COLUMN_NAME LIKE '%FIRST%' THEN REPLACE(SQL1.COLUMN_NAME,'FIRST_','MMFST_') ELSE NULL END VAL3
         FROM(
        SELECT  
            DISTINCT COLUMN_NAME 
        FROM ALL_TAB_COLUMNS 
        WHERE TABLE_NAME LIKE 'MFSSUBSBYDAYTH_201%'
        AND (COLUMN_NAME LIKE 'LAST_%' OR COLUMN_NAME LIKE 'FIRST_%')     
        ORDER BY 1) SQL1
        LEFT JOIN 
        (SELECT  
            COLUMN_NAME 
        FROM ALL_TAB_COLUMNS 
        WHERE TABLE_NAME = 'MFS_SUBS_BY_DAY'
        AND (COLUMN_NAME LIKE 'LAST_%' OR COLUMN_NAME LIKE 'FIRST_%')     
        ORDER BY 1) SQL2
        ON SQL1.COLUMN_NAME = SQL2.COLUMN_NAME
        WHERE SQL2.COLUMN_NAME IS  NULL)
        WHERE VAL1 IS NOT NULL
        ORDER BY 1
        ;
        
    BEGIN
        /*-----------------------------------------------------------------------------------------------------------------------------------
                created by AM
                SUBSCRIBER ANALYSIS BASETABLE
        ----------------------------------------------------------------------------------------------------------------------------------*/

        /*start logging for the procedure*/
        STATUS := 'START';
        CDRANALYSIS.MAINTENANCE_PKG.LOGSTATUS('MFSSUBSBYDAY_THREDDED',STATUS,TO_CHAR(RUNDATE,'yyyy/mm/dd'));


        DTPOSTFIX := TO_CHAR(RUNDATE,'YYYYMMDD');

        VDEBUG := '1. ';
        STATUS := 'Drop temporary tables -  ';
        CDRANALYSIS.MAINTENANCE_PKG.LOGSTATUS('MFSSUBSBYDAY_THREDDED', 'COMMENT1', STATUS || TO_CHAR(SYSDATE,'hh24:mi:ss'));
        /*Remove Temp Table*/
         CDRANALYSIS.MAINTENANCE_PKG.DROP_TABLE('MFSSUBSBYDAYTH_'||DTPOSTFIX);
         
         DELETE FROM MFSSUBSBYDAYTH;
         
         COMMIT;
         
        VDEBUG := '1.2 ';
        STATUS := 'Create staging table for subsbyday columns -  ';
        CDRANALYSIS.MAINTENANCE_PKG.LOGSTATUS('MFSSUBSBYDAY_THREDDED', 'COMMENT1', STATUS || TO_CHAR(SYSDATE,'hh24:mi:ss'));
        TS_NAME := CDRANALYSIS.MAINTENANCE_PKG.GET_TABLESPACE_NAME('MFSA1DSUBSFIN');
        SQL_MAIN := Q'[
        CREATE TABLE MFSSUBSBYDAYTH
        (
            COLUMN_ID NUMBER NOT NULL,
            CREATE_TABLE_STATEMENT VARCHAR2(20000)
        )
        COMPRESS 
        TABLESPACE {TS_NAME} 
        NOLOGGING]';
        SQL_MAIN := REPLACE(SQL_MAIN, '{DT}',  TO_CHAR(RUNDATE,'DD/MM/YYYY'));
        SQL_MAIN := REPLACE(SQL_MAIN, '{DT1}',  TO_CHAR(RUNDATE+1,'DD/MM/YYYY'));
        /*Make Temp Table Names Unique For Each Day*/
        SQL_MAIN := REPLACE(SQL_MAIN, '{dtpostfix}',  DTPOSTFIX);
        SQL_MAIN := REPLACE(SQL_MAIN, '{TS_NAME}',  TS_NAME);
--        cdranalysis.maintenance_pkg.logdebugmsg(debug_package, 'MFSA1DSUBS', rundate, null, sql_main, null);
--        execute immediate (sql_main);

        VDEBUG := '1.5 ';
        STATUS := 'Insert first part of the insert statement -  ';
        CDRANALYSIS.MAINTENANCE_PKG.LOGSTATUS('MFSSUBSBYDAY_THREDDED', 'COMMENT1', STATUS || TO_CHAR(SYSDATE,'hh24:mi:ss'));
        SQL_MAIN := 
        Q'[
                INSERT INTO MFSSUBSBYDAYTH 
                    (COLUMN_ID, CREATE_TABLE_STATEMENT)
                VALUES
                    (1,'CREATE TABLE MFSSUBSBYDAYTH_{dtpostfix_orig} AS SELECT /*+parallel(16)*/ TO_DATE({P}{DT}{P},{P}DD/MM/YYYY{P}) ASOF, ACCOUNT_ID, TO_DATE({P}{DT}{P},{P}DD/MM/YYYY{P}) FIRST_TRANSACTION_DATE, TO_DATE({P}{DT}{P},{P}DD/MM/YYYY{P}) LAST_TRANSACTION_DATE , SEGMENT,SUB_REGION_NAME, CATEGORY_CODE, ')]';
        /*Make Temp Table Names Unique For Each Day*/
        SQL_MAIN := REPLACE(SQL_MAIN, '{dtpostfix_orig}',  DTPOSTFIX);
        SQL_MAIN := REPLACE(SQL_MAIN, '{DT}',  TO_CHAR(RUNDATE,'DD/MM/YYYY'));
        EXECUTE IMMEDIATE (SQL_MAIN);
        
        COMMIT;
        
        VDEBUG := '2 ';
        STATUS := 'Insert columns for services -  ';
        CDRANALYSIS.MAINTENANCE_PKG.LOGSTATUS('MFSSUBSBYDAY_THREDDED', 'COMMENT1', STATUS || TO_CHAR(SYSDATE,'hh24:mi:ss'));
        
        COUNTER_COLS := 2;
        
        
        FOR COL IN SERV
        LOOP
            SQL_MAIN := Q'[INSERT INTO MFSSUBSBYDAYTH (COLUMN_ID, CREATE_TABLE_STATEMENT)
            VALUES({counter},'{column_name}')]';
            SQL_MAIN := REPLACE(SQL_MAIN, '{dtpostfix_orig}',  DTPOSTFIX);
            SQL_MAIN := REPLACE(SQL_MAIN, '{column_name}',  COL.FIRST_VAL);
            SQL_MAIN := REPLACE(SQL_MAIN, '{counter}', COUNTER_COLS);
            
            DELETE FROM DEBUG_STEPS;
             
            INSERT INTO DEBUG_STEPS (VALUE2, VALUE4) VALUES ('TRY',SQL_MAIN);
            
            COMMIT;
            
            EXECUTE IMMEDIATE (SQL_MAIN);
            COMMIT;
            COUNTER_COLS := COUNTER_COLS +1;
            
            SQL_MAIN := Q'[INSERT INTO MFSSUBSBYDAYTH (COLUMN_ID, CREATE_TABLE_STATEMENT)
            VALUES({counter},'{column_name}')]';
            SQL_MAIN := REPLACE(SQL_MAIN, '{dtpostfix_orig}',  DTPOSTFIX);
            SQL_MAIN := REPLACE(SQL_MAIN, '{column_name}',  COL.LAST_VAL);
            SQL_MAIN := REPLACE(SQL_MAIN, '{counter}', COUNTER_COLS);
            EXECUTE IMMEDIATE (SQL_MAIN);
            COMMIT;
            COUNTER_COLS := COUNTER_COLS +1;
            
            SQL_MAIN := Q'[INSERT INTO MFSSUBSBYDAYTH (COLUMN_ID, CREATE_TABLE_STATEMENT)
            VALUES({counter},'{column_name}')]';
            SQL_MAIN := REPLACE(SQL_MAIN, '{dtpostfix_orig}',  DTPOSTFIX);
--            sql_main := replace(sql_main, '{column_name}',  col.mmfst_val);
            SQL_MAIN := REPLACE(SQL_MAIN, '{counter}', COUNTER_COLS);
            --execute immediate (sql_main);
            COMMIT;
            COUNTER_COLS := COUNTER_COLS +1;
            
            COMMIT;
        END LOOP;
       
        COMMIT;
        
        VDEBUG := '3 ';
        TS_NAME := CDRANALYSIS.MAINTENANCE_PKG.GET_TABLESPACE_NAME('MFSRPTMFSUSAGE');
        STATUS := 'Insert closing part of the create table statement -  ';
        CDRANALYSIS.MAINTENANCE_PKG.LOGSTATUS('MFSSUBSBYDAY_THREDDED', 'COMMENT1', STATUS || TO_CHAR(SYSDATE,'hh24:mi:ss'));
        
         SQL_MAIN := 
        Q'[
                INSERT INTO MFSSUBSBYDAYTH (COLUMN_ID, CREATE_TABLE_STATEMENT)
                VALUES({counter},' FROM STG_MFS_SUMMARY WHERE DT {greaterequals} TO_DATE({P}{DT}{P},{P}DD/MM/YY{P}) and DT{less} TO_DATE({P}{DT1}{P},{P}DD/MM/YY{P})')
        ]';
        SQL_MAIN := REPLACE(SQL_MAIN, '{dtpostfix_orig}',  DTPOSTFIX);
        SQL_MAIN := REPLACE(SQL_MAIN, '{counter}', COUNTER_COLS);
        SQL_MAIN := REPLACE(SQL_MAIN, '{TS_NAME}',  TS_NAME);
        INSERT INTO DEBUG_STEPS (VALUE2, VALUE4) VALUES ('INSERTING TABLENAME AND DATE FILTER',SQL_MAIN);
        COMMIT;
        EXECUTE IMMEDIATE (SQL_MAIN);
        
        COMMIT;
        
        VDEBUG := '4 ';
        STATUS := 'Concat the temp table create statement -  ';
        CDRANALYSIS.MAINTENANCE_PKG.LOGSTATUS('MFSSUBSBYDAY_THREDDED', 'COMMENT1', STATUS || TO_CHAR(SYSDATE,'hh24:mi:ss'));
            
        SELECT  RTRIM(XMLAGG(XMLELEMENT(E,CREATE_TABLE_STATEMENT,' ').EXTRACT('//text()') ORDER BY COLUMN_ID).GETCLOBVAL(),',') INTO SQL_MAIN_TH
        FROM MFSSUBSBYDAYTH;
           
        VDEBUG := '5 ';
        STATUS := 'MAINTAIN DEBUG STEPS -  ';
        CDRANALYSIS.MAINTENANCE_PKG.LOGSTATUS('MFSSUBSBYDAY_THREDDED', 'COMMENT1', STATUS || TO_CHAR(SYSDATE,'hh24:mi:ss'));
                  
        COMMIT;
                  
                          
        VDEBUG := '5.1 ';
        STATUS := 'REPLACE PARAMETERS AND RUN -  ';
        CDRANALYSIS.MAINTENANCE_PKG.LOGSTATUS('MFSSUBSBYDAY_THREDDED', 'COMMENT1', STATUS || TO_CHAR(SYSDATE,'hh24:mi:ss'));
        
        SQL_MAIN_TH := REPLACE(SQL_MAIN_TH,'{P}','''');
        SQL_MAIN_TH := REPLACE(SQL_MAIN_TH,'{DT}',TO_DATE(RUNDATE,'DD/MM/YYYY'));
        SQL_MAIN_TH := REPLACE(SQL_MAIN_TH,'{DT1}',TO_DATE(RUNDATE,'DD/MM/YYYY') + 1);
        SQL_MAIN_TH := REPLACE(SQL_MAIN_TH,'{greaterequals}','>=');
        SQL_MAIN_TH := REPLACE(SQL_MAIN_TH,'{less}','<');
        SQL_MAIN_TH := REPLACE(SQL_MAIN_TH,',  FROM STG_MFS_SUMMARY',' FROM STG_MFS_SUMMARY');
        EXECUTE IMMEDIATE (SQL_MAIN_TH);
                      
          
        COMMIT;
        
        VDEBUG := '6 ';
        STATUS := 'MAINTAIN DEBUG STEPS -  ';
        CDRANALYSIS.MAINTENANCE_PKG.LOGSTATUS('MFSSUBSBYDAY_THREDDED', 'COMMENT1', STATUS || TO_CHAR(SYSDATE,'hh24:mi:ss'));
        
        SQL_MAIN := 
        Q'[ALTER TABLE MFSSUBSBYDAYTH_20180305
        ADD FIRST_TEST VARCHAR2(80)]';
        EXECUTE IMMEDIATE (SQL_MAIN);
        COMMIT;
        
        COMMIT;
        
        VDEBUG := '6.1';
        STATUS := 'MAINTAIN DEBUG STEPS -  ';
        CDRANALYSIS.MAINTENANCE_PKG.LOGSTATUS('MFSSUBSBYDAY_THREDDED', 'COMMENT1', STATUS || TO_CHAR(SYSDATE,'hh24:mi:ss'));  
        
        FOR COL_ADD IN SERV_DEL
        LOOP
        
        VDEBUG := '6.1.1';
        INSERT INTO DEBUG_STEPS (VALUE2,value3,VALUE4) VALUES ('ADD COLUMNS - col',COL_ADD.VAL1,sql_main);   
        COMMIT;
        SQL_MAIN := 
        Q'[ALTER TABLE CDRANALYSIS_MTNMFS.MFS_SUBS_BY_DAY ADD {column_name} VARCHAR2(80)]';
        SQL_MAIN := REPLACE(SQL_MAIN,'{column_name}',COL_ADD.VAL1);
                           
        INSERT INTO DEBUG_STEPS (VALUE2, VALUE4) VALUES ('ADD COLUMNS',SQL_MAIN);   
        COMMIT;
        EXECUTE IMMEDIATE (SQL_MAIN);
        COMMIT;
        
        SQL_MAIN := 
        Q'[ALTER TABLE CDRANALYSIS_MTNMFS.MFS_SUBS_BY_DAY ADD {column_name} VARCHAR2(80)]';
        INSERT INTO DEBUG_STEPS (VALUE2,value3,VALUE4) VALUES ('ADD COLUMNS - col',COL_ADD.VAL2,sql_main);   
        COMMIT;
        SQL_MAIN := REPLACE(SQL_MAIN,'{column_name}',COL_ADD.VAL2);
        EXECUTE IMMEDIATE (SQL_MAIN);
        COMMIT;
        
        INSERT INTO DEBUG_STEPS (VALUE2,value3,VALUE4) VALUES ('ADD COLUMNS - col',COL_ADD.VAL1,sql_main);   
        COMMIT;
        SQL_MAIN := 
        Q'[ALTER TABLE CDRANALYSIS_MTNMFS.MFS_SUBS_BY_DAY ADD {column_name} VARCHAR2(80)]';
        SQL_MAIN := REPLACE(SQL_MAIN,'{column_name}',COL_ADD.VAL3);
        EXECUTE IMMEDIATE (SQL_MAIN);
        COMMIT;
                    
        END LOOP;
            
        STATUS := 'END';
        CDRANALYSIS.MAINTENANCE_PKG.LOGSTATUS('MFSSUBSBYDAY_THREDDED',STATUS,TO_CHAR(RUNDATE,'yyyy/mm/dd'));

    EXCEPTION
        WHEN OTHERS THEN
            CDRANALYSIS.MAINTENANCE_PKG.LOGSTATUS('MFSSUBSBYDAY_THREDDED', 'ERROR', 'at ' || VDEBUG || ' : ' || SQLERRM || ' ' || TO_CHAR(SYSDATE,'HH24:MI:SS'), RUNDATE);
    END;
/

