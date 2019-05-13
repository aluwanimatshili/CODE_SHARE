CREATE OR REPLACE PACKAGE BODY CDR.FILERECON_V2
AS
    /**************************************************************************/    
    VERSION_NUMBER      CONSTANT VARCHAR2(6) := '6.1.3';
    HIGH_WATER_MARKER   CONSTANT VARCHAR2(30) := 'OLDESTTRACKPARS';
    DEBUG_PACKAGE       CONSTANT VARCHAR2(30) := 'FILERECON';
    DLASTPARTITION      DATE;
    /**************************************************************************/

    /*get version number*/
    FUNCTION GETVERSION RETURN VARCHAR2 IS
    BEGIN
        RETURN VERSION_NUMBER;
    END;
    
    PROCEDURE CREATE_PARTITIONS_TO_CURRENT(TABLENAME IN VARCHAR2 DEFAULT 'RECON_TRACK_FILES', PREFIX IN VARCHAR2 DEFAULT 'RT')
    AS
      SLASTPARTITION    VARCHAR2(10);
      IDAYS             NUMBER;
      SQL_MAIN          VARCHAR2(500);
    BEGIN
        --get last partition - point from which new partitions will be created
        SELECT SUBSTR(MAX(PARTITION_NAME),4) INTO SLASTPARTITION
        FROM ALL_TAB_PARTITIONS
        WHERE TABLE_NAME = TABLENAME;

        -- e.g. RT_2013053024
        --24 is not a valid hour
        IF SUBSTR(SLASTPARTITION,9) < 24 THEN
            DLASTPARTITION := TO_DATE(SLASTPARTITION,'YYYYMMDDHH24');
        ELSE
            DLASTPARTITION := TO_DATE(SUBSTR(SLASTPARTITION,1,8) || 23, 'YYYYMMDDHH24');
        END IF;

        IDAYS := TRUNC(SYSDATE + 2) - DLASTPARTITION;  --up to where partitions will be created
        FOR IDT IN 0..IDAYS LOOP
            FOR IHOUR IN 1..24 LOOP
                BEGIN
                    SQL_MAIN := 'ALTER TABLE {TABLE_NAME} ADD PARTITION {PREFIX}_{PARTITION_NAME} VALUES LESS THAN (TO_DATE(''{HOUR}'',''YYYYMMDDHH24'')) COMPRESS';
                    SQL_MAIN := REPLACE(SQL_MAIN, '{TABLE_NAME}',TABLENAME);
                    SQL_MAIN := REPLACE(SQL_MAIN, '{PREFIX}',PREFIX);
                    SQL_MAIN := REPLACE(SQL_MAIN, '{PARTITION_NAME}', TO_CHAR(DLASTPARTITION + IDT,'YYYYMMDD') || LPAD(IHOUR,2,'0'));

                    IF IHOUR < 24 THEN
                        SQL_MAIN := REPLACE(SQL_MAIN, '{HOUR}', TO_CHAR(DLASTPARTITION + IDT,'YYYYMMDD') || LPAD(IHOUR,2,'0'));
                    ELSE
                        SQL_MAIN := REPLACE(SQL_MAIN, '{HOUR}', TO_CHAR(DLASTPARTITION + IDT + 1,'YYYYMMDD') || '00');
                    END IF;

                    MAINTENANCE_PKG.LOGDEBUGMSG(DEBUG_PACKAGE, 'CREATE_PARTITIONS', TRUNC(SYSDATE), NULL, SQL_MAIN, NULL);
                    EXECUTE IMMEDIATE SQL_MAIN;
                EXCEPTION
                    WHEN OTHERS THEN NULL;
                END;
            END LOOP;
        END LOOP;
    END;

    PROCEDURE TRUNCATE_PARTITIONS_TO_CURRENT(FROMDT IN DATE, TABLENAME IN VARCHAR2, STEPNAME IN VARCHAR2, PREFIX IN VARCHAR2 DEFAULT 'RT')
    AS
        RUNDATE     VARCHAR2(1000);
        ICOUNT      NUMBER;
        PARTNAME    VARCHAR2(100);
        SQL_MAIN    VARCHAR2(500);
    BEGIN
        RUNDATE := TRUNC(SYSDATE);
        --dLastPartition is populated in Create_Partitions_to_current so that needs to be run first
        ICOUNT := ((DLASTPARTITION - FROMDT) * 24) + 1;  --number of partitions that should be truncated

        FOR IHOUR IN 1..ICOUNT LOOP
            SQL_MAIN := 'ALTER TABLE {TABLE_NAME} TRUNCATE PARTITION {PARTITION_NAME} DROP STORAGE';
            SQL_MAIN := REPLACE(SQL_MAIN, '{TABLE_NAME}',TABLENAME);

            IF TO_CHAR(FROMDT + (IHOUR/24),'HH24') > 0 THEN
                PARTNAME :=  TO_CHAR(FROMDT + (IHOUR/24),'YYYYMMDDHH24');
            ELSE
                PARTNAME :=  TO_CHAR(FROMDT + (IHOUR/24) - (1/24),'YYYYMMDD') || '24';
            END IF;

            SQL_MAIN := REPLACE(SQL_MAIN, '{PARTITION_NAME}', PREFIX || '_' || PARTNAME);
            MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE1', PARTNAME, RUNDATE);
            MAINTENANCE_PKG.LOGDEBUGMSG(DEBUG_PACKAGE, 'TRUNCATE_PARTITIONS', TRUNC(SYSDATE), NULL, SQL_MAIN, NULL);
            EXECUTE IMMEDIATE SQL_MAIN;
        END LOOP;
    END;

    PROCEDURE SETOLDESTTRACKPARS
    AS
        SLASTPARTITION  VARCHAR2(10);
        SQL_MAIN        VARCHAR2(500);
        OLDESTDATE    DATE;
    BEGIN
        OLDESTDATE := TRACEFILES.GETOLDESTTRACKPARS;
        
        SELECT /*+ PARALLEL(32)*/
            TO_CHAR(MIN(FT_TIMEPROCESSED),'YYYYMMDDHH24') INTO SLASTPARTITION
        FROM RECON_TRACK_FILES
        WHERE (TRACK_PENDING_FILES + P_PENDING_FILES + AZ_PENDING_FILES + L_PENDING_FILES) > 0 --check for any pending files
        AND TRACK_ZERO_BYTES <> 1 --exclude 0 byte files
        AND FT_TIMEPROCESSED >= (OLDESTDATE);

        IF SLASTPARTITION IS NOT NULL THEN
            SQL_MAIN := ' CDRMODULECONFIG SET VALUE = ''' || SLASTPARTITION || ''' WHERE MODULE = ''GETFILES'' AND PARAMETER = ''' || HIGH_WATER_MARKER || ''''; 
            MAINTENANCE_PKG.LOGDEBUGMSG(DEBUG_PACKAGE, 'SET_OLDEST_TRACK_PARS', TRUNC(SYSDATE), NULL, SQL_MAIN, NULL);
            EXECUTE IMMEDIATE SQL_MAIN;
            COMMIT;
        END IF;
    END;
    
    PROCEDURE EXTRACTTRACKEDFILES(RUNDATE IN DATE, DAYSBACK IN NUMBER DEFAULT 0)
    AS
        STATUS      VARCHAR2(50);
        STEPNAME    VARCHAR2(50);
        VDEBUG      VARCHAR2(50);
        IRECS       NUMBER;
        PARTPREFIX  VARCHAR2(10);
        INDEXNAME   VARCHAR2(30);
        SQL_FINAL   VARCHAR2(32000); 
        DATEFROM    DATE;       
        SQL_MAIN    CONSTANT VARCHAR2(32000) :=
                    q'[
                        SELECT {INDEX_HINT}
                            TT.FT_FileReference, TT.FT_InputServer, 
                            FILERECON_FUNCTIONS.RemapTechName(PT.TechName) TechName, 
                            NVL(FW.Description, TT.FT_ApplicationID) FeedName, TT.FT_SwitchID, TT.FT_InputFileName,
                            FILERECON_FUNCTIONS.GetMSCFromFileName(FW.Description, FILERECON_FUNCTIONS.RemapTechName(PT.TechName), TT.FT_InputFileName) MSC_FromFileName,
                            FILERECON_FUNCTIONS.GetSeqFromFileName(FW.Description, FILERECON_FUNCTIONS.RemapTechName(PT.TechName), TT.FT_InputFileName) File_Sequence,
                            CASE WHEN TT.FT_FileSize IS NOT NULL THEN 'COMPLETE' ELSE TT.Status END FT_Status,
                            TT.NextProcess, TT.FT_TimeProcessed, TT.FT_FileDateTime, TT.FT_FileSize, TT.FT_ApplicationID  
                        FROM {TABLE_NAME} TT
                        LEFT JOIN FW_APPLICATIONS FW On FW.ApplicationID = TT.FT_ApplicationID
                        LEFT JOIN DEC_PARSETECH PT ON PT.TechID = TT.FT_Technology
                        WHERE TT.{PREFIX}_TimeProcessed >= TO_DATE('{DATEFROM}','DD/MM/YYYY HH24MI')
                    ]';
        --Cursor to get the list of tracking tables used...
        CURSOR TRACKERS IS
            SELECT DISTINCT
                FS.VALUE TABLENAME
            FROM FW_APPLICATIONS FW
            LEFT JOIN FW_APPSETTINGS FS ON FS.APPLICATIONID = FW.APPLICATIONID AND FS.PARAMETER = 'TRACEFILE_TABLE'
            WHERE FW.ACTIVE = 'Y'
            AND FW.APPTYPE = (SELECT APPTYPEID FROM FW_APPTYPES WHERE UPPER(APPTYPE) = 'CDRFILETRACKER')
            AND TO_NUMBER(SUBSTR(FW.VERSION, 1, 1)) >= 5
            ORDER BY 1;
    BEGIN
        STATUS := 'START';
        STEPNAME := 'RECONV2_EXTRACT_TRACKED_FILES';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
        VDEBUG := '0 ';        
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE CDR.TMP_TRACKED_FILES CASCADE CONSTRAINTS PURGE';
        EXCEPTION 
            WHEN OTHERS THEN NULL;
        END;
        
        IF DAYSBACK > 0 THEN
            DATEFROM := TRUNC(SYSDATE - DAYSBACK);
        ELSE
            DATEFROM := TRACEFILES.GETOLDESTTRACKPARS;
        END IF;

        --Loop through all the configured trackers...
        SQL_FINAL := NULL;
        VDEBUG := '1 ';
        FOR TRACETABLE IN TRACKERS
        LOOP
            PARTPREFIX := TRACEFILES.GET_PARTITION_PREFIX(TRACETABLE.TABLENAME);
            INDEXNAME := TRACEFILES.GET_TABLEINDEX(TRACETABLE.TABLENAME);

            IF (SQL_FINAL IS NULL) THEN
                SQL_FINAL := SQL_MAIN;
            ELSE
                SQL_FINAL := SQL_FINAL || ' UNION ALL ' || SQL_MAIN;
            END IF;

            IF (INDEXNAME IS NULL) THEN
                SQL_FINAL := REPLACE(SQL_FINAL, '{INDEX_HINT}', '/*+ PARALLEL(32)*/');
            ELSE
                SQL_FINAL := REPLACE(SQL_FINAL, '{INDEX_HINT}', '/*+ INDEX(TT ' || INDEXNAME || ') */');
            END IF;

            SQL_FINAL := REPLACE(SQL_FINAL, '{TABLE_NAME}', TRACETABLE.TABLENAME);
            SQL_FINAL := REPLACE(SQL_FINAL, '{DATEFROM}', TO_CHAR(DATEFROM,'DD/MM/YYYY HH24MI'));
            SQL_FINAL := REPLACE(SQL_FINAL, '{PREFIX}', PARTPREFIX);
        END LOOP;
        SQL_FINAL := 'CREATE TABLE TMP_TRACKED_FILES COMPRESS NOLOGGING AS ' || SQL_FINAL;
        VDEBUG := '2 ';
        MAINTENANCE_PKG.LOGDEBUGMSG(DEBUG_PACKAGE, 'TMP_TRACKED_FILES', RUNDATE, NULL, SQL_FINAL, NULL);
        EXECUTE IMMEDIATE (SQL_FINAL);
        IRECS := SQL%ROWCOUNT;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE1', IRECS, RUNDATE);
        STATUS := 'END';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
    EXCEPTION
        WHEN OTHERS THEN
            MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'ERROR', 'at ' || VDEBUG || SQLERRM ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
    END;

    PROCEDURE EXTRACTPARSEDFILES(RUNDATE IN DATE, DAYSBACK IN NUMBER DEFAULT 0)
    AS
        STATUS      VARCHAR2(50);
        STEPNAME    VARCHAR2(50);
        VDEBUG      VARCHAR2(50);
        IRECS       NUMBER;
        PARTPREFIX  VARCHAR2(10);
        INDEXNAME   VARCHAR2(30);
        SQL_FINAL   VARCHAR2(32000);
        DATEFROM    DATE;
        SQL_MAIN    CONSTANT VARCHAR2(32000) :=
                    q'[
                        SELECT {INDEX_HINT}
                            TT.FT_FileReference, TT.P_InputServer,
                            CASE WHEN TT.P_FileSize IS NOT NULL THEN 'COMPLETE' ELSE TT.Status END P_Status,
                            TT.NextProcess, TT.P_TimeProcessed, TT.P_TotalRecords, TT.P_RecordsWritten, TT.P_RecordsErrored, TT.P_RecordsSkipped,
                            TT.P_ApplicationID, TT.P_FileSize, TT.P_SecondsToProcess, TT.P_FirstSequenceNo, TT.P_LastSequenceNo, TT.P_MinCDRDate, TT.P_MaxCDRDate
                        FROM {TABLE_NAME} TT
                        WHERE TT.{PREFIX}_TimeProcessed >= TO_DATE('{DATEFROM}','DD/MM/YYYY HH24MI')
                    ]';
        --Cursor to get the list of tracking tables used...
        CURSOR PARSERS IS
            SELECT DISTINCT
                FS.VALUE TABLENAME
            FROM FW_APPLICATIONS FW
            LEFT JOIN FW_APPSETTINGS FS ON FS.APPLICATIONID = FW.APPLICATIONID AND FS.PARAMETER = 'TTFILETABLE'
            WHERE FW.ACTIVE = 'Y'
            AND FW.APPTYPE IN (SELECT APPTYPEID FROM FW_APPTYPES WHERE UPPER(APPTYPE) LIKE 'CDRPARSER%')
            AND TO_NUMBER(SUBSTR(FW.VERSION, 1, 1)) >= 5
            ORDER BY 1;
    BEGIN
        STATUS := 'START';
        STEPNAME := 'RECONV2_EXTRACT_PARSED_FILES';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
        VDEBUG := '0 ';
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE CDR.TMP_PARSED_FILES CASCADE CONSTRAINTS PURGE';
        EXCEPTION 
            WHEN OTHERS THEN NULL;
        END;
        
        IF DAYSBACK > 0 THEN
            DATEFROM := TRUNC(SYSDATE - DAYSBACK);
        ELSE
            DATEFROM := TRACEFILES.GETOLDESTTRACKPARS;
        END IF;

        --Loop through all the configured trackers...
        SQL_FINAL := NULL;
        VDEBUG := '1 ';
        FOR TRACETABLE IN PARSERS
        LOOP
            PARTPREFIX := TRACEFILES.GET_PARTITION_PREFIX(TRACETABLE.TABLENAME);
            INDEXNAME := TRACEFILES.GET_TABLEINDEX(TRACETABLE.TABLENAME);

            IF (SQL_FINAL IS NULL) THEN
                SQL_FINAL := SQL_MAIN;
            ELSE
                SQL_FINAL := SQL_FINAL || ' UNION ALL ' || SQL_MAIN;
            END IF;

            IF (INDEXNAME IS NULL) THEN
                SQL_FINAL := REPLACE(SQL_FINAL, '{INDEX_HINT}', '/*+ PARALLEL(32)*/');
            ELSE
                SQL_FINAL := REPLACE(SQL_FINAL, '{INDEX_HINT}', '/*+ INDEX(TT ' || INDEXNAME || ') */');
            END IF;

            SQL_FINAL := REPLACE(SQL_FINAL, '{TABLE_NAME}', TRACETABLE.TABLENAME);
            SQL_FINAL := REPLACE(SQL_FINAL, '{DATEFROM}', TO_CHAR(DATEFROM,'DD/MM/YYYY HH24MI'));
            SQL_FINAL := REPLACE(SQL_FINAL, '{PREFIX}', PARTPREFIX);
        END LOOP;
        SQL_FINAL := 'CREATE TABLE TMP_PARSED_FILES COMPRESS NOLOGGING AS ' || SQL_FINAL;
        VDEBUG := '2 ';
        MAINTENANCE_PKG.LOGDEBUGMSG(DEBUG_PACKAGE, 'TMP_PARSED_FILES', RUNDATE, NULL, SQL_FINAL, NULL);
        EXECUTE IMMEDIATE (SQL_FINAL);
        IRECS := SQL%ROWCOUNT;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE1', IRECS, RUNDATE);
        STATUS := 'END';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
    EXCEPTION
        WHEN OTHERS THEN
            MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'ERROR', 'at ' || VDEBUG || SQLERRM ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
    END;

    PROCEDURE EXTRACTANALYZEDFILES(RUNDATE IN DATE, DAYSBACK IN NUMBER DEFAULT 0)
    AS
        STATUS      VARCHAR2(50);
        STEPNAME    VARCHAR2(50);
        VDEBUG      VARCHAR2(50);
        IRECS       NUMBER;
        PARTPREFIX  VARCHAR2(10);
        INDEXNAME   VARCHAR2(30);
        SQL_FINAL   VARCHAR2(32000);
        DATEFROM    DATE;
        SQL_MAIN    CONSTANT VARCHAR2(32000) :=
                    q'[
                        SELECT {INDEX_HINT}
                            TT.FT_FileReference, TT.AZ_InputServer,
                            CASE
                                WHEN TT.NextProcess IN (2) THEN TT.Status
                                WHEN TT.AZ_FileSize IS NOT NULL THEN 'COMPLETE'
                                ELSE NULL
                            END AZ_Status,
                            TT.NextProcess, TT.AZ_TimeProcessed, TT.AZ_TotalRecords, TT.AZ_RecordsWritten, TT.AZ_RecordsErrored, TT.AZ_RecordsSkipped,
                            TT.AZ_ApplicationID, TT.AZ_FileSize, TT.AZ_SecondsToProcess, TT.AZ_FirstSequenceNo, TT.AZ_LastSequenceNo
                        FROM {TABLE_NAME} TT
                        WHERE TT.{PREFIX}_TimeProcessed >= TO_DATE('{DATEFROM}','DD/MM/YYYY HH24MI')
                    ]';
        --Cursor to get the list of tracking tables used...
        CURSOR ANALYZERS IS
            SELECT DISTINCT
                FS.VALUE TABLENAME
            FROM FW_APPLICATIONS FW
            LEFT JOIN FW_APPSETTINGS FS ON FS.APPLICATIONID = FW.APPLICATIONID AND FS.PARAMETER = 'TTFILETABLE'
            WHERE FW.ACTIVE = 'Y'
            AND FW.APPTYPE = (SELECT APPTYPEID FROM FW_APPTYPES WHERE UPPER(APPTYPE) = 'CDRANALYSER')
            AND TO_NUMBER(SUBSTR(FW.VERSION, 1, 1)) >= 5
            ORDER BY 1;
    BEGIN
        STATUS := 'START';
        STEPNAME := 'RECONV2_EXTRACT_ANALYZED_FILES';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
        VDEBUG := '0 ';
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE CDR.TMP_ANALYZED_FILES CASCADE CONSTRAINTS PURGE';
        EXCEPTION 
            WHEN OTHERS THEN NULL;
        END;
        
        IF DAYSBACK > 0 THEN
            DATEFROM := TRUNC(SYSDATE - DAYSBACK);
        ELSE
            DATEFROM := TRACEFILES.GETOLDESTTRACKPARS;
        END IF;
        --Loop through all the configured trackers...
        SQL_FINAL := NULL;
        VDEBUG := '1 ';
        FOR TRACETABLE IN ANALYZERS
        LOOP
            PARTPREFIX := TRACEFILES.GET_PARTITION_PREFIX(TRACETABLE.TABLENAME);
            INDEXNAME := TRACEFILES.GET_TABLEINDEX(TRACETABLE.TABLENAME);

            IF (SQL_FINAL IS NULL) THEN
                SQL_FINAL := SQL_MAIN;
            ELSE
                SQL_FINAL := SQL_FINAL || ' UNION ALL ' || SQL_MAIN;
            END IF;

            IF (INDEXNAME IS NULL) THEN
                SQL_FINAL := REPLACE(SQL_FINAL, '{INDEX_HINT}', '/*+ PARALLEL(32)*/');
            ELSE
                SQL_FINAL := REPLACE(SQL_FINAL, '{INDEX_HINT}', '/*+ INDEX(TT ' || INDEXNAME || ') */');
            END IF;

            SQL_FINAL := REPLACE(SQL_FINAL, '{TABLE_NAME}', TRACETABLE.TABLENAME);
            SQL_FINAL := REPLACE(SQL_FINAL, '{DATEFROM}', TO_CHAR(DATEFROM,'DD/MM/YYYY HH24MI'));
            SQL_FINAL := REPLACE(SQL_FINAL, '{PREFIX}', PARTPREFIX);
        END LOOP;
        SQL_FINAL := '
            CREATE TABLE TMP_ANALYZED_FILES COMPRESS NOLOGGING AS
            SELECT /*+ PARALLEL(32)*/
                FT_FileReference, AZ_ApplicationID, AZ_InputServer, NextProcess,
                COUNT(*) Analyser_Files,
                SUM(CASE WHEN AZ_Status = ''COMPLETE'' THEN 1 ELSE 0 END) AZ_Complete_Files,
                SUM(CASE WHEN AZ_Status IN (''PENDING'', ''BUSY'') THEN 1 ELSE 0 END) AZ_Pending_Files,
                SUM(CASE WHEN AZ_Status = ''ERROR'' THEN 1 ELSE 0 END) AZ_Error_Files,
                SUM(CASE WHEN AZ_Status NOT IN (''ERROR'', ''COMPLETE'', ''PENDING'', ''BUSY'') THEN 1 ELSE 0 END) AZ_Other_Files,
                SUM(AZ_TotalRecords) AZ_TotalRecords,
                SUM(AZ_RecordsWritten) AZ_RecordsWritten,
                SUM(AZ_RecordsErrored) AZ_RecordsErrored,
                SUM(AZ_RecordsSkipped) AZ_RecordsSkipped,
                MAX(AZ_TimeProcessed) AZ_TimeProcessed,
                SUM(AZ_SecondsToProcess) AZ_SecondsToProcess,
                MIN(AZ_FirstSequenceNo) AZ_FirstSequenceNo,
                MAX(AZ_LastSequenceNo) AZ_LastSequenceNo,
                SUM(AZ_FileSize) AZ_FileSize
            FROM
            ( ' || SQL_FINAL || ' )
            GROUP BY FT_FileReference, AZ_ApplicationID, AZ_InputServer, NextProcess
            ORDER BY 3';

        VDEBUG := '2 ';
        MAINTENANCE_PKG.LOGDEBUGMSG(DEBUG_PACKAGE, 'TMP_ANALYZED_FILES', RUNDATE, NULL, SQL_FINAL, NULL);
        EXECUTE IMMEDIATE (SQL_FINAL);
        IRECS := SQL%ROWCOUNT;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE1', IRECS, RUNDATE);
        STATUS := 'END';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
    EXCEPTION
        WHEN OTHERS THEN
            MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'ERROR', 'at ' || VDEBUG || SQLERRM ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
    END;

    PROCEDURE EXTRACTLOADEDFILES(RUNDATE IN DATE, DAYSBACK IN NUMBER DEFAULT 0)
    AS
        STATUS      VARCHAR2(50);
        STEPNAME    VARCHAR2(50);
        VDEBUG      VARCHAR2(50);
        IRECS       NUMBER;
        PARTPREFIX  VARCHAR2(10);
        COLPREFIX   VARCHAR2(10);
        INDEXNAME   VARCHAR2(30);
        VSCHEMAUSER VARCHAR(30);
        SQL_FINAL   VARCHAR2(32000);
        DATEFROM    DATE;
        SQL_MAIN    CONSTANT VARCHAR2(32000) :=
                    q'[
                        SELECT {INDEX_HINT}
                            TT.FT_FileReference, TT.L_InputServer,
                            CASE
                                WHEN TT.NextProcess IN (4) THEN TT.Status
                                WHEN TT.L_FileSize IS NOT NULL THEN 'COMPLETE'
                                ELSE NULL
                            END L_Status,
                            TT.NextProcess, TT.L_TimeProcessed,
                            CASE 
                                WHEN TT.NextProcess = 4 AND TT.AZ_RecordsWritten IS NULL THEN TT.{WRITTEN_PREFIX}_RecordsWritten --Only parsed and loaded...
                                ELSE AZ_RecordsWritten  
                            END AZ_RecordsWritten,
                            CASE 
                                WHEN TT.NextProcess = 4 AND TT.AZ_RecordsWritten IS NULL THEN TT.{WRITTEN_PREFIX}_RecordsWritten - (TT.L_RecordsSkipped + TT.L_RecordsDiscarded + TT.L_RecordsRejected) --Only parsed and loaded...
                                ELSE AZ_RecordsWritten - (TT.L_RecordsSkipped + TT.L_RecordsDiscarded + TT.L_RecordsRejected)  
                            END CalculatedPerFileSuccess,
                            TT.L_RecordsSkipped, TT.L_RecordsDiscarded, TT.L_RecordsRejected,
                            TT.L_ApplicationID, TT.L_FileSize, TT.L_SecondsToProcess, TT.L_Partition
                        FROM {TABLE_NAME} TT
                        WHERE TT.{PREFIX}_TimeProcessed >= TO_DATE('{DATEFROM}','DD/MM/YYYY HH24MI')
                        AND TT.Status <> 'SPLIT'
                    ]';
        --Cursor to get the list of tracking tables used...
        CURSOR LOADERS IS
            SELECT
                T.TABLENAME,    
                (SELECT COUNT(*) FROM DBA_TAB_COLUMNS WHERE OWNER = VSCHEMAUSER AND COLUMN_NAME = 'P_RECORDSWRITTEN' AND TABLE_NAME = UPPER(T.NAMEONLY)) P_RECORDSCOL
            FROM
            (                
                SELECT DISTINCT 
                    FS.VALUE TABLENAME,
                    CASE WHEN INSTR(FS.VALUE, '.') > 0 THEN SUBSTR(FS.VALUE, INSTR(FS.VALUE, '.')+1) ELSE FS.VALUE END NAMEONLY
                FROM FW_APPLICATIONS FW
                LEFT JOIN FW_APPSETTINGS FS ON FS.APPLICATIONID = FW.APPLICATIONID AND FS.PARAMETER = 'TTFILETABLE'
                WHERE FW.ACTIVE = 'Y'
                AND FW.APPTYPE = (SELECT APPTYPEID FROM FW_APPTYPES WHERE UPPER(APPTYPE) = 'CDRLOADER')
                AND TO_NUMBER(SUBSTR(FW.VERSION, 1, 1)) >= 5            
            ) T    
            ORDER BY 1;
    BEGIN
        STATUS := 'START';
        STEPNAME := 'RECONV2_EXTRACT_LOADED_FILES';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
        VDEBUG := '0 ';
        SELECT SYS_CONTEXT('USERENV','SESSION_USER') INTO VSCHEMAUSER FROM DUAL;
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE CDR.TMP_LOADED_FILES CASCADE CONSTRAINTS PURGE';
        EXCEPTION 
            WHEN OTHERS THEN NULL;
        END;
        
        IF DAYSBACK > 0 THEN
            DATEFROM := TRUNC(SYSDATE - DAYSBACK);
        ELSE
            DATEFROM := TRACEFILES.GETOLDESTTRACKPARS;
        END IF;

        --Loop through all the configured trackers...
        SQL_FINAL := NULL;
        VDEBUG := '1 ';
        FOR TRACETABLE IN LOADERS
        LOOP
            PARTPREFIX := TRACEFILES.GET_PARTITION_PREFIX(TRACETABLE.TABLENAME);
            INDEXNAME := TRACEFILES.GET_TABLEINDEX(TRACETABLE.TABLENAME);

            IF (SQL_FINAL IS NULL) THEN
                SQL_FINAL := SQL_MAIN;
            ELSE
                SQL_FINAL := SQL_FINAL || ' UNION ALL ' || SQL_MAIN;
            END IF;

            IF (INDEXNAME IS NULL) THEN
                SQL_FINAL := REPLACE(SQL_FINAL, '{INDEX_HINT}', '/*+ PARALLEL(32)*/');
            ELSE
                SQL_FINAL := REPLACE(SQL_FINAL, '{INDEX_HINT}', '/*+ INDEX(TT ' || INDEXNAME || ') */');
            END IF;
            
            IF TRACETABLE.P_RECORDSCOL <> 0 THEN
                COLPREFIX := 'P'; -- Use the Parser records as the technology was not analyzed...
            ELSE
                COLPREFIX := 'AZ'; -- Use the normal Analyzed records to determine the records loaded...
            END IF;

            SQL_FINAL := REPLACE(SQL_FINAL, '{TABLE_NAME}', TRACETABLE.TABLENAME);
            SQL_FINAL := REPLACE(SQL_FINAL, '{DATEFROM}', TO_CHAR(DATEFROM,'DD/MM/YYYY HH24MI'));
            SQL_FINAL := REPLACE(SQL_FINAL, '{PREFIX}', PARTPREFIX);
            SQL_FINAL := REPLACE(SQL_FINAL, '{WRITTEN_PREFIX}', COLPREFIX);
        END LOOP;
        SQL_FINAL := '
            CREATE TABLE TMP_LOADED_FILES COMPRESS NOLOGGING AS
            SELECT /*+ PARALLEL(32)*/
                FT_FileReference, L_ApplicationID, L_InputServer, NextProcess,
                COUNT(*) Loader_Files,
                SUM(CASE WHEN (L_Status = ''COMPLETE'') THEN 1 ELSE 0 END) L_Complete_Files,
                SUM(CASE WHEN L_Status IN (''PENDING'', ''BUSY'') THEN 1 ELSE 0 END) L_Pending_Files,
                SUM(CASE WHEN SUBSTR (L_Status, 1, 5) = ''ERROR'' THEN 1 ELSE 0 END) L_Error_Files,
                SUM(CASE WHEN NOT (L_Status IN (''COMPLETE'', ''PENDING'', ''BUSY'') OR SUBSTR (L_Status, 1, 5) = ''ERROR'') THEN 1 ELSE 0 END) L_Other_Files,
                SUM(CASE WHEN L_Status IN (''PENDING'', ''BUSY'') THEN AZ_RecordsWritten ELSE 0 END) L_PendingRecords,
                SUM(CalculatedPerFileSuccess) L_Calc_PerFileSuccess,
                SUM(L_RecordsSkipped) L_RecordsSkipped,
                SUM(L_RecordsDiscarded) L_RecordsDiscarded,
                SUM(L_RecordsRejected) L_RecordsRejected,
                SUM(L_SecondsToProcess) L_SecondsToProcess,
                MAX(L_TimeProcessed) L_TimeProcessed,
                SUM(L_FileSize) L_FileSize
            FROM
            ( ' || SQL_FINAL || ' )
            GROUP BY FT_FileReference, L_ApplicationID, L_InputServer, NextProcess
            ORDER BY 3';
        VDEBUG := '2 ';
        MAINTENANCE_PKG.LOGDEBUGMSG(DEBUG_PACKAGE, 'TMP_LOADED_FILES', RUNDATE, NULL, SQL_FINAL, NULL);
        EXECUTE IMMEDIATE (SQL_FINAL);
        IRECS := SQL%ROWCOUNT;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE1', IRECS, RUNDATE);
        STATUS := 'END';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
    EXCEPTION
        WHEN OTHERS THEN
            MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'ERROR', 'at ' || VDEBUG || SQLERRM ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
    END;

    PROCEDURE BUILDRECONDETAILS(RUNDATE IN DATE, DAYSBACK IN NUMBER DEFAULT 0)
    AS
        STATUS      VARCHAR2(50);
        STEPNAME    VARCHAR2(50);
        VDEBUG      VARCHAR2(50);
        IRECS       NUMBER;
        SQL_MAIN    VARCHAR2(32000);
        DATEFROM    DATE;
    BEGIN
        VDEBUG := '1 ';                
        STATUS := 'START';
        STEPNAME := 'RECONV2_BUILD_DETAILS';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'),RUNDATE);
        
        VDEBUG := '2 ';
        IF DAYSBACK > 0 THEN
            DATEFROM := TRUNC(SYSDATE - DAYSBACK);
        ELSE
            DATEFROM := TRACEFILES.GETOLDESTTRACKPARS;
        END IF;
        
        VDEBUG := '3 ';
        CREATE_PARTITIONS_TO_CURRENT('RECON_TRACK_FILES', 'RT');
        VDEBUG := '4 ';
        TRUNCATE_PARTITIONS_TO_CURRENT(DATEFROM, 'RECON_TRACK_FILES', STEPNAME, 'RT'); --truncate can only be called after create because they share variables

        VDEBUG := '5 ';
        IRECS := 0;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME,'COMMENT1','INSERT STARTED:'||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'), RUNDATE);
        SQL_MAIN :=
        q'[
            INSERT INTO RECON_TRACK_FILES
            (
                FT_Filereference, FT_InputServer, TechName, FeedName, FT_SwitchID, MSC_FromFilename, File_Sequence, P_MinCDRDate, P_MaxCDRDate,
                Track_Files, Track_Error_Files, Track_Zero_Bytes, Track_Pending_Files, Track_Other_Files, Track_FileSize,
                P_Files, P_Error_Files, P_Pending_Files, P_Other_Files,
                AZ_Files, AZ_Error_Files, AZ_Pending_Files, AZ_Other_Files, AZ_Files_Expanded, AZ_Error_Files_Expanded, AZ_Pending_Files_Expanded, AZ_Other_Files_Expanded,
                L_Files, L_Error_Files, L_Pending_Files, L_Other_Files, L_Files_Expanded, L_Error_Files_Expanded, L_Pending_Files_Expanded, L_Other_Files_Expanded,
                P_TotalRecords, P_RecordsWritten, P_RecordsErrored, P_RecordsSkipped,
                AZ_TotalRecords, AZ_RecordsWritten, AZ_RecordsPending, AZ_RecordsErrored, AZ_RecordsSkipped,
                L_CalcPerFileSuccess, L_PendingRecords, L_RecordsSkipped, L_RecordsDiscarded, L_RecordsRejected,
                FT_Status, FT_TimeProcessed, FT_FileDateTime, P_Status, P_TimeProcessed, P_SecondsToProcess, P_FirstSequenceNo, P_LastSequenceNo,
                AZ_TimeProcessed, AZ_SecondsToProcess, AZ_FirstSequenceNo, AZ_LastSequenceNo, L_SecondsToProcess, L_TimeProcessed, FT_InputFilename,
                P_InputServer, AZ_InputServer, L_InputServer, P_ApplicationID, AZ_ApplicationID, L_ApplicationID, P_FileSize, AZ_FileSize, L_FileSize
            )
            SELECT /*+ full(P)  full(AZ)  full(L) parallel(32) */
                T.FT_Filereference, T.FT_InputServer, T.TechName, T.FeedName, T.FT_SwitchID, T.MSC_FromFilename, T.File_Sequence, P.P_MinCDRDate, P.P_MaxCDRDate,
                --TRACKER FILES,
                CASE WHEN T.FT_Status IN ('COMPLETE') THEN 1 ELSE 0 END Track_Files,
                CASE WHEN T.FT_Status IN ('ERROR') THEN 1 ELSE 0 END Track_Error_Files,
                CASE WHEN T.FT_FileSize = 0 THEN 1 ELSE 0 END Track_Zero_Bytes,
                CASE WHEN T.FT_Status IN ('PENDING', 'BUSY') THEN 1 ELSE 0 END Track_Pending_Files,
                CASE WHEN T.FT_Status NOT IN ('PENDING', 'BUSY', 'ERROR', 'COMPLETE') THEN 1 ELSE 0 END Track_Other_Files,
                T.FT_FileSize Track_FileSize,
                --PARSER FILES
                CASE WHEN P.P_Status IN ('COMPLETE') THEN 1 ELSE 0 END P_Files,
                CASE WHEN P.P_Status IN ('ERROR') THEN 1 ELSE 0 END P_Error_Files,
                CASE WHEN P.P_Status IN ('PENDING', 'BUSY') THEN 1 ELSE 0 END P_Pending_Files,
                CASE WHEN P.P_Status NOT IN ('PENDING', 'BUSY', 'ERROR', 'COMPLETE') THEN 1 ELSE 0 END P_Other_Files,
                --ANALYZER FILES CALC
                CASE WHEN AZ.Analyser_Files = AZ.AZ_Complete_Files THEN 1 ELSE 0 END AZ_Files,
                CASE WHEN AZ.AZ_Error_Files > 0 THEN 1 ELSE 0 END AZ_Error_Files,
                CASE WHEN (AZ.AZ_Pending_Files > 0 AND AZ.AZ_Error_Files = 0) OR (AZ.FT_FileReference IS NULL AND AZ.NextProcess = 2) THEN 1 ELSE 0 END AZ_Pending_Files, --if one file in the batch errored count as error
                CASE WHEN AZ.Analyser_Files <> AZ.AZ_Error_Files AND NVL (AZ.AZ_Pending_Files, 0) > 0 AND NVL (AZ.AZ_Error_Files, 0) = 0 THEN 1 ELSE 0 END AZ_Other_Files,
                --ANALYZER FILES Actual expanded
                NVL(AZ.AZ_Complete_Files, 0) AZ_Files_Expanded, NVL(AZ.AZ_Error_Files, 0) AZ_Error_Files_Expanded, NVL(AZ.AZ_Pending_Files, 0) AZ_Pending_Files_Expanded, NVL(AZ.AZ_Other_Files, 0) AZ_Other_Files_Expanded,
                --LOADER FILES CALC
                CASE WHEN L.Loader_Files = L.L_Complete_Files THEN 1 ELSE 0 END L_Files,
                CASE WHEN L.L_Error_Files > 0 THEN 1 ELSE 0 END L_Error_Files,
                CASE WHEN L.L_Pending_Files > 0 AND L.L_Error_Files = 0 THEN 1 ELSE 0 END L_Pending_Files, --if one file in the batch errored count as error
                CASE WHEN L.Loader_Files <> L.L_Complete_Files AND NOT (L.L_Error_Files > 0) AND NOT (L.L_Pending_Files > 0 AND L.L_Error_Files = 0) THEN 1 ELSE 0 END L_Other_Files,
                --LOADER FILES Actual expanded
                NVL(L.L_Complete_Files, 0) L_Files_Expanded, NVL(L.L_Error_Files, 0) L_Error_Files_Expanded, NVL(L.L_Pending_Files, 0) L_Pending_Files_Expanded, NVL(L.L_Other_Files, 0) L_Other_Files_Expanded,
                --parser CDRs
                NVL(P.P_TotalRecords, 0) P_TotalRecords, NVL(P.P_RecordsWritten, 0) P_RecordsWritten, NVL(P.P_RecordsErrored, 0) P_RecordsErrored, NVL(P.P_RecordsSkipped, 0) P_RecordsSkipped,
                --AZ CDRs
                NVL(AZ.AZ_TotalRecords, 0) AZ_TotalRecords, NVL(AZ.AZ_RecordsWritten, 0) AZ_RecordsWritten,
                CASE WHEN P.P_Status IN ('PENDING', 'BUSY') THEN GREATEST (NVL(P.P_RecordsWritten, 0) - NVL(AZ.AZ_RecordsWritten, 0), 0) ELSE 0 END AZ_RecordsPending,
                NVL(AZ.AZ_RecordsErrored, 0) AZ_RecordsErrored, NVL(AZ.AZ_RecordsSkipped, 0) AZ_RecordsSkipped,
                --Load CDRs
                NVL(L.L_Calc_PerFileSuccess, 0) L_CalcPerFileSuccess, NVL(L.L_PendingRecords, 0) L_PendingRecords, NVL(L.L_RecordsSkipped, 0) L_RecordsSkipped, NVL(L.L_RecordsDiscarded, 0) L_RecordsDiscarded, NVL(L.L_RecordsRejected, 0) L_RecordsRejected,
                --Status and timing
                T.FT_Status, T.FT_TimeProcessed, T.FT_FileDateTime, P.P_Status, P.P_TimeProcessed, P.P_SecondsToProcess, P.P_FirstSequenceNo, P.P_LastSequenceNo,
                AZ.AZ_TimeProcessed, AZ.AZ_SecondsToProcess, AZ.AZ_FirstSequenceNo, AZ.AZ_LastSequenceNo, L.L_SecondsToProcess, L.L_TimeProcessed, T.FT_InputFilename,
                P.P_InputServer, AZ.AZ_InputServer, L.L_InputServer, P.P_ApplicationID, AZ.AZ_ApplicationID, L.L_ApplicationID, P.P_FileSize, AZ.AZ_FileSize, L.L_FileSize
            FROM TMP_TRACKED_FILES T, TMP_PARSED_FILES P, TMP_ANALYZED_FILES AZ, TMP_LOADED_FILES L
            WHERE FT_TimeProcessed IS NOT NULL 
            AND T.FT_FileReference = P.FT_FileReference(+)
            AND T.FT_FileReference = AZ.FT_FileReference(+)
            AND T.FT_FileReference = L.FT_FileReference(+)
        ]';

        MAINTENANCE_PKG.LOGDEBUGMSG(DEBUG_PACKAGE, 'RECON_TRACK_FILES', RUNDATE, NULL, SQL_MAIN, NULL);
        EXECUTE IMMEDIATE (SQL_MAIN);
        IRECS := SQL%ROWCOUNT;
        COMMIT;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1','INSERT COMPLETED:'||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'), RUNDATE);
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE1', IRECS, RUNDATE);

        STATUS := 'END';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
    EXCEPTION
        WHEN OTHERS THEN
            MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'ERROR', 'at ' || VDEBUG || SQLERRM ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
    END;

    PROCEDURE BUILDRECONSUMMARY(RUNDATE IN DATE)
    AS
        STATUS      VARCHAR2(50);
        STEPNAME    VARCHAR2(50);
        VDEBUG      VARCHAR2(50);
        IRECS       NUMBER;
        SQL_MAIN    VARCHAR2(32000);
    BEGIN
        STATUS := 'START';
        STEPNAME := 'RECONV2_BUILD_SUMMARY';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'),RUNDATE);
        VDEBUG := '1 ';
        MAINTENANCE_PKG.MANAGEPARTITIONS(TRUNC(SYSDATE), 'RECON_TRACK_FILES_SUMMARY', 'RT_');

        VDEBUG := '2 ';
        IRECS := 0;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME,'COMMENT1','INSERT STARTED:'||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'), RUNDATE);
        SQL_MAIN :=
        q'[
            INSERT INTO RECON_TRACK_FILES_SUMMARY
            (
                RunAt, FileDate, DataDate, TechName, MSC_FromFileName, Track_Files, Track_Error_Files, Track_Zero_Bytes, Track_Pending_Files, Track_Other_Files, Track_FileSize,
                P_Files, P_Error_Files, P_Pending_Files, P_Other_Files, AZ_Files, AZ_Error_Files, AZ_Pending_Files, AZ_Other_Files, L_Files, L_Error_Files, L_Pending_Files, L_Other_Files,
                AZ_Files_Expanded, AZ_Error_Files_Expanded, AZ_Pending_Files_Expanded, AZ_Other_Files_Expanded, L_Files_Expanded, L_Error_Files_Expanded, L_Pending_Files_Expanded, L_Other_Files_Expanded,
                P_TotalRecords, P_RecordsWritten, P_RecordsErrored, P_RecordsSkipped, AZ_RecordsWritten, AZ_RecordsErrored, AZ_RecordsPending, AZ_RecordsSkipped,
                L_CalcPerFileSuccess, L_PendingRecords, L_RecordsSkipped, L_RecordsDiscarded, L_RecordsRejected,
                FileDate_Hour, DataDate_Hour, InputServer, MSC_ID, P_Status, P_Date, P_Date_Hour, AZ_Date, AZ_Date_Hour, L_Date, L_Date_Hour, P_SecondsToProcess, AZ_SecondsToProcess, L_SecondsToProcess,
                FileType
            )
            SELECT /*+ PARALLEL (32)*/
                SYSDATE, 
                TRUNC(FT_FILEDATETIME), 
                NVL(TRUNC(P_MAXCDRDATE,'DD'), 
                TO_DATE(SUBSTR(CDR.FILERECON_FUNCTIONS.GETDATEFROMFILEFILENAME(TECHNAME, FT_INPUTFILENAME, FT_FILEDATETIME,P_MAXCDRDATE), 1, 8), 'YYYYMMDD')), 
                TECHNAME, 
                MSC_FROMFILENAME,
                SUM(CASE WHEN RNK = 1 THEN TRACK_FILES ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN TRACK_ERROR_FILES ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN TRACK_ZERO_BYTES ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN TRACK_PENDING_FILES ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN TRACK_OTHER_FILES ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN TRACK_FILESIZE ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN P_FILES ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN P_ERROR_FILES ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN P_PENDING_FILES ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN P_OTHER_FILES ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN AZ_FILES ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN AZ_ERROR_FILES ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN AZ_PENDING_FILES ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN AZ_OTHER_FILES ELSE NULL END), 
                SUM(L_FILES), 
                SUM(L_ERROR_FILES),
                SUM(L_PENDING_FILES), 
                SUM(L_OTHER_FILES),
                SUM(CASE WHEN RNK = 1 THEN AZ_FILES_EXPANDED ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN AZ_ERROR_FILES_EXPANDED ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN AZ_PENDING_FILES_EXPANDED ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN AZ_OTHER_FILES_EXPANDED ELSE NULL END), 
                SUM(L_FILES_EXPANDED), 
                SUM(L_ERROR_FILES_EXPANDED), 
                SUM(L_PENDING_FILES_EXPANDED),
                SUM(L_OTHER_FILES_EXPANDED),
                SUM(CASE WHEN RNK = 1 THEN P_TOTALRECORDS ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN P_RECORDSWRITTEN ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN P_RECORDSERRORED ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN P_RECORDSSKIPPED ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN AZ_RECORDSWRITTEN ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN AZ_RECORDSERRORED ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN AZ_RECORDSPENDING ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN AZ_RECORDSSKIPPED ELSE NULL END), 
                SUM(L_CALCPERFILESUCCESS), 
                SUM(L_PENDINGRECORDS), 
                SUM(L_RECORDSSKIPPED), 
                SUM(L_RECORDSDISCARDED), 
                SUM(L_RECORDSREJECTED),
                TRUNC(FT_FILEDATETIME,'HH24'), 
                TRUNC(P_MAXCDRDATE,'HH24'), 
                FT_INPUTSERVER, FT_SWITCHID, 
                P_STATUS,
                TRUNC(P_TIMEPROCESSED),
                TRUNC(P_TIMEPROCESSED,'HH24'), 
                TRUNC(AZ_TIMEPROCESSED), TRUNC(AZ_TIMEPROCESSED,'HH24'), TRUNC(L_TIMEPROCESSED), TRUNC(L_TIMEPROCESSED,'HH24'),
                SUM(CASE WHEN RNK = 1 THEN P_SECONDSTOPROCESS ELSE NULL END), 
                SUM(CASE WHEN RNK = 1 THEN AZ_SECONDSTOPROCESS ELSE NULL END), 
                SUM(L_SECONDSTOPROCESS),
                CDR.FILERECON_FUNCTIONS.GETTYPEFROMFILENAME(TECHNAME, FT_INPUTFILENAME) FILETYPE
            FROM
            (                
                SELECT 
                    /*+parallel(32)*/
                    RTF.*, 
                    ROW_NUMBER() OVER (PARTITION BY FT_FILEREFERENCE ORDER BY FT_FILEREFERENCE ASC) RNK
                FROM RECON_TRACK_FILES RTF
                WHERE FT_TIMEPROCESSED >= ADD_MONTHS(TRUNC(SYSDATE,'MM'),-1)
                AND ((TRUNC(P_MAXCDRDATE) >= ADD_MONTHS(TRUNC(SYSDATE,'MM'),-1)) OR (P_MAXCDRDATE IS NULL))
                AND UPPER(TECHNAME) NOT LIKE '%REPROCESSED%'
            )    
            GROUP BY TRUNC(FT_FILEDATETIME), NVL(TRUNC(P_MAXCDRDATE,'DD'), TO_DATE(SUBSTR(CDR.FILERECON_FUNCTIONS.GETDATEFROMFILEFILENAME(TECHNAME, FT_INPUTFILENAME, FT_FILEDATETIME,P_MAXCDRDATE), 1, 8), 'YYYYMMDD')), 
            TECHNAME, MSC_FROMFILENAME, TRUNC(FT_FILEDATETIME,'HH24'), TRUNC(P_MAXCDRDATE,'HH24'), FT_INPUTSERVER, FT_SWITCHID, P_STATUS,
            TRUNC(P_TIMEPROCESSED), TRUNC(P_TIMEPROCESSED,'HH24'), TRUNC(AZ_TIMEPROCESSED), TRUNC(AZ_TIMEPROCESSED,'HH24'), TRUNC(L_TIMEPROCESSED), TRUNC(L_TIMEPROCESSED,'HH24'),
            CDR.FILERECON_FUNCTIONS.GETTYPEFROMFILENAME(TECHNAME, FT_INPUTFILENAME)
            ]';

        MAINTENANCE_PKG.LOGDEBUGMSG(DEBUG_PACKAGE, 'RECON_TRACK_FILES_SUMMARY', RUNDATE, NULL, SQL_MAIN, NULL);
        EXECUTE IMMEDIATE (SQL_MAIN);
        IRECS := SQL%ROWCOUNT;
        COMMIT;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1','INSERT COMPLETED:'||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'), RUNDATE);
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE1', IRECS, RUNDATE);
        
        -----------------------------------------------------------------------------------------
        --Merge LKP_MSC
        -----------------------------------------------------------------------------------------
        VDEBUG := '3 ';
        IRECS:= 0;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT2', 'MERGE LKP_MSC STARTED:' ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
        SQL_MAIN :=
            q'[
                MERGE INTO LKP_MSC M
                USING
                (                    
                    SELECT /*+ PARALLEL (32)*/ DISTINCT
                        TechName, NVL(MSC_FromFileName, 'unknown') MSC_FromFileName
                    FROM RECON_TRACK_FILES_SUMMARY
                    WHERE RunAt >= TRUNC(SYSDATE)                                        
                ) R ON (M.TechName = R.TechName AND M.MSC_FromFileName = R.MSC_FromFileName)
                WHEN NOT MATCHED THEN
                INSERT (TECHNAME, MSC_FROMFILENAME, MSC_DESC) VALUES (R.TechName, R.MSC_FromFileName, R.MSC_FromFileName)
            ]';        
        MAINTENANCE_PKG.LOGDEBUGMSG(DEBUG_PACKAGE, 'MERGE LKP_MSC', RUNDATE, NULL, SQL_MAIN, NULL);
        EXECUTE IMMEDIATE SQL_MAIN; 
        IRECS := SQL%ROWCOUNT;       
        COMMIT;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT2','MERGE LKP_MSC COMPLETED:'||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'), RUNDATE);
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE2', IRECS, RUNDATE);

        STATUS := 'END';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
    EXCEPTION
        WHEN OTHERS THEN
            MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'ERROR', 'at ' || VDEBUG || SQLERRM ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
    END;
    PROCEDURE MISSINGFILES_MAIN (RUNDATE IN DATE)
    AS
        STATUS      VARCHAR2(50);
        STEPNAME    VARCHAR2(50);
        VDEBUG      VARCHAR2(50);
        IRECS       NUMBER;
        SQL_MAIN    VARCHAR2(32000);
        DTPOSTFIX VARCHAR2(15);
    BEGIN
        STATUS := 'START';
        
        /*LOG MAIN MISSINGFILES*/
        STEPNAME := 'RECON_MISSINGFILES_MAIN';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'),RUNDATE);
        
        STEPNAME := 'RECONV2_EXPECTED_SEQ_BASE';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'),RUNDATE);
        VDEBUG := '1 ';
        
        DTPOSTFIX := TO_CHAR(RUNDATE,'YYYYMMDD');
        
        STATUS := 'INSERTING_EXPECTEDFILES_BASE';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', STATUS ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
        
        MAINTENANCE_PKG.DROP_TABLE('TMPV2_EXPECTEDBASE_'||DTPOSTFIX);
        
        DELETE FROM  RECON_EXPECTED_SEQ_BASE_V2;
        IRECS := SQL%ROWCOUNT;
        COMMIT;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE1', IRECS, RUNDATE);

        IRECS := 0;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME,'COMMENT1','INSERT STARTED:'||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'), RUNDATE);
        SQL_MAIN :=
        q'[
            CREATE TABLE  TMPV2_EXPECTEDBASE_{DTPOSTFIX} COMPRESS AS
             SELECT
                DISTINCT
                DATEKEY, TECHNAME, 
                CDR.FILERECON_FUNCTIONS.GETTYPEFROMFILENAME(TECHNAME, FT_INPUTFILENAME) FILETYPE, MSC_FROMFILENAME,
                FIRST_VALUE(PRIOR_SEQ) OVER (PARTITION BY DATEKEY,FILESEQ, TECHNAME, MSC_FROMFILENAME ORDER BY NVL(P_MAXCDRDATE,FT_FILEDATETIME)) FROM_SEQ,
                FIRST_VALUE(FILE_SEQUENCE) OVER (PARTITION BY DATEKEY,FILESEQ, TECHNAME, MSC_FROMFILENAME ORDER BY NVL(P_MAXCDRDATE,FT_FILEDATETIME) DESC) TO_SEQ,
                MIN(PRIOR_SEQ) OVER (PARTITION BY DATEKEY,FILESEQ, TECHNAME, MSC_FROMFILENAME) MIN_SEQ,
                MAX(FILE_SEQUENCE) OVER (PARTITION BY DATEKEY,FILESEQ, TECHNAME, MSC_FROMFILENAME) MAX_SEQ,
                FILESEQ
            FROM
            (
               SELECT
                    SQL1.FT_INPUTFILENAME, 
                    SQL1.MSC_FROMFILENAME,
                    SQL1.TECHNAME,
                    SQL1.FT_FILEDATETIME,
                    SQL1.P_MAXCDRDATE,
                    SQL1.DATEKEY,
                    SQL1.FILESEQ,
                    SQL1.FILE_SEQUENCE,
                    CASE 
                        WHEN LAG(FILE_SEQUENCE,1,0) OVER (PARTITION BY  TECHNAME, MSC_FROMFILENAME ORDER BY FILE_SEQUENCE) <= 0 THEN CAST(FILE_SEQUENCE AS NUMBER) 
                        ELSE LAG(FILE_SEQUENCE,1,0) OVER (PARTITION BY  TECHNAME, MSC_FROMFILENAME ORDER BY FILE_SEQUENCE)+1 
                    END PRIOR_SEQ  
                FROM
                (    
                    SELECT /*+parallel(32)*/
                        DISTINCT FT_INPUTFILENAME, 
                        MSC_FROMFILENAME, 
                        TECHNAME, 
                        FT_FILEDATETIME,
                        CDR.FILERECON_FUNCTIONS.GETSEQFROMFILESEQUENCE(TECHNAME, FILE_SEQUENCE, FT_INPUTFILENAME) FILESEQ, 
                        FILE_SEQUENCE,
                        P_MAXCDRDATE,
                        SUBSTR(CDR.FILERECON_FUNCTIONS.GETDATEFROMFILEFILENAME(TECHNAME, FT_INPUTFILENAME, FT_FILEDATETIME), 1, 8) DATEKEY
                    FROM RECON_TRACK_FILES
                    WHERE FT_TIMEPROCESSED >= ADD_MONTHS(TRUNC(TO_DATE('{DATE}','DD/MM/YYYY'), 'MM'),-1)
                    AND FT_TIMEPROCESSED >= TO_DATE('1-DEC-2018','DD/MM/YYYY')
                    AND FILE_SEQUENCE IS NOT NULL 
                    AND FILE_SEQUENCE > '0' 
                    AND MAINTENANCE_PKG.IS_NUMBER(FILE_SEQUENCE) = 'Y'
                ) SQL1
                WHERE DATEKEY BETWEEN TO_CHAR(TO_DATE('{DATE}','DD/MM/YYYY')-2,'YYYYMMDD')  AND TO_CHAR(TO_DATE('{DATE}','DD/MM/YYYY')+1,'YYYYMMDD')
            )
            WHERE DATEKEY BETWEEN TO_CHAR(TO_DATE('{DATE}','DD/MM/YYYY'),'YYYYMMDD')  AND TO_CHAR(TO_DATE('{DATE}','DD/MM/YYYY'),'YYYYMMDD') 
            ]';
        SQL_MAIN := REPLACE(SQL_MAIN, '{DATE}',  TO_CHAR(RUNDATE,'DD/MM/YYYY'));
        SQL_MAIN := REPLACE(SQL_MAIN, '{DTPOSTFIX}',  DTPOSTFIX);             
        MAINTENANCE_PKG.LOGDEBUGMSG(DEBUG_PACKAGE, 'RECON_EXPECTED_SEQ_BASE', RUNDATE, NULL, SQL_MAIN, NULL);
        EXECUTE IMMEDIATE (SQL_MAIN);
        IRECS := SQL%ROWCOUNT;
        COMMIT;
        
        MAINTENANCE_PKG.EXCHANGE_PARTITION('RECON_EXPECTED_SEQ_BASE_V2',' DATA_PARTITION', 'TMPV2_EXPECTEDBASE_'||DTPOSTFIX);
        
        STATUS := 'INSERTING_EXPECTEDFILES_BASE COMPLETED';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', STATUS ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
        
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE2', IRECS, RUNDATE);

        STATUS := 'END';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
        
        STATUS := 'KICKED OFF MISSING_FILE_SEQUENCES';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT2', STATUS ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
        
        BUILDMISSINGFILESEQ(RUNDATE);
        
    EXCEPTION
        WHEN OTHERS THEN
            MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'ERROR', 'at ' || VDEBUG || SQLERRM ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
    END;
    
    PROCEDURE BUILDMISSINGFILESEQ(RUNDATE IN DATE)
    AS
        STATUS      VARCHAR2(5000);
        STEPNAME    VARCHAR2(50);
        VDEBUG      VARCHAR2(50);
        IRECS       NUMBER;
        
        CURSOR GENERAL_RECS IS 
        SELECT  
            DISTINCT TECHNAME, 
            MSC_FROMFILENAME, 
            FILETYPE, DATEKEY, 
            FILESEQ, 
            TO_SEQ, 
            FROM_SEQ , 
            MAX_SEQ, 
            MIN_SEQ
        FROM RECON_EXPECTED_SEQ_BASE_V2
        ORDER BY 1;
        
    BEGIN
        STATUS := 'START';
        STEPNAME := 'RECONV2_BUILD_MISSING_FILE_SEQ';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'), RUNDATE);

        VDEBUG := '1 ';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', 'DELETE RECON_SEQ_NUMBER_CHECK STARTED:' ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'), RUNDATE);
        DELETE FROM RECON_SEQ_NUMBER_CHECK_V2;
        IRECS := SQL%ROWCOUNT;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE1', IRECS);
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', 'DELETE RECON_SEQ_NUMBER_CHECK COMPLETED:' ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'), RUNDATE);
        COMMIT;

        VDEBUG := '2 ';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', 'DELETE RECON_MISSING_FILE_SEQ STARTED:' ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'), RUNDATE);
        DELETE FROM RECON_MISSING_FILE_SEQ_V2 WHERE TRUNC(RUN_AT) = TRUNC(RUNDATE);
        IRECS := SQL%ROWCOUNT;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE1', IRECS);
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', 'DELETE RECON_MISSING_FILE_SEQ COMPLETED:' ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'), RUNDATE);
        COMMIT;

        VDEBUG := '3 ';
        IRECS := 0;
        
        --LOOP THROUGH ALL TECHNOLOGY AND MSCS TO CREATE A LIST OF EXPECTED FILES
        STATUS := 'RECON_EXPECTED_FILES: RUNNING';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', STATUS ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
        
        STATUS := 'START';
        STEPNAME := 'RECONV2_EXPECTED_FILES';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'), RUNDATE);
        
        DELETE FROM CDR.RECON_SEQ_NUMBER_CHECK_V2;
        IRECS := SQL%ROWCOUNT;
        COMMIT;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE2', IRECS);
             
        FOR G_REC IN GENERAL_RECS
        LOOP
        
            IF G_REC.TO_SEQ > G_REC.FROM_SEQ THEN      
             
                STATUS := 'TECHNOLOGY: '||G_REC.TECHNAME||' - '||G_REC.MSC_FROMFILENAME||' SEQUENCES - '||G_REC.TO_SEQ ||' - '|| G_REC.FROM_SEQ;
                MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', STATUS ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
                                    
                INSERT INTO CDR. RECON_SEQ_NUMBER_CHECK_V2(DATEKEY, TECHNAME, FILETYPE, MSC, SEQUENCE_GROUP, SEQ)
                SELECT 
                    DATEKEY,
                    TECHNAME, 
                    FILETYPE,
                    MSC_FROMFILENAME,
                    FILESEQ SEQUENCE_GROUP,
                    (FROM_SEQ+ROWNUM) -1  SEQ
                FROM
                (
                    SELECT DISTINCT 
                        DATEKEY,
                        TECHNAME, 
                        FILETYPE,
                        MSC_FROMFILENAME,
                        FILESEQ,
                        TO_SEQ,
                        FROM_SEQ,
                        TO_SEQ-FROM_SEQ  DIFF  
                    FROM RECON_EXPECTED_SEQ_BASE_V2 
                    WHERE  DATEKEY = G_REC.DATEKEY
                    AND TECHNAME = G_REC.TECHNAME 
                    AND FILETYPE = G_REC.FILETYPE
                    AND MSC_FROMFILENAME = G_REC.MSC_FROMFILENAME
                    AND FILESEQ = G_REC.FILESEQ
                ) AA
                CONNECT BY ROWNUM <= DIFF + 1;
                
                IRECS := IRECS + 1;  
                                                   
                COMMIT; 
            ELSE
                    
                --seq resets
                FOR I IN G_REC.FROM_SEQ .. G_REC.MAX_SEQ
                LOOP
                
                    STATUS := 'TECHNOLOGY: '||G_REC.TECHNAME||' - '||G_REC.MSC_FROMFILENAME||' SEQUENCES - '||G_REC.TO_SEQ ||' - '|| G_REC.FROM_SEQ;
                    MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', STATUS ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
                                                        
                    INSERT INTO CDR. RECON_SEQ_NUMBER_CHECK_V2(DATEKEY, TECHNAME, FILETYPE, MSC, SEQUENCE_GROUP, SEQ)
                    SELECT 
                        DATEKEY,
                        TECHNAME, 
                        FILETYPE,
                        MSC_FROMFILENAME,
                        FILESEQ SEQUENCE_GROUP,
                        (FROM_SEQ+ROWNUM) -1  SEQ
                    FROM
                    (
                        SELECT DISTINCT 
                            DATEKEY,
                            TECHNAME, 
                            FILETYPE,
                            MSC_FROMFILENAME,
                            FILESEQ,
                            TO_SEQ,
                            FROM_SEQ,
                            TO_SEQ-FROM_SEQ  DIFF  
                        FROM RECON_EXPECTED_SEQ_BASE_V2 
                        WHERE  DATEKEY = G_REC.DATEKEY
                        AND TECHNAME = G_REC.TECHNAME 
                        AND FILETYPE = G_REC.FILETYPE
                        AND MSC_FROMFILENAME = G_REC.MSC_FROMFILENAME
                        AND FILESEQ = G_REC.FILESEQ
                    ) AA
                    CONNECT BY ROWNUM <= DIFF + 1;
                                   
                COMMIT; 
                
                IRECS := IRECS + 1;  
                
                END LOOP;

                FOR I IN G_REC.MIN_SEQ  .. G_REC.TO_SEQ
                LOOP
                                        STATUS := 'TECHNOLOGY: '||G_REC.TECHNAME||' - '||G_REC.MSC_FROMFILENAME||' SEQUENCES - '||G_REC.TO_SEQ ||' - '|| G_REC.FROM_SEQ;
                    MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', STATUS ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
                                                        
                    INSERT INTO CDR. RECON_SEQ_NUMBER_CHECK_V2(DATEKEY, TECHNAME, FILETYPE, MSC, SEQUENCE_GROUP, SEQ)
                    SELECT 
                        DATEKEY,
                        TECHNAME, 
                        FILETYPE,
                        MSC_FROMFILENAME,
                        FILESEQ SEQUENCE_GROUP,
                        (FROM_SEQ+ROWNUM) -1  SEQ
                    FROM
                    (
                        SELECT DISTINCT 
                            DATEKEY,
                            TECHNAME, 
                            FILETYPE,
                            MSC_FROMFILENAME,
                            FILESEQ,
                            TO_SEQ,
                            FROM_SEQ,
                            TO_SEQ-FROM_SEQ  DIFF  
                        FROM RECON_EXPECTED_SEQ_BASE_V2 
                        WHERE  DATEKEY = G_REC.DATEKEY
                        AND TECHNAME = G_REC.TECHNAME 
                        AND FILETYPE = G_REC.FILETYPE
                        AND MSC_FROMFILENAME = G_REC.MSC_FROMFILENAME
                        AND FILESEQ = G_REC.FILESEQ
                    ) AA
                    CONNECT BY ROWNUM <= DIFF + 1;
                    
                    COMMIT;
                    
                    IRECS := IRECS + 1;
                END LOOP;
            END IF;
        END LOOP;
        
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE2', IRECS);
        
        STATUS := 'EXPECTED FILES: '||IRECS;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT2', STATUS);
        
        STATUS := 'END';
        STEPNAME := 'RECONV2_EXPECTED_FILES';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'), RUNDATE);
        
        
        STEPNAME := 'RECONV2_BUILD_MISSING_FILE_SEQ';
        
        STATUS := 'INSERT_MISSING_FILES: RUNNING';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', STATUS ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
        
        VDEBUG := '4 ';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT3', 'INSERT MISSING FILES STARTED:' ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
        INSERT INTO RECON_MISSING_FILE_SEQ_V2(TECHNAME, FILETYPE, DT, RUN_AT, MSC, SEQ)
        SELECT /*+ PARALLEL(32)*/ DISTINCT
            C.TECHNAME, 
            C.FILETYPE, 
            TO_DATE(C.DATEKEY,'YYYYMMDD') DT,
            TRUNC(RUNDATE) RUN_AT, 
            C.MSC, 
            C.SEQ
        FROM
        (
            SELECT /*+ PARALLEL(32)*/
                *
            FROM
            (
                SELECT /*+ PARALLEL(32)*/
                    FT_INPUTFILENAME, TECHNAME, MSC_FROMFILENAME MSC, FT_FILEDATETIME,
                    REPLACE(REPLACE(FILE_SEQUENCE, 'E', ''), 'M', '') SEQ,
                    SUBSTR(FILERECON_FUNCTIONS.GETDATEFROMFILEFILENAME(TECHNAME, FT_INPUTFILENAME, FT_FILEDATETIME), 1, 8) DATEKEY
                FROM RECON_TRACK_FILES
                WHERE FT_TIMEPROCESSED BETWEEN TRUNC(RUNDATE) - 14 AND TRUNC(RUNDATE) + 30
                AND FT_TIMEPROCESSED >= TO_DATE('1-DEC-2018','DD/MM/YYYY') 
            )
            WHERE TO_DATE(DATEKEY,'YYYYMMDD') BETWEEN TRUNC(RUNDATE) AND TRUNC(RUNDATE)
        ) A,
        RECON_SEQ_NUMBER_CHECK_V2 C
        WHERE C.TECHNAME = A.TECHNAME(+)
        AND C.MSC = A.MSC(+)
        AND C.SEQ = A.SEQ(+)
--        AND C.DATEKEY = A.DATEKEY
        AND FT_INPUTFILENAME IS NULL; --just the missing ones
        
        IRECS := SQL%ROWCOUNT;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE3', IRECS);
        
        COMMIT;

        VDEBUG := '5 ';
        STATUS := 'REMOVE_CONFIRMED_EXCEPTIONS: RUNNING';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', STATUS ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));

        DELETE FROM RECON_MISSING_FILE_SEQ_V2 WHERE (TECHNAME, DT, MSC, SEQ) IN (SELECT TECHNAME, DT, MSC, SEQ FROM RECON_MISSING_FILE_SEQ_EX);
        
        IRECS := SQL%ROWCOUNT;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE4', IRECS);
        
        COMMIT;
      
        STATUS := 'END';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
        
        /*LOG MAIN MISSINGFILES*/
        STEPNAME := 'RECON_MISSINGFILES_MAIN';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'),RUNDATE);
        
    EXCEPTION
        WHEN OTHERS THEN
            MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'ERROR', 'at ' || VDEBUG || SQLERRM ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
    END;
    
    PROCEDURE BUILDPERFORMANCEDETAILS(RUNDATE IN DATE, DAYSBACK IN NUMBER DEFAULT 0)
    AS
        STATUS      VARCHAR2(50);
        STEPNAME    VARCHAR2(50);
        VDEBUG      VARCHAR2(50);
        IRECS       NUMBER;
        SQL_MAIN    VARCHAR2(32000);
        DATEFROM    DATE;
    BEGIN
        VDEBUG := '1 ';               
        STATUS := 'START';
        STEPNAME := 'RECONV2_BUILD_PERFORMANCE_DETAILS';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'),RUNDATE);       
        
        VDEBUG := '2 ';
        IF DAYSBACK > 0 THEN
            DATEFROM := TRUNC(SYSDATE - DAYSBACK);
        ELSE
            DATEFROM := TRACEFILES.GETOLDESTTRACKPARS;
        END IF;
        
        VDEBUG := '3 ';
        CREATE_PARTITIONS_TO_CURRENT('RECON_PERFORMANCE_DETAILS', 'RP');
        VDEBUG := '4 ';
        TRUNCATE_PARTITIONS_TO_CURRENT(DATEFROM, 'RECON_PERFORMANCE_DETAILS', STEPNAME, 'RP'); --truncate can only be called after create because they share variables

        VDEBUG := '5 ';
        IRECS := 0;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME,'COMMENT1','INSERT STARTED:'||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'), RUNDATE);
        SQL_MAIN :=
        q'[
            INSERT INTO RECON_PERFORMANCE_DETAILS
            (
                Dt, AppType, Application, ApplicationID, InputServer, File_Cnt, KB, Records_Read, Records_Written, Files_Per_Sec, KB_Per_Sec, CDRS_Per_Sec
            )
            SELECT /*+ PARALLEL(32)*/
                A.Dt, T.AppType, FW.Description Application, A.ApplicationID, A.InputServer,
                A.File_Cnt, ROUND((A.File_Size/1024), 2) KB, A.Records_Read, A.Records_Written,
                ROUND((A.File_Cnt/60), 2) Files_Per_Sec, ROUND(((A.File_Size/1024)/60), 2) KB_Per_Sec, ROUND((A.Records_Written/60), 2) CDRS_Per_Sec     
            FROM
            (    
                SELECT /*+ PARALLEL(32)*/
                    ApplicationID, Dt, InputServer,
                    SUM(File_Cnt) File_Cnt,
                    SUM(File_Size) File_Size,
                    SUM(Records_Read) Records_Read,
                    SUM(Records_Written) Records_Written
                FROM
                (    
                    SELECT /*+ PARALLEL(32)*/ 
                        FT_FileReference FileRef, FT_ApplicationID ApplicationID, TRUNC(FT_TimeProcessed, 'MI') Dt, FT_InputServer InputServer,
                        1 File_Cnt, FT_FileSize File_Size, 0 Records_Read, 0 Records_Written
                    FROM TMP_TRACKED_FILES
                    WHERE FT_TimeProcessed IS NOT NULL AND FT_Status = 'COMPLETE'
                    UNION ALL
                    SELECT /*+ PARALLEL(32)*/ 
                        FT_FileReference FileRef, P_ApplicationID ApplicationID, TRUNC(P_TimeProcessed, 'MI') Dt, P_InputServer InputServer,
                        1 File_Cnt, P_FileSize File_Size, P_TotalRecords Records_Read, P_RecordsWritten Records_Written
                    FROM TMP_PARSED_FILES
                    WHERE P_TimeProcessed IS NOT NULL AND P_STATUS = 'COMPLETE'
                    UNION ALL
                    SELECT /*+ PARALLEL(32)*/ 
                        FT_FileReference FileRef, AZ_ApplicationID ApplicationID, TRUNC(AZ_TimeProcessed, 'MI') Dt, AZ_InputServer InputServer,
                        Analyser_Files File_Cnt, AZ_FileSize File_Size, AZ_TotalRecords Records_Read, AZ_RecordsWritten Records_Written
                    FROM TMP_ANALYZED_FILES
                    WHERE AZ_TimeProcessed IS NOT NULL AND AZ_ApplicationID IS NOT NULL
                    UNION ALL
                    SELECT /*+ PARALLEL(32)*/ 
                        FT_FileReference FileRef, L_ApplicationID ApplicationID, TRUNC(L_TimeProcessed, 'MI') Dt, L_InputServer InputServer,
                        Loader_Files File_Cnt, L_FileSize File_Size, 0 Records_Read, L_Calc_PerFileSuccess Records_Written
                    FROM TMP_LOADED_FILES
                    WHERE L_TimeProcessed IS NOT NULL AND L_ApplicationID IS NOT NULL                    
                )    
                GROUP BY ApplicationID, Dt, InputServer
            ) A
            LEFT JOIN FW_APPLICATIONS FW ON FW.ApplicationID = A.ApplicationID
            LEFT JOIN FW_APPTYPES T ON T.AppTypeID = FW.AppType
        ]';
        
        MAINTENANCE_PKG.LOGDEBUGMSG(DEBUG_PACKAGE, 'RECON_PERFORMANCE_DETAILS', RUNDATE, NULL, SQL_MAIN, NULL);
        EXECUTE IMMEDIATE (SQL_MAIN);
        IRECS := SQL%ROWCOUNT;
        COMMIT;
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1','INSERT COMPLETED:'||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'), RUNDATE);
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'VALUE1', IRECS, RUNDATE);

        STATUS := 'END';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
    EXCEPTION
        WHEN OTHERS THEN
            MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'ERROR', 'at ' || VDEBUG || SQLERRM ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
    END;
    PROCEDURE RUNSTEPS(RUNDATE IN DATE)
    AS
        THREAD_LIST THREAD_LIST_TYPE := THREAD_LIST_TYPE();
        STATUS      VARCHAR2(50);
        STEPNAME    VARCHAR2(50);
        VDEBUG      VARCHAR2(50);
        PROCNAME    VARCHAR2(30);
        JOBID       NUMBER;
        SQL_MAIN    VARCHAR2(32000);
        OLDESTDATE    DATE;
    BEGIN
        STATUS := 'START';
        STEPNAME := 'BUILD_FILERECON';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
        MAINTENANCE_PKG.CLEARDEBUGMSG(DEBUG_PACKAGE);
        VDEBUG := '0 ';
        --Initialize and reset values to NULL
        THREAD_LIST.EXTEND(4);
        VDEBUG := '1 ';        

        STATUS := 'Trace Table Extract Started ';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', STATUS ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
        FOR INDX IN 1 .. THREAD_LIST.COUNT
        LOOP
            THREAD_LIST(INDX) := NULL;
        END LOOP;
        --Loop through the Types and create a TMP table for each...
        FOR INDX IN 1..4
        LOOP
            VDEBUG := '2.' || INDX || ' ';
            IF INDX = 1 THEN
                PROCNAME := 'ExtractTrackedFiles';
            END IF;
            IF INDX = 2 THEN
                PROCNAME := 'ExtractParsedFiles';
            END IF;
            IF INDX = 3 THEN
                PROCNAME := 'ExtractAnalyzedFiles';
            END IF;
            IF INDX = 4 THEN
                PROCNAME := 'ExtractLoadedFiles';
            END IF;
            SQL_MAIN :=
            q'[
                BEGIN
                    FILERECON.{PROCNAME}(TO_DATE('{RUNDATE}','DD/MM/YYYY'));
                END;
            ]';
            SQL_MAIN := REPLACE(SQL_MAIN, '{RUNDATE}', TO_CHAR(RUNDATE,'DD/MM/YYYY'));
            SQL_MAIN := REPLACE(SQL_MAIN, '{PROCNAME}', PROCNAME);
            DBMS_JOB.SUBMIT(JOBID, SQL_MAIN);
            THREAD_LIST(INDX) := JOBID;
            COMMIT; --To submit the job
        END LOOP;
        DBMS_LOCK.SLEEP(5); --give jobs time start up
        MAINTENANCE_PKG.CHECK_THREAD_STATUS(THREAD_LIST); --Pause and wait for the jobs to complete
        STATUS := 'Trace Table Extract Completed ';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', STATUS ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));

        VDEBUG := '3 ';
        FILERECON.BUILDRECONDETAILS(RUNDATE);
        FILERECON.MISSINGFILES_MAIN (RUNDATE);
--        FILERECON.BUILDMISSINGFILESEQ(RUNDATE);
        FILERECON.BUILDRECONSUMMARY(RUNDATE);
        FILERECON.BUILDPERFORMANCEDETAILS(RUNDATE);

        VDEBUG := '4 ';
        FILERECON.SETOLDESTTRACKPARS; --set new oldest active partition
        OLDESTDATE := TRACEFILES.GETOLDESTTRACKPARS;
        STATUS := 'Oldest Track Parse Water-Level: ' || TO_CHAR(OLDESTDATE,'YYYYMMDDHH24');
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT4', STATUS || ':' ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));

        STATUS := 'END';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
    EXCEPTION
        WHEN OTHERS THEN
            MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'ERROR', 'at ' || VDEBUG || SQLERRM ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
    END;    
    PROCEDURE RUNPERFORMANCESTEPS(DAYSBACK IN NUMBER)
    AS
        THREAD_LIST THREAD_LIST_TYPE := THREAD_LIST_TYPE();
        STATUS      VARCHAR2(50);
        STEPNAME    VARCHAR2(50);
        VDEBUG      VARCHAR2(50);
        PROCNAME    VARCHAR2(30);
        JOBID       NUMBER;
        SQL_MAIN    VARCHAR2(32000);
        RUNDATE     DATE;
    BEGIN
        STATUS := 'START';
        STEPNAME := 'BUILD_PERFORMANCERECON';
        RUNDATE := TRUNC(SYSDATE);
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
        MAINTENANCE_PKG.CLEARDEBUGMSG(DEBUG_PACKAGE);
        VDEBUG := '0 ';
        --Initialize and reset values to NULL
        THREAD_LIST.EXTEND(4);
        VDEBUG := '1 ';

        STATUS := 'Trace Table Extract Started ';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', STATUS ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
        FOR INDX IN 1 .. THREAD_LIST.COUNT
        LOOP
            THREAD_LIST(INDX) := NULL;
        END LOOP;
        --Loop through the Types and create a TMP table for each...
        FOR INDX IN 1..4
        LOOP
            VDEBUG := '2.' || INDX || ' ';
            IF INDX = 1 THEN
                PROCNAME := 'ExtractTrackedFiles';
            END IF;
            IF INDX = 2 THEN
                PROCNAME := 'ExtractParsedFiles';
            END IF;
            IF INDX = 3 THEN
                PROCNAME := 'ExtractAnalyzedFiles';
            END IF;
            IF INDX = 4 THEN
                PROCNAME := 'ExtractLoadedFiles';
            END IF;
            SQL_MAIN :=
            q'[
                BEGIN
                    FILERECON.{PROCNAME}(TO_DATE('{RUNDATE}','DD/MM/YYYY'), {DAYS_BACK});
                END;
            ]';
            SQL_MAIN := REPLACE(SQL_MAIN, '{RUNDATE}', TO_CHAR(RUNDATE,'DD/MM/YYYY'));
            SQL_MAIN := REPLACE(SQL_MAIN, '{DAYS_BACK}', DAYSBACK);            
            SQL_MAIN := REPLACE(SQL_MAIN, '{PROCNAME}', PROCNAME);
            DBMS_JOB.SUBMIT(JOBID, SQL_MAIN);
            THREAD_LIST(INDX) := JOBID;
            COMMIT; --To submit the job
        END LOOP;
        DBMS_LOCK.SLEEP(5); --give jobs time start up
        MAINTENANCE_PKG.CHECK_THREAD_STATUS(THREAD_LIST); --Pause and wait for the jobs to complete
        STATUS := 'Trace Table Extract Completed ';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'COMMENT1', STATUS ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));

        VDEBUG := '3 ';
        FILERECON.BUILDPERFORMANCEDETAILS(RUNDATE, DAYSBACK);        

        STATUS := 'END';
        MAINTENANCE_PKG.LOGSTATUS(STEPNAME, STATUS, TO_CHAR(RUNDATE,'YYYY/MM/DD'));
    EXCEPTION
        WHEN OTHERS THEN
            MAINTENANCE_PKG.LOGSTATUS(STEPNAME, 'ERROR', 'at ' || VDEBUG || SQLERRM ||' at '|| TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'));
    END;
END;
/

