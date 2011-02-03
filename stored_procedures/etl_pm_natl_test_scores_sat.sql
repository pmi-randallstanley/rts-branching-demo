/*
$Rev: 7435 $ 
$Author: randall.stanley $ 
$Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_natl_test_scores_sat.sql $
$Id: etl_pm_natl_test_scores_sat.sql 7435 2009-07-24 13:58:49Z randall.stanley $ 
 */

####################################################################
# Insert SAT data into pm_natl_test_scores.
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_pm_natl_test_scores_sat//

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_natl_test_scores_sat()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7435 $Date: 2007-08-06 15:19:51 -0400 (Mon, 06 Aug 2007) $'

PROC: BEGIN 

    declare     v_date_format_mask varchar(15) default '%m%y';

    ##############################################################
    # Insert SAT scores
    ##############################################################
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_sat';

    if @view_exists > 0 then

        set @satTestDateFormatMask := pmi_f_get_etl_setting('satTestDateFormatMask');

        if @satTestDateFormatMask is not null then
            set v_date_format_mask = @satTestDateFormatMask;
        end if;

        INSERT INTO pm_natl_test_scores (
            test_type_id
            ,subject_id
            ,student_id
            ,test_year
            ,test_month
            ,score
            ,last_user_id
            ,create_timestamp
            )
            
            SELECT 
                dt.test_type_id
                ,dt.subject_id
                ,dt.student_id
                ,dt.test_year
                ,dt.test_month
                ,dt.score
                ,1234
                ,now()
            FROM (
                    SELECT 
                        tty.test_type_id
                        ,tsub.subject_id
                        ,st.student_id
                        ,year(str_to_date(odsv.testdate, v_date_format_mask)) as test_year
                        ,month(str_to_date(odsv.testdate, v_date_format_mask)) as test_month
                        ,(CASE WHEN tsub.subject_code like '%Math'
                                    THEN odsv.math_score
                                WHEN tsub.subject_code like '%Verbal'
                                    THEN odsv.verbal_score
                                WHEN tsub.subject_code like '%Writing'
                                    THEN odsv.writing_score
                                WHEN tsub.subject_code like '%satMvsum'
                                    THEN (odsv.math_score + odsv.verbal_score)
                                WHEN tsub.subject_code like '%satMvwsum'
                                    THEN (odsv.math_score + odsv.verbal_score + odsv.writing_score)
                                ELSE NULL
                                END) AS score
                    FROM v_pmi_ods_sat AS odsv
                    JOIN c_student st
                        ON st.student_code = odsv.studentid
                    JOIN pm_natl_test_type AS tty
                        ON  tty.test_type_code = 'sat'
                    JOIN pm_natl_test_subject AS tsub
                        ON  tsub.test_type_id = tty.test_type_id
                        AND tsub.subject_code NOT LIKE '%Max%'
            # Need to revisit the need for this join since the target 
            # table does not even contain school year
            # may make more sense to restrict on testdate.
            #                     JOIN c_school_year sy 
            #                        ON odsv.school_year = sy.school_year_id
                ) AS dt
            WHERE dt.score > 100
    
        ON DUPLICATE key UPDATE last_user_id = 1234
            ,score = values(score)
        ;

        ####################################################################
        # Insert MV/MVW Max Sums:
        ####################################################################
    
        INSERT INTO pm_natl_test_scores (
            test_type_id
            ,subject_id
            ,student_id
            ,test_year
            ,test_month
            ,score
            ,last_user_id
            ,create_timestamp
            )
    
            SELECT 
                dt.test_type_id
                ,dt.subject_id
                ,dt.student_id
                ,dt.test_year
                ,dt.test_month
                ,dt.score
                ,1234
                ,now()
            FROM (
                    SELECT 
                        maxer.test_type_id
                        ,tsub2.subject_id
                        ,maxer.student_id
                        ,MAX(maxer.test_year) AS test_year
                        ,MAX(ts2.test_month) AS test_month
                        ,CASE WHEN tsub2.subject_code LIKE '%MvSum' THEN max(maxer.max_math) + MAX(maxer.max_verbal)
                                WHEN tsub2.subject_code LIKE '%MvwSum' THEN max(maxer.max_math) + MAX(maxer.max_verbal) + MAX(maxer.max_writing)
                                ELSE NULL END AS score
                    FROM 
                        (SELECT 
                            ts.test_type_id
                            ,ts.student_id
                            ,CASE WHEN tsub.subject_code LIKE '%Math' THEN MAX(ts.score) ELSE NULL END AS max_math
                            ,CASE WHEN tsub.subject_code LIKE '%Verbal' THEN MAX(ts.score) ELSE NULL END AS max_verbal
                            ,CASE WHEN tsub.subject_code LIKE '%Writing' THEN MAX(ts.score) ELSE NULL END AS max_writing
                            ,MAX(ts.test_year) AS test_year
                            
                        FROM pm_natl_test_scores AS ts
                        JOIN pm_natl_test_subject AS tsub
                            ON   tsub.subject_id = ts.subject_id
                            AND  tsub.test_type_id = ts.test_type_id
                        JOIN pm_natl_test_type AS tty
                            ON   tty.test_type_id = ts.test_type_id
                            AND  tty.test_type_code IN ('sat')
                        GROUP BY ts.student_id
                                ,ts.test_type_id
                                ,ts.subject_id  ) AS maxer
                    JOIN pm_natl_test_subject AS tsub2
                        ON   tsub2.test_type_id = maxer.test_type_id
                        AND  tsub2.subject_code LIKE '%Max%'
                    JOIN pm_natl_test_scores AS ts2
                        ON   ts2.test_type_id = maxer.test_type_id
                        AND  ts2.student_id = maxer.student_id
                        AND  ts2.test_year = maxer.test_year
                    GROUP BY maxer.student_id
                            ,maxer.test_type_id
                            ,tsub2.subject_code
                ) AS dt
            WHERE dt.score > 100
    
        ON DUPLICATE key UPDATE last_user_id = 1234
            ,score = values(score)
        ;

        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_sat', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;  

    end if;
    
END PROC;
//
