/*
$Rev: 7435 $ 
$Author: randall.stanley $ 
$Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_natl_test_scores_explore.sql $
$Id: etl_pm_natl_test_scores_explore.sql 7435 2009-07-24 13:58:49Z randall.stanley $ 
 */

####################################################################
# Insert Explorer data into pm_natl_test_scores.
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_pm_natl_test_scores_explore//

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_natl_test_scores_explore()
CONTAINS SQL
COMMENT '$Rev: 7435 $ $Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $'
SQL SECURITY INVOKER

PROC: BEGIN 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_ga_explorer';

    if @view_exists > 0 then

        ##############################################################
        # Insert Explore scores
        ##############################################################
        
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
                ,year(str_to_date(test_date, '%Y%m')) AS test_year
                ,month(str_to_date(test_date, '%Y%m')) AS test_month
                ,(CASE WHEN tsub.moniker like 'English'
                            THEN odsv.english_ss
                        WHEN tsub.moniker like 'Math'
                            THEN odsv.math_ss
                        WHEN tsub.moniker like 'Reading'
                            THEN odsv.reading_ss
                        WHEN tsub.moniker like 'Science'
                            THEN odsv.science_ss
                        WHEN tsub.moniker like 'Composite'
                            THEN odsv.composite
                        ELSE NULL
                        END) AS score
            FROM v_pmi_ods_ga_explorer AS odsv
            JOIN c_student as st
                ON  odsv.student_id = st.student_state_code
            JOIN pm_natl_test_type AS tty
                ON  tty.test_type_code = 'explore'
            JOIN pm_natl_test_subject AS tsub
                ON  tty.test_type_id = tsub.test_type_id
        ) AS dt
        WHERE dt.score IS NOT NULL
        AND dt.score not in ('--','**')
            ON DUPLICATE KEY UPDATE last_user_id = 1234, score = dt.score;

        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_ga_explorer', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;  

    end if;
    
END PROC;
//
