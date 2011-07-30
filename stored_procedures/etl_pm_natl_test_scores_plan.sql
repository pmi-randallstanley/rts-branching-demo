/*
$Rev: 7435 $ 
$Author: randall.stanley $ 
$Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_natl_test_scores_plan.sql $
$Id: etl_pm_natl_test_scores_plan.sql 7435 2009-07-24 13:58:49Z randall.stanley $ 
 */

####################################################################
# Insert ACT data into pm_natl_test_scores.
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_pm_natl_test_scores_plan //

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_natl_test_scores_plan()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7435 $ $Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $'

PROC: BEGIN 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_plan';

    if @view_exists > 0 then

        ##############################################################
        # Insert Plan scores
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
        SELECT  dt.test_type_id
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
                  ,year(test_date) AS test_year
                  ,month(test_date) AS test_month
                  ,(CASE WHEN tsub.moniker like 'English'
                              THEN odsv.english
                          WHEN tsub.moniker like 'Math'
                              THEN odsv.math
                          WHEN tsub.moniker like 'Reading'
                              THEN odsv.reading
                          WHEN tsub.moniker like 'Science'
                              THEN odsv.science
                          WHEN tsub.moniker like 'Composite'
                              THEN odsv.composite
                          ELSE NULL
                          END) AS score         
              FROM v_pmi_ods_plan AS odsv
              JOIN c_student as st
                  ON  odsv.student_id = st.student_state_code
              JOIN pm_natl_test_type AS tty
                  ON  tty.test_type_code = 'plan'
              JOIN pm_natl_test_subject AS tsub
                  ON  tty.test_type_id = tsub.test_type_id                                        
          ) AS dt     
        WHERE dt.score IS NOT NULL
        ON DUPLICATE KEY UPDATE score = values(score)
            ,last_user_id = 1234
        ;

        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_plan', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;  

    end if;
    
END PROC;
//
