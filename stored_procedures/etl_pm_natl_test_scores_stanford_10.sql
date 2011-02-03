DROP PROCEDURE IF EXISTS etl_pm_natl_test_scores_stanford_10//

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_natl_test_scores_stanford_10()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7435 $ $Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $'

/*
$Rev: 7435 $ 
$Author: randall.stanley $ 
$Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_natl_test_scores_stanford_10.sql $
$Id: etl_pm_natl_test_scores_stanford_10.sql 7435 2009-07-24 13:58:49Z randall.stanley $ 
 */

PROC: BEGIN 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    ##################
    # Stanford 10    #
    ##################

    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_stanford_10';

    if @view_exists > 0 then

        SELECT count(*) INTO @stan10_count FROM v_pmi_ods_stanford_10 AS m;
    
        IF (@stan10_count > 1000) THEN
            
            select  test_type_id
            into    @test_type_id
            from    pm_natl_test_type
            where   test_type_code = 'stan10'
            ;

            delete from pm_natl_test_scores
            where   test_type_id = @test_type_id
            ;
            
        END IF;
    
        INSERT pm_natl_test_scores (
                student_id
                ,test_type_id
                ,subject_id
                ,test_year
                ,test_month
                ,stan_scale_score
                ,stan_perf_index
                ,stan_raw_score
                ,stan_natl_prct
                ,stan_nce
                ,stan_ge
                ,stan_natl_stanine
                ,stan_aac
                ,last_user_id
                ,create_timestamp)
        
            SELECT  dt.student_id
                   ,dt.test_type_id
                   ,dt.subject_id
                   ,dt.yyyy
                   ,dt.mm
                   ,dt.ss_score
                   ,dt.pl_score
                   ,dt.rs_score
                   ,dt.np_score
                   ,ROUND(dt.nce_score / 10, 1) -- NOTE: Division is from 'Stanford 10 Student Data Description' Documentation
                   ,ROUND( CASE dt.ge_score WHEN 222 THEN -10
                                            WHEN 333 THEN   0
                                            ELSE dt.ge_score END / 10, 1) -- NOTE: Case and division is from 'Stanford 10 Student Data Description' Documentation
                   ,dt.ns_score
                   ,dt.aac_score
                   ,1234
                   ,now()
            FROM ( SELECT  s.student_id
                       ,ts.test_type_id
                       ,ts.subject_id
                       ,m.yyyy
                       ,m.mm
                       ,(CASE ts.subject_code  -- NOTE: Case order is from 'Stanford 10 Student Data Description' Documentation
                            WHEN 'stan10ReadTot'    THEN m.ss_1
                            WHEN 'stan10WrdStd'     THEN m.ss_2
                            WHEN 'stan10WrdRead'    THEN m.ss_3
                            WHEN 'stan10ReadComp'   THEN m.ss_4
                            WHEN 'stan10MathTot'    THEN m.ss_5
                            WHEN 'stan10MathProb'   THEN m.ss_6
                            WHEN 'stan10MathProc'   THEN m.ss_7
                            WHEN 'stan10Lang'       THEN m.ss_9
                            WHEN 'stan10Spell'      THEN m.ss_12
                            WHEN 'stan10Read1st'    THEN m.ss_13
                            WHEN 'stan10Env'        THEN m.ss_14
                            WHEN 'stan10List'       THEN m.ss_17
                            WHEN 'stan10BasBat'     THEN m.ss_20
                            WHEN 'stan10TotBat'     THEN m.ss_21
                            WHEN 'stan10SenRead'    THEN m.ss_22
                            WHEN 'stan10ReadTot1'   THEN m.ss_25
                            WHEN 'stan10ReadTot2'   THEN m.ss_26
                            WHEN 'stan10ReadTot3'   THEN m.ss_27
                            ELSE NULL
                            END) AS ss_score
                       ,(CASE ts.subject_code
                            WHEN 'stan10ReadTot'    THEN m.pl_1
                            WHEN 'stan10WrdStd'     THEN m.pl_2
                            WHEN 'stan10WrdRead'    THEN m.pl_3
                            WHEN 'stan10ReadComp'   THEN m.pl_4
                            WHEN 'stan10MathTot'    THEN m.pl_5
                            WHEN 'stan10MathProb'   THEN m.pl_6
                            WHEN 'stan10MathProc'   THEN m.pl_7
                            WHEN 'stan10Lang'       THEN m.pl_9
                            WHEN 'stan10Spell'      THEN m.pl_12
                            WHEN 'stan10Read1st'    THEN m.pl_13
                            WHEN 'stan10Env'        THEN m.pl_14
                            WHEN 'stan10List'       THEN m.pl_17
                            WHEN 'stan10BasBat'     THEN m.pl_20
                            WHEN 'stan10TotBat'     THEN m.pl_21
                            WHEN 'stan10SenRead'    THEN m.pl_22
                            WHEN 'stan10ReadTot1'   THEN m.pl_25
                            WHEN 'stan10ReadTot2'   THEN m.pl_26
                            WHEN 'stan10ReadTot3'   THEN m.pl_27
                            ELSE NULL
                            END) AS pl_score
                       ,(CASE ts.subject_code
                            WHEN 'stan10ReadTot'    THEN m.rs_1
                            WHEN 'stan10WrdStd'     THEN m.rs_2
                            WHEN 'stan10WrdRead'    THEN m.rs_3
                            WHEN 'stan10ReadComp'   THEN m.rs_4
                            WHEN 'stan10MathTot'    THEN m.rs_5
                            WHEN 'stan10MathProb'   THEN m.rs_6
                            WHEN 'stan10MathProc'   THEN m.rs_7
                            WHEN 'stan10Lang'       THEN m.rs_9
                            WHEN 'stan10Spell'      THEN m.rs_12
                            WHEN 'stan10Read1st'    THEN m.rs_13
                            WHEN 'stan10Env'        THEN m.rs_14
                            WHEN 'stan10List'       THEN m.rs_17
                            WHEN 'stan10BasBat'     THEN m.rs_20
                            WHEN 'stan10TotBat'     THEN m.rs_21
                            WHEN 'stan10SenRead'    THEN m.rs_22
                            WHEN 'stan10ReadTot1'   THEN m.rs_25
                            WHEN 'stan10ReadTot2'   THEN m.rs_26
                            WHEN 'stan10ReadTot3'   THEN m.rs_27
                            ELSE NULL
                            END) AS rs_score
    
                       ,(CASE ts.subject_code
                            WHEN 'stan10ReadTot'    THEN m.np_1
                            WHEN 'stan10WrdStd'     THEN m.np_2
                            WHEN 'stan10WrdRead'    THEN m.np_3
                            WHEN 'stan10ReadComp'   THEN m.np_4
                            WHEN 'stan10MathTot'    THEN m.np_5
                            WHEN 'stan10MathProb'   THEN m.np_6
                            WHEN 'stan10MathProc'   THEN m.np_7
                            WHEN 'stan10Lang'       THEN m.np_9
                            WHEN 'stan10Spell'      THEN m.np_12
                            WHEN 'stan10Read1st'    THEN m.np_13
                            WHEN 'stan10Env'        THEN m.np_14
                            WHEN 'stan10List'       THEN m.np_17
                            WHEN 'stan10BasBat'     THEN m.np_20
                            WHEN 'stan10TotBat'     THEN m.np_21
                            WHEN 'stan10SenRead'    THEN m.np_22
                            WHEN 'stan10ReadTot1'   THEN m.np_25
                            WHEN 'stan10ReadTot2'   THEN m.np_26
                            WHEN 'stan10ReadTot3'   THEN m.np_27
                            ELSE NULL
                            END) AS np_score
    
                       ,(CASE ts.subject_code
                            WHEN 'stan10ReadTot'    THEN m.nce_1
                            WHEN 'stan10WrdStd'     THEN m.nce_2
                            WHEN 'stan10WrdRead'    THEN m.nce_3
                            WHEN 'stan10ReadComp'   THEN m.nce_4
                            WHEN 'stan10MathTot'    THEN m.nce_5
                            WHEN 'stan10MathProb'   THEN m.nce_6
                            WHEN 'stan10MathProc'   THEN m.nce_7
                            WHEN 'stan10Lang'       THEN m.nce_9
                            WHEN 'stan10Spell'      THEN m.nce_12
                            WHEN 'stan10Read1st'    THEN m.nce_13
                            WHEN 'stan10Env'        THEN m.nce_14
                            WHEN 'stan10List'       THEN m.nce_17
                            WHEN 'stan10BasBat'     THEN m.nce_20
                            WHEN 'stan10TotBat'     THEN m.nce_21
                            WHEN 'stan10SenRead'    THEN m.nce_22
                            WHEN 'stan10ReadTot1'   THEN m.nce_25
                            WHEN 'stan10ReadTot2'   THEN m.nce_26
                            WHEN 'stan10ReadTot3'   THEN m.nce_27
                            ELSE NULL
                            END) AS nce_score
    
                       ,(CASE ts.subject_code
                            WHEN 'stan10ReadTot'    THEN m.ge_1
                            WHEN 'stan10WrdStd'     THEN m.ge_2
                            WHEN 'stan10WrdRead'    THEN m.ge_3
                            WHEN 'stan10ReadComp'   THEN m.ge_4
                            WHEN 'stan10MathTot'    THEN m.ge_5
                            WHEN 'stan10MathProb'   THEN m.ge_6
                            WHEN 'stan10MathProc'   THEN m.ge_7
                            WHEN 'stan10Lang'       THEN m.ge_9
                            WHEN 'stan10Spell'      THEN m.ge_12
                            WHEN 'stan10Read1st'    THEN m.ge_13
                            WHEN 'stan10Env'        THEN m.ge_14
                            WHEN 'stan10List'       THEN m.ge_17
                            WHEN 'stan10BasBat'     THEN m.ge_20
                            WHEN 'stan10TotBat'     THEN m.ge_21
                            WHEN 'stan10SenRead'    THEN m.ge_22
                            WHEN 'stan10ReadTot1'   THEN m.ge_25
                            WHEN 'stan10ReadTot2'   THEN m.ge_26
                            WHEN 'stan10ReadTot3'   THEN m.ge_27
                            ELSE NULL
                            END) AS ge_score
    
                       ,(CASE ts.subject_code
                            WHEN 'stan10ReadTot'    THEN m.ns_1
                            WHEN 'stan10WrdStd'     THEN m.ns_2
                            WHEN 'stan10WrdRead'    THEN m.ns_3
                            WHEN 'stan10ReadComp'   THEN m.ns_4
                            WHEN 'stan10MathTot'    THEN m.ns_5
                            WHEN 'stan10MathProb'   THEN m.ns_6
                            WHEN 'stan10MathProc'   THEN m.ns_7
                            WHEN 'stan10Lang'       THEN m.ns_9
                            WHEN 'stan10Spell'      THEN m.ns_12
                            WHEN 'stan10Read1st'    THEN m.ns_13
                            WHEN 'stan10Env'        THEN m.ns_14
                            WHEN 'stan10List'       THEN m.ns_17
                            WHEN 'stan10BasBat'     THEN m.ns_20
                            WHEN 'stan10TotBat'     THEN m.ns_21
                            WHEN 'stan10SenRead'    THEN m.ns_22
                            WHEN 'stan10ReadTot1'   THEN m.ns_25
                            WHEN 'stan10ReadTot2'   THEN m.ns_26
                            WHEN 'stan10ReadTot3'   THEN m.ns_27
                            ELSE NULL
                            END) AS ns_score
    
                       ,(CASE ts.subject_code
                            WHEN 'stan10ReadTot'    THEN m.aac_1
                            WHEN 'stan10WrdStd'     THEN m.aac_2
                            WHEN 'stan10WrdRead'    THEN m.aac_3
                            WHEN 'stan10ReadComp'   THEN m.aac_4
                            WHEN 'stan10MathTot'    THEN m.aac_5
                            WHEN 'stan10MathProb'   THEN m.aac_6
                            WHEN 'stan10MathProc'   THEN m.aac_7
                            WHEN 'stan10Lang'       THEN m.aac_9
                            WHEN 'stan10Spell'      THEN m.aac_12
                            WHEN 'stan10Read1st'    THEN m.aac_13
                            WHEN 'stan10Env'        THEN m.aac_14
                            WHEN 'stan10List'       THEN m.aac_17
                            WHEN 'stan10BasBat'     THEN m.aac_20
                            WHEN 'stan10TotBat'     THEN m.aac_21
                            WHEN 'stan10SenRead'    THEN m.aac_22
                            WHEN 'stan10ReadTot1'   THEN m.aac_25
                            WHEN 'stan10ReadTot2'   THEN m.aac_26
                            WHEN 'stan10ReadTot3'   THEN m.aac_27
                            ELSE NULL
                            END) AS aac_score
    
                FROM   v_pmi_ods_stanford_10 AS m
                JOIN   pm_natl_test_type AS t
                    ON   t.test_type_code = 'stan10'
                JOIN   pm_natl_test_subject AS ts
                    ON   ts.test_type_id = t.test_type_id
                JOIN   c_student AS s
                    ON   s.student_code = m.student_id
                JOIN   c_school_year sy
                    ON   CAST(CONCAT(yyyy,mm,dd) AS date) BETWEEN sy.begin_date AND sy.end_date
                JOIN   c_student_year AS sty
                    ON   sty.student_id = s.student_id
                    AND  sty.school_year_id = sy.school_year_id ) AS dt
            WHERE   (  dt.ss_score > 0
                    OR dt.pl_score > 0
                    OR dt.rs_score > 0
                    OR dt.np_score > 0
                    OR dt.nce_score > 0
                    OR dt.ge_score > 0
                    OR dt.ns_score > 0
                    OR dt.aac_score > 0 )
                AND dt.rs_score NOT IN ( 996, 997, 998, 999 ) -- NOTE: Not vaild tests from 'Stanford 10 Student Data Description' Documentation
                AND dt.nce_score <> '.'
        ON DUPLICATE KEY UPDATE 
            last_user_id = values(last_user_id)
            ,stan_scale_score  = values(stan_scale_score)
            ,stan_perf_index   = values(stan_perf_index)
            ,stan_raw_score    = values(stan_raw_score)
            ,stan_natl_prct    = values(stan_natl_prct)
            ,stan_nce          = values(stan_nce)
            ,stan_ge           = values(stan_ge)
            ,stan_natl_stanine = values(stan_natl_stanine)
            ,stan_aac          = values(stan_aac)
        ;

        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_stanford_10', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;  

    end if;

END PROC;
//
