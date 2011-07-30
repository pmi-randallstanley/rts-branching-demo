DROP PROCEDURE IF EXISTS etl_pm_natl_test_scores_stanford_olsat//

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_natl_test_scores_stanford_olsat()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7435 $ $Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $'

/*
$Rev: 7435 $ 
$Author: randall.stanley $ 
$Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_natl_test_scores_stanford_olsat.sql $
$Id: etl_pm_natl_test_scores_stanford_olsat.sql 7435 2009-07-24 13:58:49Z randall.stanley $ 
 */

PROC: BEGIN 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    ##################
    # OLSAT          #
    ##################
    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_olsat';

    if @view_exists > 0 then

        SELECT count(*) INTO @olsat_count FROM v_pmi_ods_olsat AS m;
    
        IF (@olsat_count > 1000) THEN
    
                select  test_type_id
                into    @test_type_id
                from    pm_natl_test_type
                where   test_type_code = 'olsat'
                ;
    
                delete from pm_natl_test_scores
                where   test_type_id = @test_type_id
                ;
        END IF;
        
        IF (@olsat_count > 0) THEN
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
                    ,stan_natl_stanine
                    ,stan_nce
                    ,last_user_id
                    ,create_timestamp)
            
                SELECT  dt.student_id
                    ,dt.test_type_id
                    ,dt.subject_id
                    ,dt.test_year
                    ,dt.test_month
                    ,dt.ss_score
                    ,dt.pl_score
                    ,dt.rs_score
                    ,dt.np_score
                    ,dt.ns_score
                    ,dt.nce_score
                    ,1234
                    ,now()
                FROM ( SELECT  s.student_id
                        ,ts.test_type_id
                        ,ts.subject_id
                        ,COALESCE(m.yyyy, sy.school_year_id) AS test_year
                        ,COALESCE(m.mm, MONTH(now())) AS test_month
                        ,CASE ts.subject_code
                                WHEN 'olsatTot'        THEN strand_ss_01
                                WHEN 'olsatVerb'       THEN strand_ss_02
                                WHEN 'olsatNonverb'    THEN strand_ss_03
                            END AS ss_score
                        ,CASE ts.subject_code
                                WHEN 'olsatTot'        THEN strand_pl_01
                                WHEN 'olsatVerb'       THEN strand_pl_02
                                WHEN 'olsatNonverb'    THEN strand_pl_03
                            END AS pl_score
                        ,CASE ts.subject_code
                                WHEN 'olsatTot'        THEN strand_rs_1
                                WHEN 'olsatVerb'       THEN strand_rs_2
                                WHEN 'olsatNonverb'    THEN strand_rs_3
                            END AS rs_score
                        ,CASE ts.subject_code
                                WHEN 'olsatTot'        THEN strand_np_1
                                WHEN 'olsatVerb'       THEN strand_np_2
                                WHEN 'olsatNonverb'    THEN strand_np_3
                            END AS np_score
                        ,CASE ts.subject_code
                                WHEN 'olsatTot'        THEN strand_ns_1
                                WHEN 'olsatVerb'       THEN strand_ns_2
                                WHEN 'olsatNonverb'    THEN strand_ns_3
                            END AS ns_score
                        ,CASE ts.subject_code
                                WHEN 'olsatTot'        THEN strand_nce_1
                                WHEN 'olsatVerb'       THEN strand_nce_2
                                WHEN 'olsatNonverb'    THEN strand_nce_3
                            END AS nce_score
                #SELECT COUNT(*)
                    FROM   v_pmi_ods_olsat AS m
                    JOIN   pm_natl_test_type AS t
                        ON   t.test_type_code = 'olsat'
                    JOIN   pm_natl_test_subject AS ts
                        ON   ts.test_type_id = t.test_type_id
                    JOIN   c_student AS s
                        ON   s.student_code = m.student_id
                    JOIN   c_school_year sy
                        ON   sy.active_flag = 1     
                    JOIN   c_student_year AS sty
                        ON   sty.student_id = s.student_id
                        AND  sty.school_year_id = sy.school_year_id ) AS dt
                WHERE   (  dt.ss_score > 0
                        OR dt.pl_score > 0 )
            ON DUPLICATE KEY UPDATE 
                last_user_id = values(last_user_id)
                ,stan_scale_score = values(stan_scale_score)
                ,stan_perf_index  = values(stan_perf_index)
            ;
            
        END IF;

        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_olsat', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;  

    end if;
    
END PROC;
//
