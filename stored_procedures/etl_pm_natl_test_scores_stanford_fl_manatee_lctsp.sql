DROP PROCEDURE IF EXISTS etl_pm_natl_test_scores_stanford_fl_manatee_lctsp//

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_natl_test_scores_stanford_fl_manatee_lctsp()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7435 $ $Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $'

/*
$Rev: 7435 $ 
$Author: randall.stanley $ 
$Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_natl_test_scores_stanford_fl_manatee_lctsp.sql $
$Id: etl_pm_natl_test_scores_stanford_fl_manatee_lctsp.sql 7435 2009-07-24 13:58:49Z randall.stanley $ 
 */

PROC: BEGIN 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    #################
    # SAT10 Subject #
    #################

    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_fl_manatee_lctsp';

    if @view_exists > 0 then

        INSERT pm_natl_test_scores (
            student_id
            ,test_type_id
            ,subject_id
            ,test_year
            ,test_month
            ,stan_raw_score
            ,stan_scale_score
            ,stan_natl_prct
            ,stan_nce
            ,stan_ge
            ,stan_natl_stanine
            ,stan_perf_index
            ,stan_aac
            ,last_user_id
            ,create_timestamp)
    
        SELECT  s.student_id
               ,ts.test_type_id
               ,ts.subject_id
               ,year(m.test_date)
               ,month(m.test_date)
               ,m.score_raw
               ,m.score_scale
               ,m.score_ntl_percentile
               ,m.score_nrml_curve_eq
               ,m.score_grd_eqv
               ,m.score_natl_stanin
               ,m.score_perfrm_index
               ,m.score_achievement_level
               ,1234
               ,now()
        FROM    v_pmi_ods_fl_manatee_lctsp AS m
        JOIN    pm_natl_test_subject AS ts
             ON  ts.subject_code = CAST(CASE m.subtest_id
                                        WHEN 01 THEN 'stan10TotBat'
                                        WHEN 02 THEN 'stan10BasBat'
                                        WHEN 10 THEN 'stan10ReadTot'
                                        WHEN 11 THEN 'stan10ReadComp'
                                        WHEN 12 THEN 'stan10ReadVocab'
                                        WHEN 20 THEN 'stan10MathTot'
                                        WHEN 30 THEN 'stan10Lang'
                                        WHEN 31 THEN 'stan10Spell'
                                        WHEN 45 THEN 'stan10SenRead'
                                        WHEN 47 THEN 'stan10WrdRead'
                                        WHEN 48 THEN 'stan10WrdStd'
                                        WHEN 63 THEN 'stan10MathProb'
                                        WHEN 64 THEN 'stan10MathProc'
                                        WHEN 65 THEN 'stan10Env'
                                        END AS char)
        JOIN    c_student AS s
             ON     s.student_code = m.student_number
        JOIN    c_school_year sy
             ON     CAST(m.test_date AS date) BETWEEN sy.begin_date AND sy.end_date       
        JOIN    c_student_year AS sty
             ON    sty.student_id = s.student_id
             AND   sty.school_year_id = sy.school_year_id
        WHERE      m.score_scale is not NULL
             AND   m.test_id = 'SAT10'           
        ON DUPLICATE KEY UPDATE last_user_id = values(last_user_id)
                        ,stan_raw_score =    values(stan_raw_score)
                        ,stan_scale_score =  values(stan_scale_score)
                        ,stan_natl_prct =    values(stan_natl_prct)
                        ,stan_nce =          values(stan_nce)
                        ,stan_ge =           values(stan_ge)
                        ,stan_natl_stanine = values(stan_natl_stanine)
                        ,stan_perf_index =   values(stan_perf_index)
                        ,stan_aac =          values(stan_aac)
        ;
    
    
    
        #################
        # SDMT Subject  #
        #################
    
        INSERT pm_natl_test_scores (
            student_id
            ,test_type_id
            ,subject_id
            ,test_year
            ,test_month
            ,stan_raw_score
            ,stan_scale_score
            ,stan_natl_prct
            ,stan_nce
            ,stan_ge
            ,stan_natl_stanine
            ,stan_perf_index
            ,stan_aac
            ,last_user_id
            ,create_timestamp)
    
        SELECT  s.student_id
               ,ts.test_type_id
               ,ts.subject_id
               ,year(m.test_date)
               ,month(m.test_date)
               ,m.score_raw
               ,m.score_scale
               ,m.score_ntl_percentile
               ,m.score_nrml_curve_eq
               ,m.score_grd_eqv
               ,m.score_natl_stanin
               ,m.score_perfrm_index
               ,m.score_achievement_level
               ,1234
               ,now()    
        FROM    v_pmi_ods_fl_manatee_lctsp AS m
        JOIN    pm_natl_test_subject AS ts
             ON  ts.subject_code = CAST(CASE m.subtest_id
                                        WHEN 20 THEN  'sdmtMath'    
                                        WHEN 21 THEN  'sdmtComp'    
                                        WHEN 22 THEN  'sdmtConc'    
                                        WHEN 51 THEN  'sdmtNbrSys'  
                                        WHEN 52 THEN  'sdmtProbSlv' 
                                        WHEN 53 THEN  'sdmtGraphs'  
                                        WHEN 54 THEN  'sdmtStatPrb' 
                                        WHEN 55 THEN  'sdmtGeomMea' 
                                        WHEN 56 THEN  'sdmtDecimals'
                                        WHEN 57 THEN  'sdmtEquation'
                                        WHEN 58 THEN  'sdmtP_MWhlNum' 
                                        WHEN 59 THEN  'sdmtMltWhlNum' 
                                        WHEN 60 THEN  'sdmtDivWhlNum' 
                                        WHEN 61 THEN  'sdmtFrc_MxdNum'
                                        WHEN 62 THEN  'sdmtMlt_DivNum'
                                        WHEN 63 THEN  'sdmtAddWhlNum' 
                                        WHEN 64 THEN  'sdmtSubWhlNum' 
                                        WHEN 65 THEN  'sdmtMltFcts' 
                                        WHEN 66 THEN  'sdmtMltOper' 
                                        WHEN 67 THEN  'sdmtDivFcts' 
                                        WHEN 68 THEN  'sdmtPatterns'
                                        WHEN 69 THEN  'sdmtMeasure' 
                                        WHEN 70 THEN  'sdmtGeometry'
                                        WHEN 71 THEN  'sdmtAddFcts' 
                                        WHEN 72 THEN  'sdmtAddOper' 
                                        WHEN 73 THEN  'sdmtSubFcts' 
                                        WHEN 74 THEN  'sdmtSubOper' 
                                        END AS char)
        JOIN    c_student AS s
             ON     s.student_code = m.student_number
        JOIN    c_school_year sy
             ON     CAST(m.test_date AS date) BETWEEN sy.begin_date AND sy.end_date
        JOIN    c_student_year AS sty
             ON    sty.student_id = s.student_id
             AND   sty.school_year_id = sy.school_year_id
        WHERE      m.score_scale is not NULL
             AND   m.test_id = 'sdmt'
        ON DUPLICATE KEY UPDATE last_user_id = values(last_user_id)
                        ,stan_raw_score =    values(stan_raw_score)
                        ,stan_scale_score =  values(stan_scale_score)
                        ,stan_natl_prct =    values(stan_natl_prct)
                        ,stan_nce =          values(stan_nce)
                        ,stan_ge =           values(stan_ge)
                        ,stan_natl_stanine = values(stan_natl_stanine)
                        ,stan_perf_index =   values(stan_perf_index)
                        ,stan_aac =          values(stan_aac)
        ;
    
    
        #################
        # SDRT Subject  #
        #################
    
        INSERT pm_natl_test_scores (
            student_id
            ,test_type_id
            ,subject_id
            ,test_year
            ,test_month
            ,stan_raw_score
            ,stan_scale_score
            ,stan_natl_prct
            ,stan_nce
            ,stan_ge
            ,stan_natl_stanine
            ,stan_perf_index
            ,stan_aac
            ,last_user_id
            ,create_timestamp)
    
        SELECT  s.student_id
               ,ts.test_type_id
               ,ts.subject_id
               ,year(m.test_date)
               ,month(m.test_date)
               ,m.score_raw
               ,m.score_scale
               ,m.score_ntl_percentile
               ,m.score_nrml_curve_eq
               ,m.score_grd_eqv
               ,m.score_natl_stanin
               ,m.score_perfrm_index
               ,m.score_achievement_level
               ,1234
               ,now()    
        FROM    v_pmi_ods_fl_manatee_lctsp AS m
        JOIN    pm_natl_test_subject AS ts
             ON  ts.subject_code = CAST(CASE m.subtest_id
                                        WHEN 10  THEN 'sdrtReadTot'
                                        WHEN 11  THEN 'sdrtComprehe'
                                        WHEN 12  THEN 'sdrtVocab'
                                        WHEN 51  THEN 'sdrtSynonyms'
                                        WHEN 52  THEN 'sdrtClassify'
                                        WHEN 53  THEN 'sdrtWrdPrts'
                                        WHEN 54  THEN 'sdrtCntArea'
                                        WHEN 55  THEN 'sdrtRecRead'
                                        WHEN 56  THEN 'sdrtTxtRead'
                                        WHEN 57  THEN 'sdrtFncRead'
                                        WHEN 58  THEN 'sdrtIntUndr'
                                        WHEN 59  THEN 'sdrtIntrprt'
                                        WHEN 60  THEN 'sdrtCrtAnly'
                                        WHEN 61  THEN 'sdrtPrcStra'
                                        WHEN 62  THEN 'sdrtScan'
                                        END AS char)
        JOIN    c_student AS s
             ON     s.student_code = m.student_number
        JOIN    c_school_year sy
             ON     CAST(m.test_date AS date) BETWEEN sy.begin_date AND sy.end_date
        JOIN    c_student_year AS sty
             ON    sty.student_id = s.student_id
             AND   sty.school_year_id = sy.school_year_id
        WHERE      m.score_scale is not NULL
             AND   m.test_id = 'sdrt'
        ON DUPLICATE KEY UPDATE last_user_id = values(last_user_id)
                        ,stan_raw_score =    values(stan_raw_score)
                        ,stan_scale_score =  values(stan_scale_score)
                        ,stan_natl_prct =    values(stan_natl_prct)
                        ,stan_nce =          values(stan_nce)
                        ,stan_ge =           values(stan_ge)
                        ,stan_natl_stanine = values(stan_natl_stanine)
                        ,stan_perf_index =   values(stan_perf_index)
                        ,stan_aac =          values(stan_aac)
        ;
    

        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_fl_manatee_lctsp', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;  

    end if;
        
END PROC;
//
