DROP PROCEDURE IF EXISTS etl_pm_natl_test_scores_stanford_ltdb_crosspoint//

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_natl_test_scores_stanford_ltdb_crosspoint()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'

/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_natl_test_scores_stanford_ltdb_crosspoint.sql $
$Id: etl_pm_natl_test_scores_stanford_ltdb_crosspoint.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
 */

PROC: BEGIN 


    #################
    # SAT10 Subject #
    #################
   INSERT pm_natl_test_scores (
        student_id
        ,test_type_id
        ,subject_id
        ,test_year
        ,test_month
        ,stan_raw_score
        ,stan_scale_score
        ,stan_ge
        ,stan_nce
        ,stan_natl_prct
        ,stan_natl_stanine
        ,last_user_id
        ,create_timestamp)

    SELECT  s.student_id
           ,ts.test_type_id
           ,ts.subject_id
           ,m.yyyy
           ,m.mm
           ,m.score_01 * 1
           ,m.score_02 * 1
           ,m.score_03 * 1
           -- ,m.score_04 * 1
           ,m.score_05 * 1
           ,m.score_06 * 1
           ,m.score_07 * 1
           ,1234
           ,now()
    FROM    v_pmi_ods_ltdb_crosspoint AS m
    JOIN    pm_natl_test_subject AS ts
         ON  ts.subject_code = CAST(CASE m.subtest_id
                                    WHEN 04 THEN 'stan10ReadTot'
                                    WHEN 06 THEN 'stan10MathTot'
                                    END AS char)
    JOIN    c_student AS s
         ON    s.student_code = m.student_id
    JOIN    c_school_year sy
         ON    CAST(Concat(m.yyyy, '-', m.mm, '-', m.dd) AS date) BETWEEN sy.begin_date AND sy.end_date
    JOIN    c_student_year AS sty
         ON    sty.student_id = s.student_id
         AND   sty.school_year_id = sy.school_year_id
    WHERE m.test_id = 'ST5'
    ON DUPLICATE KEY UPDATE last_user_id = 1234
                    ,stan_raw_score = m.score_01 * 1
                    ,stan_scale_score = m.score_02 * 1
                    ,stan_ge = m.score_03 * 1
                    ,stan_nce = m.score_05 * 1
                    ,stan_natl_prct = m.score_06 * 1
                    ,stan_natl_stanine = m.score_07 * 1;


END PROC;
//
