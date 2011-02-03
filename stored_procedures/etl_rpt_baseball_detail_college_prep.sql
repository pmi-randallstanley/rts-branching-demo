/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_baseball_detail_college_prep.sql $
$Id: etl_rpt_baseball_detail_college_prep.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
 */

####################################################################
# Insert sat/psat data into rpt tables for baseball report.
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_rpt_baseball_detail_college_prep//

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_baseball_detail_college_prep()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'

PROC: BEGIN 
    
                    
    ##############################################################
    # Insert College Prep data
    ##############################################################

    INSERT INTO rpt_baseball_detail_college_prep
        (
        bb_group_id
        , bb_measure_id
        , bb_measure_item_id
        , student_id
        , school_year_id
        , score
        , score_color
        , last_user_id
        , create_timestamp
        ) 
        # sat/psat 
        SELECT  bbg.bb_group_id AS bb_group_id
                ,bbm.bb_measure_id AS bb_measure_id
                ,0
                ,grp.student_id
                ,grp.test_year
                ,grp.score
                ,cl.moniker
                ,1234
                ,CURRENT_TIMESTAMP
        FROM
            (   SELECT  ts.student_id
                        ,tty.test_type_id
                        ,tsub.subject_id
                        ,CASE WHEN tsub.subject_code LIKE '%MaxMvwSum' THEN CONCAT(tty.test_type_code,'Total')
                            ELSE tsub.subject_code END AS subject_code
                        ,MAX(ts.test_year) AS test_year
                        ,MAX(ts.score) AS score
                FROM pm_natl_test_scores AS ts
                JOIN pm_natl_test_subject AS tsub
                    ON   tsub.subject_id = ts.subject_id
                    AND  tsub.test_type_id = ts.test_type_id
                JOIN pm_natl_test_type AS tty
                    ON   tty.test_type_id = ts.test_type_id
                    AND  tty.test_type_code IN ('sat','psat')
                GROUP BY ts.student_id
                        ,ts.test_type_id
                        ,ts.subject_id
            ) AS grp
                    
        JOIN pm_natl_test_type AS tty
            ON   tty.test_type_id = grp.test_type_id
        JOIN pm_baseball_group AS bbg
            ON   bbg.bb_group_code = tty.test_type_code
            AND  bbg.bb_group_code IN ('sat', 'psat')
        JOIN pm_baseball_measure AS bbm
            ON   bbm.bb_measure_code = grp.subject_code
        LEFT JOIN pm_color_natl_test_subject AS clrts
            ON   clrts.subject_id = grp.subject_id
            AND  grp.test_type_id = clrts.test_type_id
            AND  grp.test_year BETWEEN clrts.begin_year AND clrts.end_year
            AND  grp.score BETWEEN clrts.min_score AND clrts.max_score
        LEFT JOIN pmi_color AS cl
            ON cl.color_id = clrts.color_id

    ON DUPLICATE key UPDATE last_user_id = 1234
        ,score = values(score)
        ,score_color = values(score_color)
    ;

END PROC;
//
