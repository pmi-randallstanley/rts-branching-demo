/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_test_scores.sql $
$Id: etl_rpt_test_scores.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_rpt_test_scores //

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_test_scores`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
BEGIN
    
truncate TABLE rpt_test_scores;

DROP TABLE IF EXISTS tmp_rpt_test_scores_temp;

CREATE TABLE tmp_rpt_test_scores_temp
SELECT  t.test_id
    ,te.test_event_id
    ,ts.student_id
    ,ak.test_question_id
    ,r.rubric_total
    ,t.moniker
    ,te.start_date AS test_date
    ,t.course_type_id
    ,te.include_wavg_calc_flag

FROM    sam_test t
JOIN    sam_test_event AS te
        ON    te.test_id = t.test_id
JOIN    sam_test_student AS ts
        ON    ts.test_id = te.test_id
        AND   ts.test_event_id = te.test_event_id
        AND EXISTS  (   SELECT  *
                            FROM    sam_student_response AS ts2
                            WHERE   ts2.test_id = ts.test_id
                            AND     ts2.student_id = ts.student_id
                            AND     ts2.test_event_id = ts.test_event_id
                        )
JOIN    sam_answer_key ak
        ON    ak.test_id = t.test_id         
JOIN    sam_rubric r
        ON    r.rubric_id = ak.rubric_id
WHERE         t.purge_flag = 0
        AND   te.purge_flag = 0
        AND   r.rubric_total != 0;


CREATE index ind_tmp_rpt_test_scores_temp ON tmp_rpt_test_scores_temp(test_id, test_event_id, student_id, test_question_id);

INSERT INTO rpt_test_scores (
    test_id
    , test_event_id
    , student_id
    , test_name
    , test_date
    , course_type_id
    , points_earned
    , points_possible
    , include_wavg_flag
    , last_user_id
    ) 

SELECT   er.test_id
   ,er.test_event_id
   ,er.student_id
   ,MIN(tmp1.moniker)
   ,MIN(tmp1.test_date)
   ,MIN(tmp1.course_type_id)
   ,SUM(er.rubric_value)
   ,SUM(tmp1.rubric_total)
   ,MIN(tmp1.include_wavg_calc_flag)
   ,1234

FROM    tmp_rpt_test_scores_temp AS tmp1
JOIN    sam_student_response er
     ON    er.test_id = tmp1.test_id
     AND   er.test_event_id = tmp1.test_event_id
     AND   er.student_id = tmp1.student_id
     AND   er.test_question_id = tmp1.test_question_id
        join    c_student as st
            on      st.student_id = er.student_id
            and     st.active_flag = 1
WHERE      er.rubric_value IS NOT NULL
GROUP BY er.test_id, er.test_event_id, er.student_id
;

DROP TABLE IF EXISTS tmp_rpt_test_scores_temp;

END;
//
