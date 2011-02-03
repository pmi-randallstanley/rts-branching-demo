/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_test_curriculum_scores.sql $
$Id: etl_rpt_test_curriculum_scores.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_rpt_test_curriculum_scores//

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_test_curriculum_scores`()
CONTAINS SQL
SQL SECURITY INVOKER
comment '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
BEGIN
    
truncate table rpt_test_curriculum_scores;

INSERT INTO rpt_test_curriculum_scores (
   student_id, 
   curriculum_id, 
   test_id, 
   points_earned, 
   points_possible,
   last_user_id
) 


SELECT
    er.student_id,
    al.curriculum_id,
    t.test_id,
    SUM(er.rubric_value),
    SUM(r.rubric_total),
    1234
FROM    sam_test t
JOIN    sam_answer_key ak
        ON   ak.test_id = t.test_id         
JOIN    sam_rubric r
        ON   r.rubric_id = ak.rubric_id
        AND  r.rubric_total != 0
JOIN    sam_test_event te
        ON   te.test_id = t.test_id
JOIN    sam_student_response er
        ON   er.test_id = ak.test_id
        AND  er.test_event_id = te.test_event_id
        AND  er.test_question_id = ak.test_question_id
JOIN    sam_alignment_list al
        ON   ak.test_id = al.test_id
        AND  ak.test_question_id = al.test_question_id
WHERE        t.purge_flag = 0
        AND  te.purge_flag = 0
-- er.rubric_value is not null
GROUP BY er.student_id, er.test_id, al.curriculum_id;



    
END;
//
