/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_calc_rubrics.sql $
$Id: etl_bm_calc_rubrics.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_bm_calc_rubrics //

create definer=`dbadmin`@`localhost` procedure etl_bm_calc_rubrics()
CONTAINS SQL
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
SQL SECURITY INVOKER

BEGIN 

##################################
## pmi_ods_scan_results cleanup ##
##################################

    UPDATE sam_answer_key SET
    answer = lower(answer)
    where answer rlike '^[[:alpha:]]$';
    
    UPDATE sam_student_response SET
    student_answer = lower(student_answer)
    where student_answer rlike '^[[:alpha:]]$';
        
#############################################
# sam_student_response-scoring
#############################################
    
    #-- Update internally graded responses...
    UPDATE  sam_student_response as er
    JOIN  (
            SELECT 
                 er.test_id
                ,er.test_event_id
                ,er.student_id
                ,er.test_question_id
                ,max(round(
                  case sm.scoring_method_code when 's' then (case when er.student_answer = ak.answer then r.rubric_total else 0 end)
                                             when 'c' then (case when coalesce(er.student_answer, er.rubric_score) = cast(cast(rl.rubric_score as signed) as char(3)) then rl.rubric_value else 0 end)
                                             when 'g' then (case when er.student_answer * 1 = ak.answer * 1 then r.rubric_total else 0 end)
                                             else null
                                             end, 3)) as rubric_value
                ,max(round(
                  case sm.scoring_method_code when 's' then (case when er.student_answer = ak.answer then 1 else 0 end)
                                             when 'c' then (case when coalesce(er.student_answer, er.rubric_score) = cast(cast(rl.rubric_score as signed) as char(3)) then cast(cast(rl.rubric_score as signed) as char(3)) else 0 end)
                                             when 'g' then (case when er.student_answer * 1 = ak.answer * 1 then 1 else 0 end)
                                             else null
                                             end, 3)) as rubric_score
            FROM    sam_student_response as er
            JOIN    sam_answer_key as ak
                    ON  ak.test_id = er.test_id
                    AND ak.test_question_id = er.test_question_id
            JOIN    sam_rubric as r
                    ON  r.rubric_id = ak.rubric_id
            JOIN    sam_rubric_list as rl
                    ON  rl.rubric_id = r.rubric_id
            JOIN    sam_test t
                    ON  t.test_id = ak.test_id
                    AND t.external_grading_flag = 0  -- only internal responses...
            JOIN    sam_question_type qt
                    ON  qt.question_type_id = ak.question_type_id
            JOIN    sam_scoring_method sm
                    ON  sm.scoring_method_id = qt.scoring_method_id
            WHERE   er.gui_edit_flag <> 1 
            GROUP BY er.test_id, er.test_event_id, er.student_id, er.test_question_id ) as dt
        ON  dt.test_id = er.test_id
        AND dt.test_event_id = er.test_event_id
        AND dt.test_question_id = er.test_question_id
        AND dt.student_id = er.student_id
    SET    er.rubric_value = dt.rubric_value
            ,er.rubric_score = dt.rubric_score
            ,er.last_user_id = 1234
    WHERE   er.gui_edit_flag <> 1;


    #-- Update externally graded responses...
    UPDATE sam_student_response er
        JOIN    sam_test t
            ON      t.test_id = er.test_id
            AND     t.external_grading_flag = 1
        JOIN    sam_answer_key ak
            ON      er.test_id = ak.test_id
            AND     er.test_question_id = ak.test_question_id
        JOIN    sam_rubric r
            ON      ak.rubric_id = r.rubric_id
        JOIN    sam_rubric_list rl
            ON      rl.rubric_id = r.rubric_id
            AND     er.student_answer = rl.rubric_score
    SET er.rubric_value = rl.rubric_value,
        er.rubric_score = rl.rubric_score
    WHERE   er.gui_edit_flag <> 1;


    #-- Cleanup for newly imported nulls/blanks...
    UPDATE sam_student_response SET
    rubric_value = 0
    WHERE student_answer is null;

    UPDATE sam_student_response 
    SET rubric_value = 0
    WHERE rubric_value IS NULL;


END;
//
