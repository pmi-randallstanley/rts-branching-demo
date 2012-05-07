DROP PROCEDURE IF EXISTS etl_rpt_question_scores_district //

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_question_scores_district`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
BEGIN
    
    truncate table rpt_question_scores_district;
    
    insert into rpt_question_scores_district (
       test_id, 
       test_question_id, 
       points_earned, 
       points_possible, 
       last_user_id
       
       ) 
    select  ak.test_id,
       ak.test_question_id,
       sum(coalesce(er.rubric_value, 0)) as points_earned,
       sum(r.rubric_total) as points_possible,
       1234
    
    from    sam_answer_key as ak
    join    sam_rubric as r
            on      ak.rubric_id = r.rubric_id
            and     r.rubric_total != 0
    join   sam_student_response as er
            on      ak.test_id = er.test_id
            and     ak.test_question_id = er.test_question_id
    join   c_student as stu
            on      er.student_id = stu.student_id
            and     stu.active_flag = 1
    group by ak.test_id, ak.test_question_id
    
    union
    
    select  ak.test_id,
       ak.test_question_id,
       sum(coalesce(er.rubric_value, 0)) as points_earned,
       sum(r.rubric_total) as points_possible,
       1234
    
    from    sam_answer_key as ak
    join    sam_rubric as r
            on      ak.rubric_id = r.rubric_id
            and     r.rubric_total != 0
    left join   sam_student_response as er
            on      ak.test_id = er.test_id
            and     ak.test_question_id = er.test_question_id
    where   er.student_id is null
    group by ak.test_id, ak.test_question_id
    
    ;
    
    optimize table rpt_question_scores_district;

END;
//
