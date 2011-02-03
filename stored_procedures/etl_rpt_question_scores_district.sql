/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_question_scores_district.sql $
$Id: etl_rpt_question_scores_district.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
 */

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
    left join   sam_student_response as er
            on      ak.test_id = er.test_id
            and     ak.test_question_id = er.test_question_id
    group by ak.test_id, ak.test_question_id
    ;

END;
//
