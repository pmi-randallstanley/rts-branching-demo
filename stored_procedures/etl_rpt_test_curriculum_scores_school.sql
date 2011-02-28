DROP PROCEDURE IF EXISTS `etl_rpt_test_curriculum_scores_school` //

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_test_curriculum_scores_school`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev$ $Date$'
BEGIN

    truncate TABLE rpt_test_curriculum_scores_school;
    
    INSERT INTO rpt_test_curriculum_scores_school (
       school_id
       ,test_id
       ,curriculum_id
       ,points_earned
       ,points_possible
       ,last_user_id
    )
    
    select   sty.school_id
       ,rtcs.test_id
       ,rtcs.curriculum_id
       ,sum(rtcs.points_earned) as points_earned
       ,sum(rtcs.points_possible) as points_possible
       ,1234
    
    from     rpt_test_curriculum_scores as rtcs
    join     c_school_year as sy
             on    sy.active_flag = 1
    join     c_student_year as sty
             on     sty.student_id = rtcs.student_id
             and    sty.school_year_id = sy.school_year_id
             and    sty.active_flag = 1
    group by sty.school_id, rtcs.test_id, rtcs.curriculum_id
    ;

END;
//
