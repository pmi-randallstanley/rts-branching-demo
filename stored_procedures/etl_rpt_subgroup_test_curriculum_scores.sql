# etl_rpt_subgroup_test_curriculum_scores.sql
# RStanley  9-Jun-2011 01:34:19 PM -0400 on Thursday

DROP PROCEDURE IF EXISTS `etl_rpt_subgroup_test_curriculum_scores` //

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_subgroup_test_curriculum_scores`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT 'tbd'
BEGIN

    truncate TABLE rpt_subgroup_test_curriculum_scores;
    
    insert rpt_subgroup_test_curriculum_scores (
        curriculum_id
        ,ayp_group_id
        ,test_id
        ,points_earned
        ,points_possible
        ,last_user_id
    )
    
    select  rtcs.curriculum_id
        ,rsg.ayp_group_id
        ,rtcs.test_id
       ,sum(rtcs.points_earned) as points_earned
       ,sum(rtcs.points_possible) as points_possible
       ,1234
    
    from    rpt_test_curriculum_scores as rtcs
    join    c_school_year as sy
            on      sy.active_flag = 1
    join    c_student_year as sty
            on      sty.student_id = rtcs.student_id
            and     sty.school_year_id = sy.school_year_id
            and     sty.active_flag = 1
    join    rpt_student_group as rsg
            on      rtcs.student_id = rsg.student_id
            and     rsg.school_year_id = sy.school_year_id
    join    c_ayp_group as ag
            on      rsg.ayp_group_id = ag.ayp_group_id
            and     ag.ayp_accel_flag = 1
    group by rtcs.curriculum_id, rsg.ayp_group_id, rtcs.test_id
    ;

END;
//
