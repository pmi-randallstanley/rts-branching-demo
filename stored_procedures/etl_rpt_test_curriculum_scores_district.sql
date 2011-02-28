DROP PROCEDURE IF EXISTS `etl_rpt_test_curriculum_scores_district` //

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_test_curriculum_scores_district`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev$ $Date$'
BEGIN

    truncate TABLE rpt_test_curriculum_scores_district;
    
    INSERT INTO rpt_test_curriculum_scores_district (
       test_id
       ,curriculum_id
       ,points_earned
       ,points_possible
       ,last_user_id
    )
    
    select  rtcs.test_id
       ,rtcs.curriculum_id
       ,sum(rtcs.points_earned) as points_earned
       ,sum(rtcs.points_possible) as points_possible
       ,1234
    
    from     rpt_test_curriculum_scores as rtcs
    group by rtcs.test_id, rtcs.curriculum_id
    ;

END;
//
