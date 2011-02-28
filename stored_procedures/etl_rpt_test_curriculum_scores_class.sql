DROP PROCEDURE IF EXISTS `etl_rpt_test_curriculum_scores_class` //

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_test_curriculum_scores_class`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev$ $Date$'
BEGIN

    TRUNCATE TABLE rpt_test_curriculum_scores_class;
    
    INSERT INTO rpt_test_curriculum_scores_class (
       class_id
       ,test_id
       ,curriculum_id
       ,points_earned
       ,points_possible
       ,last_user_id
    ) 
       
    SELECT   cle.class_id
       ,rtcs.test_id
       ,rtcs.curriculum_id
       ,SUM(rtcs.points_earned) AS points_earned
       ,SUM(rtcs.points_possible) AS points_possible
       ,1234
    
    FROM     rpt_test_curriculum_scores AS rtcs
    JOIN     c_school_year AS sy
             ON    sy.active_flag = 1
    JOIN     c_class_enrollment AS cle
             ON    cle.student_id = rtcs.student_id
    JOIN     c_class AS cl
             ON    cl.class_id = cle.class_id
    GROUP BY cle.class_id, rtcs.test_id, rtcs.curriculum_id
    ;

END;
//
