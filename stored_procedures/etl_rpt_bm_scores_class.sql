/*
$Rev: 9366 $ 
$Author: randall.stanley $ 
$Date: 2010-10-06 11:28:38 -0400 (Wed, 06 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bm_scores_class.sql $
$Id: etl_rpt_bm_scores_class.sql 9366 2010-10-06 15:28:38Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_rpt_bm_scores_class //

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_bm_scores_class`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 9366 $ $Date: 2010-10-06 11:28:38 -0400 (Wed, 06 Oct 2010) $'
BEGIN

    TRUNCATE TABLE rpt_bm_scores_class;
    
    INSERT INTO rpt_bm_scores_class (
       class_id
       ,test_id
       ,curriculum_id
       ,ayp_subject_id
       ,ayp_strand_id
       ,points_earned
       ,points_possible
       ,last_user_id
    ) 
       
    SELECT   cle.class_id
       ,rsbs.test_id
       ,rsbs.curriculum_id
       ,rsbs.ayp_subject_id
       ,rsbs.ayp_strand_id
       ,SUM(rsbs.points_earned) AS points_earned
       ,SUM(rsbs.points_possible) AS points_possible
       ,1234
    
    FROM     rpt_bm_scores AS rsbs
    JOIN     c_school_year AS sy
             ON    sy.active_flag = 1
    JOIN     c_class_enrollment AS cle
             ON    cle.student_id = rsbs.student_id
    JOIN     c_class AS cl
             ON    cl.class_id = cle.class_id
    GROUP BY cle.class_id, rsbs.test_id, rsbs.curriculum_id, rsbs.ayp_subject_id, rsbs.ayp_strand_id
    ;

END;
//
