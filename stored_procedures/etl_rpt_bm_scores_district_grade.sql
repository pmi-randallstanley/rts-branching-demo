/*
$Rev: 9366 $ 
$Author: randall.stanley $ 
$Date: 2010-10-06 11:28:38 -0400 (Wed, 06 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bm_scores_district_grade.sql $
$Id: etl_rpt_bm_scores_district_grade.sql 9366 2010-10-06 15:28:38Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_rpt_bm_scores_district_grade //

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_bm_scores_district_grade`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 9366 $ $Date: 2010-10-06 11:28:38 -0400 (Wed, 06 Oct 2010) $'
BEGIN

    truncate TABLE rpt_bm_scores_district_grade;
    
    INSERT INTO rpt_bm_scores_district_grade (
       curriculum_id
       ,test_id
       ,grade_level_id
       ,ayp_subject_id
       ,ayp_strand_id
       ,points_earned
       ,points_possible
       ,last_user_id
    )
    
    select  rsbs.curriculum_id
       ,rsbs.test_id
       ,sty.grade_level_id
       ,rsbs.ayp_subject_id
       ,rsbs.ayp_strand_id
       ,sum(rsbs.points_earned) as points_earned
       ,sum(rsbs.points_possible) as points_possible
       ,1234
    
    from     rpt_bm_scores as rsbs
    join     c_school_year as sy
             on    sy.active_flag = 1
    join     c_student_year as sty
             on     sty.student_id = rsbs.student_id 
             and   sty.school_year_id = sy.school_year_id
             and   sty.active_flag = 1
    group by rsbs.curriculum_id , rsbs.test_id, sty.grade_level_id, rsbs.ayp_subject_id, rsbs.ayp_strand_id
    ;

END;
//
