/*
$Rev: 9375 $ 
$Author: randall.stanley $ 
$Date: 2010-10-07 08:57:01 -0400 (Thu, 07 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_ayp_group_results_school_grade.sql $
$Id: etl_rpt_ayp_group_results_school_grade.sql 9375 2010-10-07 12:57:01Z randall.stanley $ 
 */


DROP PROCEDURE IF EXISTS etl_rpt_ayp_group_results_school_grade //

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_ayp_group_results_school_grade`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 9375 $ $Date: 2010-10-07 08:57:01 -0400 (Thu, 07 Oct 2010) $'
BEGIN
    
truncate TABLE rpt_ayp_group_results_school_grade;

INSERT INTO rpt_ayp_group_results_school_grade (
   school_id, 
   ayp_subject_id, 
   ayp_group_id, 
   grade_level_id,
   school_year_id, 
   prof_count, 
   ayp_group_count,
   bm_pass_count,
   last_user_id
) 


SELECT   dt.school_id,
   dt.ayp_subject_id,
   dt.ayp_group_id,
   dt.grade_level_id,
   dt.school_year_id,
   dt.pass_count,
   dt.group_count,
   dt.bm_pass_count,
   1234
   
FROM   (

            SELECT   sty.school_id,
            ss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            ss.school_year_id,
            SUM(  CASE  WHEN  e.ethnicity_code = 'a' AND atta.pass_flag = 1 THEN 1 ELSE 0 END) AS pass_count,
            SUM(  CASE  WHEN  e.ethnicity_code = 'a' THEN 1 ELSE 0 END) AS group_count,
            COUNT(CASE WHEN e.ethnicity_code = 'a' AND cb.ayp_on_track_flag = 1 THEN ss.student_id END) AS bm_pass_count
            
            FROM   c_ayp_subject_student ss
            JOIN   (SELECT  tt.ayp_subject_id ,MAX(tty.school_year_id) AS yr_c FROM   c_ayp_test_type_year AS tty JOIN   c_ayp_subject tt ON tty.ayp_test_type_id = tt.ayp_test_type_id GROUP BY tt.ayp_subject_id) dt
                ON   dt.ayp_subject_id = ss.ayp_subject_id
                AND  dt.yr_c = ss.school_year_id
            JOIN   c_student st
                ON   st.student_id = ss.student_id
                AND  st.active_flag = 1
                AND  EXISTS   (  SELECT    *
                                    FROM   c_class_enrollment AS cle
                                    WHERE   cle.student_id = st.student_id)
            JOIN   c_ethnicity AS e
                ON   e.ethnicity_id = st.ethnicity_id
            JOIN   c_student_year sty
                ON   sty.student_id = ss.student_id
            JOIN   c_school_year sy
                ON   sy.school_year_id = sty.school_year_id
                AND  sy.active_flag = 1
            JOIN   c_grade_level gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN   c_ayp_subject as sub
                ON   sub.ayp_subject_id = ss.ayp_subject_id
            JOIN   c_ayp_test_type_al as atta
                ON   atta.ayp_test_type_id = sub.ayp_test_type_id
            JOIN   c_ayp_achievement_level as aal
                ON   aal.al_id = atta.al_id
                AND  aal.al_id = ss.al_id
            JOIN   c_ayp_group ag
                ON   ag.ayp_group_code = 'ethnAsian'
            LEFT JOIN   rpt_profile_leading_subject_stu AS rplss
                ON   rplss.ayp_subject_id = ss.ayp_subject_id
                AND  rplss.student_id = ss.student_id
            LEFT JOIN   c_color_ayp_benchmark AS cb
                ON   rplss.ayp_subject_id = cb.ayp_subject_id
                AND  ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            
            WHERE   ss.al_id IS NOT NULL 
            AND     ss.score_record_flag = 1
            GROUP BY sty.school_id, ss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id, ss.school_year_id

            UNION ALL 
            
            SELECT   sty.school_id,
            ss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            ss.school_year_id,
            SUM(  CASE  WHEN  e.ethnicity_code = 'b' AND atta.pass_flag = 1 THEN 1 ELSE 0 END) AS pass_count,
            SUM(  CASE  WHEN  e.ethnicity_code = 'b' THEN 1 ELSE 0 END) AS group_count,
            COUNT(CASE WHEN e.ethnicity_code = 'b' AND cb.ayp_on_track_flag = 1 THEN ss.student_id END) AS bm_pass_count
            
            FROM   c_ayp_subject_student ss
            JOIN   (SELECT  tt.ayp_subject_id ,MAX(tty.school_year_id) AS yr_c FROM   c_ayp_test_type_year AS tty JOIN   c_ayp_subject tt ON tty.ayp_test_type_id = tt.ayp_test_type_id GROUP BY tt.ayp_subject_id) dt
                ON   dt.ayp_subject_id = ss.ayp_subject_id
                AND  dt.yr_c = ss.school_year_id
            JOIN   c_student st
                ON   st.student_id = ss.student_id
                AND  st.active_flag = 1
                AND  EXISTS   (  SELECT    *
                                    FROM   c_class_enrollment AS cle
                                    WHERE   cle.student_id = st.student_id)
            JOIN   c_ethnicity AS e
                ON   e.ethnicity_id = st.ethnicity_id
            JOIN   c_student_year sty
                ON   sty.student_id = ss.student_id
            JOIN   c_school_year sy
                ON   sy.school_year_id = sty.school_year_id
                AND  sy.active_flag = 1
            JOIN   c_grade_level gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN   c_ayp_subject as sub
                ON   sub.ayp_subject_id = ss.ayp_subject_id
            JOIN   c_ayp_test_type_al as atta
                ON   atta.ayp_test_type_id = sub.ayp_test_type_id
            JOIN   c_ayp_achievement_level as aal
                ON   aal.al_id = atta.al_id
                AND  aal.al_id = ss.al_id
            JOIN   c_ayp_group ag
                ON   ag.ayp_group_code = 'ethnBlack'
            LEFT JOIN   rpt_profile_leading_subject_stu AS rplss
                ON   rplss.ayp_subject_id = ss.ayp_subject_id
                AND  rplss.student_id = ss.student_id
            LEFT JOIN   c_color_ayp_benchmark AS cb
                ON   rplss.ayp_subject_id = cb.ayp_subject_id
                AND  ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            WHERE   ss.al_id IS NOT NULL 
            AND     ss.score_record_flag = 1
            GROUP BY sty.school_id, ss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id, ss.school_year_id
         
            UNION ALL 
            
            SELECT   sty.school_id,
            ss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            ss.school_year_id,
            SUM(  CASE  WHEN  e.ethnicity_code = 'p' AND atta.pass_flag = 1 THEN 1 ELSE 0 END) AS pass_count,
            SUM(  CASE  WHEN  e.ethnicity_code = 'p' THEN 1 ELSE 0 END) AS group_count,
            COUNT(CASE WHEN e.ethnicity_code = 'p' AND cb.ayp_on_track_flag = 1 THEN ss.student_id END) AS bm_pass_count
            
            FROM   c_ayp_subject_student ss
            JOIN   (SELECT  tt.ayp_subject_id ,MAX(tty.school_year_id) AS yr_c FROM   c_ayp_test_type_year AS tty JOIN   c_ayp_subject tt ON tty.ayp_test_type_id = tt.ayp_test_type_id GROUP BY tt.ayp_subject_id) dt
                ON   dt.ayp_subject_id = ss.ayp_subject_id
                AND  dt.yr_c = ss.school_year_id
            JOIN   c_student st
                ON   st.student_id = ss.student_id
                AND  st.active_flag = 1
                AND  EXISTS   (  SELECT    *
                                    FROM   c_class_enrollment AS cle
                                    WHERE   cle.student_id = st.student_id)
            JOIN   c_ethnicity AS e
                ON   e.ethnicity_id = st.ethnicity_id
            JOIN   c_student_year sty
                ON   sty.student_id = ss.student_id
            JOIN   c_school_year sy
                ON   sy.school_year_id = sty.school_year_id
                AND  sy.active_flag = 1
            JOIN   c_grade_level gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN   c_ayp_subject as sub
                ON   sub.ayp_subject_id = ss.ayp_subject_id
            JOIN   c_ayp_test_type_al as atta
                ON   atta.ayp_test_type_id = sub.ayp_test_type_id
            JOIN   c_ayp_achievement_level as aal
                ON   aal.al_id = atta.al_id
                AND  aal.al_id = ss.al_id
            JOIN   c_ayp_group ag
                ON   ag.ayp_group_code = 'ethnHawaiian'
            LEFT JOIN   rpt_profile_leading_subject_stu AS rplss
                ON   rplss.ayp_subject_id = ss.ayp_subject_id
                AND  rplss.student_id = ss.student_id
            LEFT JOIN   c_color_ayp_benchmark AS cb
                ON   rplss.ayp_subject_id = cb.ayp_subject_id
                AND  ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            WHERE   ss.al_id IS NOT NULL 
            AND     score_record_flag = 1
            GROUP BY sty.school_id, ss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id, ss.school_year_id
            
            UNION ALL 
            
            SELECT   sty.school_id,
            ss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            ss.school_year_id,
            SUM(  CASE  WHEN  e.ethnicity_code = 'h' AND atta.pass_flag = 1 THEN 1 ELSE 0 END) AS pass_count,
            SUM(  CASE  WHEN  e.ethnicity_code = 'h' THEN 1 ELSE 0 END) AS group_count,
            COUNT(CASE WHEN e.ethnicity_code = 'h' AND cb.ayp_on_track_flag = 1 THEN ss.student_id END) AS bm_pass_count
            
            FROM   c_ayp_subject_student ss
            JOIN   (SELECT  tt.ayp_subject_id ,MAX(tty.school_year_id) AS yr_c FROM   c_ayp_test_type_year AS tty JOIN   c_ayp_subject tt ON tty.ayp_test_type_id = tt.ayp_test_type_id GROUP BY tt.ayp_subject_id) dt
                ON   dt.ayp_subject_id = ss.ayp_subject_id
                AND  dt.yr_c = ss.school_year_id
            JOIN   c_student st
                ON   st.student_id = ss.student_id
                AND  st.active_flag = 1
                AND  EXISTS   (  SELECT    *
                                    FROM   c_class_enrollment AS cle
                                    WHERE   cle.student_id = st.student_id)
            JOIN   c_ethnicity AS e
                ON   e.ethnicity_id = st.ethnicity_id
            JOIN   c_student_year sty
                ON   sty.student_id = ss.student_id
            JOIN   c_school_year sy
                ON   sy.school_year_id = sty.school_year_id
                AND  sy.active_flag = 1
            JOIN   c_grade_level gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN   c_ayp_subject as sub
                ON   sub.ayp_subject_id = ss.ayp_subject_id
            JOIN   c_ayp_test_type_al as atta
                ON   atta.ayp_test_type_id = sub.ayp_test_type_id
            JOIN   c_ayp_achievement_level as aal
                ON   aal.al_id = atta.al_id
                AND  aal.al_id = ss.al_id
            JOIN   c_ayp_group ag
                ON   ag.ayp_group_code = 'ethnHispanic'
            LEFT JOIN   rpt_profile_leading_subject_stu AS rplss
                ON   rplss.ayp_subject_id = ss.ayp_subject_id
                AND  rplss.student_id = ss.student_id
            LEFT JOIN   c_color_ayp_benchmark AS cb
                ON   rplss.ayp_subject_id = cb.ayp_subject_id
                AND  ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            WHERE   ss.al_id IS NOT NULL 
            AND     ss.score_record_flag = 1
            GROUP BY sty.school_id, ss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id, ss.school_year_id
            
            UNION ALL 
            
            SELECT   sty.school_id,
            ss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            ss.school_year_id,
            SUM(  CASE  WHEN  e.ethnicity_code = 'i' AND atta.pass_flag = 1 THEN 1 ELSE 0 END) AS pass_count,
            SUM(  CASE  WHEN  e.ethnicity_code = 'i' THEN 1 ELSE 0 END) AS group_count,
            COUNT(CASE WHEN e.ethnicity_code = 'i' AND cb.ayp_on_track_flag = 1 THEN ss.student_id END) AS bm_pass_count
            
            FROM   c_ayp_subject_student ss
            JOIN   (SELECT  tt.ayp_subject_id ,MAX(tty.school_year_id) AS yr_c FROM   c_ayp_test_type_year AS tty JOIN   c_ayp_subject tt ON tty.ayp_test_type_id = tt.ayp_test_type_id GROUP BY tt.ayp_subject_id) dt
                ON   dt.ayp_subject_id = ss.ayp_subject_id
                AND  dt.yr_c = ss.school_year_id
            JOIN   c_student st
                ON   st.student_id = ss.student_id
                AND  st.active_flag = 1
                AND  EXISTS   (  SELECT    *
                                    FROM   c_class_enrollment AS cle
                                    WHERE   cle.student_id = st.student_id)
            JOIN   c_ethnicity AS e
                ON   e.ethnicity_id = st.ethnicity_id
            JOIN   c_student_year sty
                ON   sty.student_id = ss.student_id
            JOIN   c_school_year sy
                ON   sy.school_year_id = sty.school_year_id
                AND  sy.active_flag = 1
            JOIN   c_grade_level gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN   c_ayp_subject as sub
                ON   sub.ayp_subject_id = ss.ayp_subject_id
            JOIN   c_ayp_test_type_al as atta
                ON   atta.ayp_test_type_id = sub.ayp_test_type_id
            JOIN   c_ayp_achievement_level as aal
                ON   aal.al_id = atta.al_id
                AND  aal.al_id = ss.al_id
            JOIN   c_ayp_group ag
                ON   ag.ayp_group_code = 'ethnIndian'
            LEFT JOIN   rpt_profile_leading_subject_stu AS rplss
                ON   rplss.ayp_subject_id = ss.ayp_subject_id
                AND  rplss.student_id = ss.student_id
            LEFT JOIN   c_color_ayp_benchmark AS cb
                ON   rplss.ayp_subject_id = cb.ayp_subject_id
                AND  ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            WHERE   ss.al_id IS NOT NULL 
            AND     ss.score_record_flag = 1
            GROUP BY sty.school_id, ss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id, ss.school_year_id
            
            UNION ALL 
            
            SELECT   sty.school_id,
            ss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            ss.school_year_id,
            SUM(  CASE  WHEN  e.ethnicity_code = 'm' AND atta.pass_flag = 1 THEN 1 ELSE 0 END) AS pass_count,
            SUM(  CASE  WHEN  e.ethnicity_code = 'm' THEN 1 ELSE 0 END) AS group_count,
            COUNT(CASE WHEN e.ethnicity_code = 'm' AND cb.ayp_on_track_flag = 1 THEN ss.student_id END) AS bm_pass_count
            
            FROM   c_ayp_subject_student ss
            JOIN   (SELECT  tt.ayp_subject_id ,MAX(tty.school_year_id) AS yr_c FROM   c_ayp_test_type_year AS tty JOIN   c_ayp_subject tt ON tty.ayp_test_type_id = tt.ayp_test_type_id GROUP BY tt.ayp_subject_id) dt
                ON   dt.ayp_subject_id = ss.ayp_subject_id
                AND  dt.yr_c = ss.school_year_id
            JOIN   c_student st
                ON   st.student_id = ss.student_id
                AND  st.active_flag = 1
                AND  EXISTS   (  SELECT    *
                                    FROM   c_class_enrollment AS cle
                                    WHERE   cle.student_id = st.student_id)
            JOIN   c_ethnicity AS e
                ON   e.ethnicity_id = st.ethnicity_id
            JOIN   c_student_year sty
                ON   sty.student_id = ss.student_id
            JOIN   c_school_year sy
                ON   sy.school_year_id = sty.school_year_id
                AND  sy.active_flag = 1
            JOIN   c_grade_level gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN   c_ayp_subject as sub
                ON   sub.ayp_subject_id = ss.ayp_subject_id
            JOIN   c_ayp_test_type_al as atta
                ON   atta.ayp_test_type_id = sub.ayp_test_type_id
            JOIN   c_ayp_achievement_level as aal
                ON   aal.al_id = atta.al_id
                AND  aal.al_id = ss.al_id
            JOIN   c_ayp_group ag
                ON   ag.ayp_group_code = 'ethnMulti'
            LEFT JOIN   rpt_profile_leading_subject_stu AS rplss
                ON   rplss.ayp_subject_id = ss.ayp_subject_id
                AND  rplss.student_id = ss.student_id
            LEFT JOIN   c_color_ayp_benchmark AS cb
                ON   rplss.ayp_subject_id = cb.ayp_subject_id
                AND  ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            WHERE   ss.al_id IS NOT NULL 
            AND     score_record_flag = 1
            GROUP BY sty.school_id, ss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id, ss.school_year_id
            
            UNION ALL 
            
            SELECT   sty.school_id,
            ss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            ss.school_year_id,
            SUM(  CASE  WHEN  e.ethnicity_code = 'w' AND atta.pass_flag = 1 THEN 1 ELSE 0 END) AS pass_count,
            SUM(  CASE  WHEN  e.ethnicity_code = 'w' THEN 1 ELSE 0 END) AS group_count,
            COUNT(CASE WHEN e.ethnicity_code = 'w' AND cb.ayp_on_track_flag = 1 THEN ss.student_id END) AS bm_pass_count
            
            FROM   c_ayp_subject_student ss
            JOIN   (SELECT  tt.ayp_subject_id ,MAX(tty.school_year_id) AS yr_c FROM   c_ayp_test_type_year AS tty JOIN   c_ayp_subject tt ON tty.ayp_test_type_id = tt.ayp_test_type_id GROUP BY tt.ayp_subject_id) dt
                ON   dt.ayp_subject_id = ss.ayp_subject_id
                AND  dt.yr_c = ss.school_year_id
            JOIN   c_student st
                ON   st.student_id = ss.student_id
                AND  st.active_flag = 1
                AND  EXISTS   (  SELECT    *
                                    FROM   c_class_enrollment AS cle
                                    WHERE   cle.student_id = st.student_id)
            JOIN   c_ethnicity AS e
                ON   e.ethnicity_id = st.ethnicity_id
            JOIN   c_student_year sty
                ON   sty.student_id = ss.student_id
            JOIN   c_school_year sy
                ON   sy.school_year_id = sty.school_year_id
                AND  sy.active_flag = 1
            JOIN   c_grade_level gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN   c_ayp_subject as sub
                ON   sub.ayp_subject_id = ss.ayp_subject_id
            JOIN   c_ayp_test_type_al as atta
                ON   atta.ayp_test_type_id = sub.ayp_test_type_id
            JOIN   c_ayp_achievement_level as aal
                ON   aal.al_id = atta.al_id
                AND  aal.al_id = ss.al_id
            JOIN   c_ayp_group ag
                ON   ag.ayp_group_code = 'ethnWhite'
            LEFT JOIN   rpt_profile_leading_subject_stu AS rplss
                ON   rplss.ayp_subject_id = ss.ayp_subject_id
                AND  rplss.student_id = ss.student_id
            LEFT JOIN   c_color_ayp_benchmark AS cb
                ON   rplss.ayp_subject_id = cb.ayp_subject_id
                AND  ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            WHERE   ss.al_id IS NOT NULL 
            AND     ss.score_record_flag = 1
            GROUP BY sty.school_id, ss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id, ss.school_year_id
         ) AS dt
   ON   DUPLICATE KEY UPDATE 
   prof_count = dt.pass_count,
   ayp_group_count =  dt.group_count,
   bm_pass_count =    dt.bm_pass_count,
   last_user_id = 1234;
   


INSERT INTO rpt_ayp_group_results_school_grade (
   school_id, 
   ayp_subject_id, 
   ayp_group_id, 
   grade_level_id,
   school_year_id, 
   prof_count, 
   ayp_group_count, 
   bm_pass_count,
   last_user_id
) 


SELECT   dt.school_id,
   dt.ayp_subject_id,
   dt.ayp_group_id,
   dt.grade_level_id,
   dt.school_year_id,
   dt.pass_count,
   dt.group_count,
   dt.bm_pass_count,
   1234
   
FROM   (

            SELECT   sty.school_id,
            ss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            ss.school_year_id,
            SUM(  CASE  WHEN  sty.swd_flag = 1 AND atta.pass_flag = 1 THEN 1 ELSE 0 END) AS pass_count,
            SUM(  CASE  WHEN  sty.swd_flag = 1 THEN 1 ELSE 0 END) AS group_count,
            COUNT(CASE WHEN sty.swd_flag = 1 AND cb.ayp_on_track_flag = 1 THEN ss.student_id END) AS bm_pass_count
            
            FROM   c_ayp_subject_student ss
            JOIN   (SELECT  tt.ayp_subject_id ,MAX(tty.school_year_id) AS yr_c FROM   c_ayp_test_type_year AS tty JOIN   c_ayp_subject tt ON tty.ayp_test_type_id = tt.ayp_test_type_id GROUP BY tt.ayp_subject_id) dt
                ON   dt.ayp_subject_id = ss.ayp_subject_id
                AND  dt.yr_c = ss.school_year_id
            JOIN   c_student st
                ON   st.student_id = ss.student_id
                AND  st.active_flag = 1
                AND  EXISTS   (  SELECT    *
                                    FROM   c_class_enrollment AS cle
                                    WHERE   cle.student_id = st.student_id)
            JOIN   c_student_year sty
                ON   sty.student_id = ss.student_id
            JOIN   c_school_year sy
                ON   sy.school_year_id = sty.school_year_id
                AND  sy.active_flag = 1
            JOIN   c_grade_level gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN   c_ayp_subject as sub
                ON   sub.ayp_subject_id = ss.ayp_subject_id
            JOIN   c_ayp_test_type_al as atta
                ON   atta.ayp_test_type_id = sub.ayp_test_type_id
            JOIN   c_ayp_achievement_level as aal
                ON   aal.al_id = atta.al_id
                AND  aal.al_id = ss.al_id
            JOIN   c_ayp_group ag
                ON   ag.ayp_group_code = 'swd'
            LEFT JOIN   rpt_profile_leading_subject_stu AS rplss
                ON   rplss.ayp_subject_id = ss.ayp_subject_id
                AND  rplss.student_id = ss.student_id
            LEFT JOIN   c_color_ayp_benchmark AS cb
                ON   rplss.ayp_subject_id = cb.ayp_subject_id
                AND  ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            WHERE   ss.al_id IS NOT NULL 
            AND     ss.score_record_flag = 1
            GROUP BY sty.school_id, ss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id, ss.school_year_id

            UNION ALL 
            
            SELECT   sty.school_id,
            ss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            ss.school_year_id,
            SUM(  CASE  WHEN  sty.lep_flag = 1 AND atta.pass_flag = 1 THEN 1 ELSE 0 END) AS pass_count,
            SUM(  CASE  WHEN  sty.lep_flag = 1 THEN 1 ELSE 0 END) AS group_count,
            COUNT(CASE WHEN sty.lep_flag = 1 AND cb.ayp_on_track_flag = 1 THEN ss.student_id END) AS bm_pass_count
            
            FROM   c_ayp_subject_student ss
            JOIN   (SELECT  tt.ayp_subject_id ,MAX(tty.school_year_id) AS yr_c FROM   c_ayp_test_type_year AS tty JOIN   c_ayp_subject tt ON tty.ayp_test_type_id = tt.ayp_test_type_id GROUP BY tt.ayp_subject_id) dt
                ON   dt.ayp_subject_id = ss.ayp_subject_id
                AND  dt.yr_c = ss.school_year_id
            JOIN   c_student st
                ON   st.student_id = ss.student_id
                AND  st.active_flag = 1
                AND  EXISTS   (  SELECT    *
                                    FROM   c_class_enrollment AS cle
                                    WHERE   cle.student_id = st.student_id)
            JOIN   c_student_year sty
                ON   sty.student_id = ss.student_id
            JOIN   c_school_year sy
                ON   sy.school_year_id = sty.school_year_id
                AND  sy.active_flag = 1
            JOIN   c_grade_level gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN   c_ayp_subject as sub
                ON   sub.ayp_subject_id = ss.ayp_subject_id
            JOIN   c_ayp_test_type_al as atta
                ON   atta.ayp_test_type_id = sub.ayp_test_type_id
            JOIN   c_ayp_achievement_level as aal
                ON   aal.al_id = atta.al_id
                AND  aal.al_id = ss.al_id
            JOIN   c_ayp_group ag
                ON   ag.ayp_group_code = 'lep'
            LEFT JOIN   rpt_profile_leading_subject_stu AS rplss
                ON   rplss.ayp_subject_id = ss.ayp_subject_id
                AND  rplss.student_id = ss.student_id
            LEFT JOIN   c_color_ayp_benchmark AS cb
                ON   rplss.ayp_subject_id = cb.ayp_subject_id
                AND  ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            WHERE   ss.al_id IS NOT NULL 
            AND     ss.score_record_flag = 1
            GROUP BY sty.school_id, ss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id, ss.school_year_id
         
            UNION ALL 
            
            SELECT   sty.school_id,
            ss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            ss.school_year_id,
            SUM(  CASE  WHEN  sty.econ_disadv_flag = 1 AND atta.pass_flag = 1 THEN 1 ELSE 0 END) AS pass_count,
            SUM(  CASE  WHEN  sty.econ_disadv_flag = 1 THEN 1 ELSE 0 END) AS group_count,
            COUNT(CASE WHEN sty.econ_disadv_flag = 1 AND cb.ayp_on_track_flag = 1 THEN ss.student_id END) AS bm_pass_count
            
            FROM   c_ayp_subject_student ss
            JOIN   (SELECT  tt.ayp_subject_id ,MAX(tty.school_year_id) AS yr_c FROM   c_ayp_test_type_year AS tty JOIN   c_ayp_subject tt ON tty.ayp_test_type_id = tt.ayp_test_type_id GROUP BY tt.ayp_subject_id) dt
                ON   dt.ayp_subject_id = ss.ayp_subject_id
                AND  dt.yr_c = ss.school_year_id
            JOIN   c_student st
                ON   st.student_id = ss.student_id
                AND  st.active_flag = 1
                AND  EXISTS   (  SELECT    *
                                    FROM   c_class_enrollment AS cle
                                    WHERE   cle.student_id = st.student_id)
            JOIN   c_student_year sty
                ON   sty.student_id = ss.student_id
            JOIN   c_school_year sy
                ON   sy.school_year_id = sty.school_year_id
                AND  sy.active_flag = 1
            JOIN   c_grade_level gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN   c_ayp_subject as sub
                ON   sub.ayp_subject_id = ss.ayp_subject_id
            JOIN   c_ayp_test_type_al as atta
                ON   atta.ayp_test_type_id = sub.ayp_test_type_id
            JOIN   c_ayp_achievement_level as aal
                ON   aal.al_id = atta.al_id
                AND  aal.al_id = ss.al_id
            JOIN   c_ayp_group ag
                ON   ag.ayp_group_code = 'econDisadv'
            LEFT JOIN   rpt_profile_leading_subject_stu AS rplss
                ON   rplss.ayp_subject_id = ss.ayp_subject_id
                AND  rplss.student_id = ss.student_id
            LEFT JOIN   c_color_ayp_benchmark AS cb
                ON   rplss.ayp_subject_id = cb.ayp_subject_id
                AND  ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            WHERE   ss.al_id IS NOT NULL
            AND     ss.score_record_flag = 1
            GROUP BY sty.school_id, ss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id, ss.school_year_id
            
            UNION ALL
                        
            SELECT   sty.school_id,
            ss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            ss.school_year_id,
            SUM(  CASE  WHEN  atta.pass_flag = 1 THEN 1 ELSE 0 END) AS pass_count,
            SUM(  CASE  WHEN  1=1 THEN 1 ELSE 0 END) AS group_count,
            COUNT(CASE WHEN cb.ayp_on_track_flag = 1 THEN ss.student_id END) AS bm_pass_count
            
            FROM   c_ayp_subject_student ss
            JOIN   (SELECT  tt.ayp_subject_id ,MAX(tty.school_year_id) AS yr_c FROM   c_ayp_test_type_year AS tty JOIN   c_ayp_subject tt ON tty.ayp_test_type_id = tt.ayp_test_type_id GROUP BY tt.ayp_subject_id) dt
                ON   dt.ayp_subject_id = ss.ayp_subject_id
                AND  dt.yr_c = ss.school_year_id
            JOIN   c_student st
                ON   st.student_id = ss.student_id
                AND  st.active_flag = 1
                AND  EXISTS   (  SELECT    *
                                    FROM   c_class_enrollment AS cle
                                    WHERE   cle.student_id = st.student_id)
            JOIN   c_student_year sty
                ON   sty.student_id = ss.student_id
            JOIN   c_school_year sy
                ON   sy.school_year_id = sty.school_year_id
                AND  sy.active_flag = 1
            JOIN   c_grade_level gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN   c_ayp_subject as sub
                ON   sub.ayp_subject_id = ss.ayp_subject_id
            JOIN   c_ayp_test_type_al as atta
                ON   atta.ayp_test_type_id = sub.ayp_test_type_id
            JOIN   c_ayp_achievement_level as aal
                ON   aal.al_id = atta.al_id
                AND  aal.al_id = ss.al_id
            JOIN   c_ayp_group ag
                ON   ag.ayp_group_code = 'ALL'
            LEFT JOIN   rpt_profile_leading_subject_stu AS rplss
                ON   rplss.ayp_subject_id = ss.ayp_subject_id
                AND  rplss.student_id = ss.student_id
            LEFT JOIN   c_color_ayp_benchmark AS cb
                ON   rplss.ayp_subject_id = cb.ayp_subject_id
                AND  ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            WHERE   ss.al_id IS NOT NULL 
            AND     ss.score_record_flag = 1
            GROUP BY sty.school_id, ss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id, ss.school_year_id
         ) AS dt
   ON   DUPLICATE KEY UPDATE 
   prof_count = dt.pass_count,
   ayp_group_count =  dt.group_count,
   bm_pass_count =    dt.bm_pass_count,
   last_user_id = 1234;
    
END;
//              
