/*
$Rev: 9375 $ 
$Author: randall.stanley $ 
$Date: 2010-10-07 08:57:01 -0400 (Thu, 07 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bm_summ_ayp_group_school_grade.sql $
$Id: etl_rpt_bm_summ_ayp_group_school_grade.sql 9375 2010-10-07 12:57:01Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_rpt_bm_summ_ayp_group_school_grade //

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_bm_summ_ayp_group_school_grade`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 9375 $ $Date: 2010-10-07 08:57:01 -0400 (Thu, 07 Oct 2010) $'
BEGIN
    
truncate table rpt_bm_summ_ayp_group_school_grade;

INSERT INTO rpt_bm_summ_ayp_group_school_grade (
    school_id
    , ayp_subject_id
    , ayp_group_id
    , grade_level_id
    , ayp_group_count
    , ayp_on_track_count
    , last_user_id
    , create_timestamp
    ) 

SELECT   dt.school_id
   ,dt.ayp_subject_id
   ,dt.ayp_group_id
   ,dt.grade_level_id
   ,dt.group_count
   ,dt.ayp_on_track_count
   ,1234
   ,current_timestamp

FROM     (

            SELECT   sty.school_id,
            rplss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            COUNT(st.student_id) AS group_count,
            COUNT(CASE WHEN cb.ayp_on_track_flag = 1 THEN st.student_id END) AS ayp_on_track_count
            
            FROM     rpt_profile_leading_subject_stu AS rplss
            JOIN     c_student AS st
                     ON    st.student_id = rplss.student_id
            JOIN    c_ethnicity AS e
                    ON      e.ethnicity_id = st.ethnicity_id
                    AND     e.ethnicity_code = 'a'
            JOIN    c_school_year AS sy
                    ON      sy.active_flag = 1
            JOIN     c_student_year AS sty
                     ON    sty.student_id = st.student_id
                     AND   sty.school_year_id = sy.school_year_id
            JOIN     c_grade_level AS gl
                     ON    gl.grade_level_id = sty.grade_level_id
            JOIN     c_ayp_group AS ag
                     ON    ag.ayp_group_code = 'ethnAsian'
            JOIN     c_color_ayp_benchmark AS cb
                     ON    rplss.ayp_subject_id = cb.ayp_subject_id
                     AND   ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            
            GROUP BY sty.school_id, rplss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id

            UNION ALL 
            
            SELECT   sty.school_id,
            rplss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            COUNT(st.student_id) AS group_count,
            COUNT(CASE WHEN cb.ayp_on_track_flag = 1 THEN st.student_id END) AS ayp_on_track_count
            
            FROM     rpt_profile_leading_subject_stu AS rplss
            JOIN     c_student AS st
                     ON    st.student_id = rplss.student_id
            JOIN    c_ethnicity AS e
                    ON      e.ethnicity_id = st.ethnicity_id
                    AND     e.ethnicity_code = 'b'
            JOIN    c_school_year AS sy
                    ON      sy.active_flag = 1
            JOIN     c_student_year AS sty
                     ON    sty.student_id = st.student_id
                     AND   sty.school_year_id = sy.school_year_id
            JOIN     c_grade_level AS gl
                     ON    gl.grade_level_id = sty.grade_level_id
            JOIN     c_ayp_group AS ag
                     ON    ag.ayp_group_code = 'ethnBlack'
            JOIN     c_color_ayp_benchmark AS cb
                     ON    rplss.ayp_subject_id = cb.ayp_subject_id
                     AND   ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            
            GROUP BY sty.school_id, rplss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id

            UNION ALL 
            
            SELECT   sty.school_id,
            rplss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            COUNT(st.student_id) AS group_count,
            COUNT(CASE WHEN cb.ayp_on_track_flag = 1 THEN st.student_id END) AS ayp_on_track_count
            
            FROM     rpt_profile_leading_subject_stu AS rplss
            JOIN     c_student AS st
                     ON    st.student_id = rplss.student_id
            JOIN    c_ethnicity AS e
                    ON      e.ethnicity_id = st.ethnicity_id
                    AND     e.ethnicity_code = 'p'
            JOIN    c_school_year AS sy
                    ON      sy.active_flag = 1
            JOIN     c_student_year AS sty
                     ON    sty.student_id = st.student_id
                     AND   sty.school_year_id = sy.school_year_id
            JOIN     c_grade_level AS gl
                     ON    gl.grade_level_id = sty.grade_level_id
            JOIN     c_ayp_group AS ag
                     ON    ag.ayp_group_code = 'ethnHawaiian'
            JOIN     c_color_ayp_benchmark AS cb
                     ON    rplss.ayp_subject_id = cb.ayp_subject_id
                     AND   ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            
            GROUP BY sty.school_id, rplss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id

            UNION ALL 
            
            SELECT   sty.school_id,
            rplss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            COUNT(st.student_id) AS group_count,
            COUNT(CASE WHEN cb.ayp_on_track_flag = 1 THEN st.student_id END) AS ayp_on_track_count
            
            FROM     rpt_profile_leading_subject_stu AS rplss
            JOIN     c_student AS st
                     ON    st.student_id = rplss.student_id
            JOIN    c_ethnicity AS e
                    ON      e.ethnicity_id = st.ethnicity_id
                    AND     e.ethnicity_code = 'h'
            JOIN    c_school_year AS sy
                    ON      sy.active_flag = 1
            JOIN     c_student_year AS sty
                     ON    sty.student_id = st.student_id
                     AND   sty.school_year_id = sy.school_year_id
            JOIN     c_grade_level AS gl
                     ON    gl.grade_level_id = sty.grade_level_id
            JOIN     c_ayp_group AS ag
                     ON    ag.ayp_group_code = 'ethnHispanic'
            JOIN     c_color_ayp_benchmark AS cb
                     ON    rplss.ayp_subject_id = cb.ayp_subject_id
                     AND   ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            
            GROUP BY sty.school_id, rplss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id

            UNION ALL 
            
            SELECT   sty.school_id,
            rplss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            COUNT(st.student_id) AS group_count,
            COUNT(CASE WHEN cb.ayp_on_track_flag = 1 THEN st.student_id END) AS ayp_on_track_count
            
            FROM     rpt_profile_leading_subject_stu AS rplss
            JOIN     c_student AS st
                     ON    st.student_id = rplss.student_id
            JOIN    c_ethnicity AS e
                    ON      e.ethnicity_id = st.ethnicity_id
                    AND     e.ethnicity_code = 'i'
            JOIN    c_school_year AS sy
                    ON      sy.active_flag = 1
            JOIN     c_student_year AS sty
                     ON    sty.student_id = st.student_id
                     AND   sty.school_year_id = sy.school_year_id
            JOIN     c_grade_level AS gl
                     ON    gl.grade_level_id = sty.grade_level_id
            JOIN     c_ayp_group AS ag
                     ON    ag.ayp_group_code = 'ethnIndian'
            JOIN     c_color_ayp_benchmark AS cb
                     ON    rplss.ayp_subject_id = cb.ayp_subject_id
                     AND   ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            
            GROUP BY sty.school_id, rplss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id

            UNION ALL 
            
            SELECT   sty.school_id,
            rplss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            COUNT(st.student_id) AS group_count,
            COUNT(CASE WHEN cb.ayp_on_track_flag = 1 THEN st.student_id END) AS ayp_on_track_count
            
            FROM     rpt_profile_leading_subject_stu AS rplss
            JOIN     c_student AS st
                     ON    st.student_id = rplss.student_id
            JOIN    c_ethnicity AS e
                    ON      e.ethnicity_id = st.ethnicity_id
                    AND     e.ethnicity_code = 'm'
            JOIN    c_school_year AS sy
                    ON      sy.active_flag = 1
            JOIN     c_student_year AS sty
                     ON    sty.student_id = st.student_id
                     AND   sty.school_year_id = sy.school_year_id
            JOIN     c_grade_level AS gl
                     ON    gl.grade_level_id = sty.grade_level_id
            JOIN     c_ayp_group AS ag
                     ON    ag.ayp_group_code = 'ethnMulti'
            JOIN     c_color_ayp_benchmark AS cb
                     ON    rplss.ayp_subject_id = cb.ayp_subject_id
                     AND   ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            
            GROUP BY sty.school_id, rplss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id

            UNION ALL 
            
            SELECT   sty.school_id,
            rplss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            COUNT(st.student_id) AS group_count,
            COUNT(CASE WHEN cb.ayp_on_track_flag = 1 THEN st.student_id END) AS ayp_on_track_count
            
            FROM     rpt_profile_leading_subject_stu AS rplss
            JOIN     c_student AS st
                     ON    st.student_id = rplss.student_id
            JOIN    c_ethnicity AS e
                    ON      e.ethnicity_id = st.ethnicity_id
                    AND     e.ethnicity_code = 'w'
            JOIN    c_school_year AS sy
                    ON      sy.active_flag = 1
            JOIN     c_student_year AS sty
                     ON    sty.student_id = st.student_id
                     AND   sty.school_year_id = sy.school_year_id
            JOIN     c_grade_level AS gl
                     ON    gl.grade_level_id = sty.grade_level_id
            JOIN     c_ayp_group AS ag
                     ON    ag.ayp_group_code = 'ethnWhite'
            JOIN     c_color_ayp_benchmark AS cb
                     ON    rplss.ayp_subject_id = cb.ayp_subject_id
                     AND   ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            
            GROUP BY sty.school_id, rplss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id
         ) AS dt
ON DUPLICATE KEY UPDATE 
   ayp_group_count =  dt.group_count,
   ayp_on_track_count =    dt.ayp_on_track_count,
   last_user_id = 1234;


INSERT INTO rpt_bm_summ_ayp_group_school_grade (
    school_id
    , ayp_subject_id
    , ayp_group_id
    , grade_level_id
    , ayp_group_count
    , ayp_on_track_count
    , last_user_id
    , create_timestamp
    ) 

SELECT   dt.school_id
   ,dt.ayp_subject_id
   ,dt.ayp_group_id
   ,dt.grade_level_id
   ,dt.group_count
   ,dt.ayp_on_track_count
   ,1234
   ,current_timestamp

FROM     (

            SELECT   sty.school_id,
            rplss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            COUNT(st.student_id) AS group_count,
            COUNT(CASE WHEN cb.ayp_on_track_flag = 1 THEN st.student_id END) AS ayp_on_track_count
            
            FROM     rpt_profile_leading_subject_stu AS rplss
            JOIN     c_student AS st
                     ON    st.student_id = rplss.student_id
            JOIN    c_school_year AS sy
                    ON      sy.active_flag = 1
            JOIN     c_student_year AS sty
                     ON    sty.student_id = st.student_id
                     AND   sty.school_year_id = sy.school_year_id
                     AND   sty.swd_flag = 1
            JOIN     c_grade_level AS gl
                     ON    gl.grade_level_id = sty.grade_level_id
            JOIN     c_ayp_group AS ag
                     ON    ag.ayp_group_code = 'swd'
            JOIN     c_color_ayp_benchmark AS cb
                     ON    rplss.ayp_subject_id = cb.ayp_subject_id
                     AND   ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            
            GROUP BY sty.school_id, rplss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id

            UNION ALL 

            SELECT   sty.school_id,
            rplss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            COUNT(st.student_id) AS group_count,
            COUNT(CASE WHEN cb.ayp_on_track_flag = 1 THEN st.student_id END) AS ayp_on_track_count
            
            FROM     rpt_profile_leading_subject_stu AS rplss
            JOIN     c_student AS st
                     ON    st.student_id = rplss.student_id
            JOIN    c_school_year AS sy
                    ON      sy.active_flag = 1
            JOIN     c_student_year AS sty
                     ON    sty.student_id = st.student_id
                     AND   sty.school_year_id = sy.school_year_id
                     AND   sty.lep_flag = 1
            JOIN     c_grade_level AS gl
                     ON    gl.grade_level_id = sty.grade_level_id
            JOIN     c_ayp_group AS ag
                     ON    ag.ayp_group_code = 'lep'
            JOIN     c_color_ayp_benchmark AS cb
                     ON    rplss.ayp_subject_id = cb.ayp_subject_id
                     AND   ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            
            GROUP BY sty.school_id, rplss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id

            UNION ALL 

            SELECT   sty.school_id,
            rplss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            COUNT(st.student_id) AS group_count,
            COUNT(CASE WHEN cb.ayp_on_track_flag = 1 THEN st.student_id END) AS ayp_on_track_count
            
            FROM     rpt_profile_leading_subject_stu AS rplss
            JOIN     c_student AS st
                     ON    st.student_id = rplss.student_id
            JOIN    c_school_year AS sy
                    ON      sy.active_flag = 1
            JOIN     c_student_year AS sty
                     ON    sty.student_id = st.student_id
                     AND   sty.school_year_id = sy.school_year_id
                     AND   sty.econ_disadv_flag = 1
            JOIN     c_grade_level AS gl
                     ON    gl.grade_level_id = sty.grade_level_id
            JOIN     c_ayp_group AS ag
                     ON    ag.ayp_group_code = 'econDisadv'
            JOIN     c_color_ayp_benchmark AS cb
                     ON    rplss.ayp_subject_id = cb.ayp_subject_id
                     AND   ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            
            GROUP BY sty.school_id, rplss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id

            UNION ALL 

            SELECT   sty.school_id,
            rplss.ayp_subject_id,
            ag.ayp_group_id,
            gl.grade_level_id,
            COUNT(st.student_id) AS group_count,
            COUNT(CASE WHEN cb.ayp_on_track_flag = 1 THEN st.student_id END) AS ayp_on_track_count
            
            FROM     rpt_profile_leading_subject_stu AS rplss
            JOIN     c_student AS st
                     ON    st.student_id = rplss.student_id
            JOIN    c_school_year AS sy
                    ON      sy.active_flag = 1
            JOIN     c_student_year AS sty
                     ON    sty.student_id = st.student_id
                     AND   sty.school_year_id = sy.school_year_id
            JOIN     c_grade_level AS gl
                     ON    gl.grade_level_id = sty.grade_level_id
            JOIN     c_ayp_group AS ag
                     ON    ag.ayp_group_code = 'all'
            JOIN     c_color_ayp_benchmark AS cb
                     ON    rplss.ayp_subject_id = cb.ayp_subject_id
                     AND   ROUND(COALESCE(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
            
            GROUP BY sty.school_id, rplss.ayp_subject_id, ag.ayp_group_id, gl.grade_level_id

         ) AS dt
ON DUPLICATE KEY UPDATE 
   ayp_group_count =  dt.group_count,
   ayp_on_track_count =    dt.ayp_on_track_count,
   last_user_id = 1234;


END;
//
