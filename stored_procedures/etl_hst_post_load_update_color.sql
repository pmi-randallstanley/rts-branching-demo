
/*
$Rev: 8022 $ 
$Author: randall.stanley $ 
$Date: 2009-12-14 15:27:20 -0500 (Mon, 14 Dec 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_hst_post_load_update_color.sql $
$Id: etl_hst_post_load_update_color.sql 8022 2009-12-14 20:27:20Z randall.stanley $ 
 */


drop procedure if exists etl_hst_post_load_update_color//

CREATE DEFINER=`dbadmin`@`localhost` PROCEDURE `etl_hst_post_load_update_color`()
    contains sql
    SQL SECURITY INVOKER
    COMMENT '$Rev: 8022 $ $Date: 2009-12-14 15:27:20 -0500 (Mon, 14 Dec 2009)'
proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    SELECT  count(case when st.state_abbr = 'fl' then st.state_abbr end)
            ,count(case when st.state_abbr = 'ga' then st.state_abbr end)
            ,count(case when st.state_abbr = 'mn' then st.state_abbr end)
            ,count(case when st.state_abbr = 'ky' then st.state_abbr end)
    INTO    @is_fl_client
            ,@is_ga_client
            ,@is_mn_client
            ,@is_ky_client
    FROM    pmi_admin.pmi_state AS st
    WHERE   st.state_id = @state_id
    AND     st.state_abbr in ('fl','ga','mn','ky');

    IF @is_fl_client > 0 then
        
        UPDATE c_ayp_subject_student AS ss
        JOIN    c_ayp_subject AS sub
                ON      ss.ayp_subject_id = sub.ayp_subject_id
                AND     sub.ayp_subject_code not in ('fcatScience','fcatWriting','fleocAlgebra1','fleocGeometry','fleocBiology')
        JOIN    c_student_year AS sty
                ON      sty.student_id = ss.student_id
                AND     sty.school_year_id = ss.school_year_id
        JOIN    c_grade_level AS gl
                ON      gl.grade_level_id = sty.grade_level_id
        LEFT  JOIN    c_color_ayp_subject AS csub
                ON      csub.ayp_subject_id = ss.ayp_subject_id
                AND     ss.school_year_id between csub.begin_year AND csub.end_year
                AND     gl.grade_sequence between csub.begin_grade_sequence AND csub.end_grade_sequence
                AND     round(ss.alt_ayp_score,0) between csub.min_score AND csub.max_score
        LEFT  JOIN    pmi_color AS clr
                ON      clr.color_id = csub.color_id
        SET     ss.alt_ayp_score_color = case when ss.alt_ayp_score is NULL then NULL else coalesce(clr.moniker, 'white') end
                ,ss.ayp_score_color = case when ss.ayp_score is NULL then NULL else coalesce(clr.moniker, 'white') end
        ;

        
        UPDATE c_ayp_subject_student AS ss
        JOIN    c_ayp_subject AS sub
                ON      ss.ayp_subject_id = sub.ayp_subject_id
                AND     sub.ayp_subject_code in ('fcatScience','fcatWriting','fleocAlgebra1','fleocGeometry','fleocBiology')
        JOIN    c_student_year AS sty
                ON      sty.student_id = ss.student_id
                AND     sty.school_year_id = ss.school_year_id
        JOIN    c_grade_level AS gl
                ON      gl.grade_level_id = sty.grade_level_id
        LEFT  JOIN    c_color_ayp_subject AS csub
                ON      csub.ayp_subject_id = ss.ayp_subject_id
                AND     ss.school_year_id between csub.begin_year AND csub.end_year
                AND     gl.grade_sequence between csub.begin_grade_sequence AND csub.end_grade_sequence
                AND     round(ss.ayp_score,1) between csub.min_score AND csub.max_score
        LEFT  JOIN    pmi_color AS clr
                ON      clr.color_id = csub.color_id
        SET     ss.alt_ayp_score_color = case when ss.alt_ayp_score is NULL then NULL else coalesce(clr.moniker, 'white') end
                ,ss.ayp_score_color = case when ss.ayp_score is NULL then NULL else coalesce(clr.moniker, 'white') end
        ;


        UPDATE c_ayp_subject_student AS ss
        JOIN    c_ayp_subject AS sub
                ON      ss.ayp_subject_id = sub.ayp_subject_id
                AND     sub.ayp_subject_code in ('fcatMath', 'fcatReading')
        JOIN    c_student_year AS sty
                ON      sty.student_id = ss.student_id
                AND     sty.school_year_id = ss.school_year_id
        JOIN    c_grade_level AS gl
                ON      gl.grade_level_id = sty.grade_level_id
        LEFT  JOIN    c_color_ayp_subject AS csub
                ON      csub.ayp_subject_id = ss.ayp_subject_id
                AND     case when ss.alt_ayp_score > 500 then ss.school_year_id -1 between csub.begin_year AND csub.end_year
                            when ss.alt_ayp_score <= 500 then ss.school_year_id  between csub.begin_year AND csub.end_year
                        end 
                AND     gl.grade_sequence between csub.begin_grade_sequence AND csub.end_grade_sequence
                AND     round(ss.alt_ayp_score,0) between csub.min_score AND csub.max_score
        LEFT  JOIN    pmi_color AS clr
                ON      clr.color_id = csub.color_id
        SET     ss.alt_ayp_score_color = case when ss.alt_ayp_score is NULL then NULL else coalesce(clr.moniker, 'white') end
                ,ss.ayp_score_color = case when ss.ayp_score is NULL then NULL else coalesce(clr.moniker, 'white') end
        WHERE ss.school_year_id = 2012
        ;
        
    ELSEIF @is_ga_client > 0 THEN 
        
        UPDATE c_ayp_subject_student AS ss
        JOIN    c_ayp_subject AS sub
                ON      ss.ayp_subject_id = sub.ayp_subject_id
                AND     sub.ayp_subject_code like ('eoct%')
        JOIN    c_student_year AS sty
                ON      sty.student_id = ss.student_id
                AND     sty.school_year_id = ss.school_year_id
        JOIN    c_grade_level AS gl
                ON      gl.grade_level_id = sty.grade_level_id
        LEFT  JOIN    c_color_ayp_subject AS csub
                ON      csub.ayp_subject_id = ss.ayp_subject_id
                AND     ss.school_year_id between csub.begin_year AND csub.end_year
                AND     gl.grade_sequence between csub.begin_grade_sequence AND csub.end_grade_sequence
                AND     round(ss.alt_ayp_score,0) between csub.min_score AND csub.max_score
        LEFT  JOIN    pmi_color AS clr
                ON      clr.color_id = csub.color_id
        SET     ss.alt_ayp_score_color = coalesce(clr.moniker, 'white')
                ,ss.ayp_score_color = coalesce(clr.moniker, 'white')
        ;

        
        UPDATE c_ayp_subject_student AS ss
        JOIN    c_ayp_subject AS sub
                ON      ss.ayp_subject_id = sub.ayp_subject_id
                AND     sub.ayp_subject_code not like ('eoct%')
        JOIN    c_student_year AS sty
                ON      sty.student_id = ss.student_id
                AND     sty.school_year_id = ss.school_year_id
        JOIN    c_grade_level AS gl
                ON      gl.grade_level_id = sty.grade_level_id
        LEFT  JOIN    c_color_ayp_subject AS csub
                ON      csub.ayp_subject_id = ss.ayp_subject_id
                AND     ss.school_year_id between csub.begin_year AND csub.end_year
                AND     gl.grade_sequence between csub.begin_grade_sequence AND csub.end_grade_sequence
                AND     round(ss.ayp_score,0) between csub.min_score AND csub.max_score
        LEFT  JOIN    pmi_color AS clr
                ON      clr.color_id = csub.color_id
        SET     ss.ayp_score_color = coalesce(clr.moniker, 'white')
        ;
        
    ELSEIF @is_mn_client > 0 THEN 
        
        UPDATE c_ayp_subject_student AS ss
        JOIN    c_ayp_subject AS sub
                ON      ss.ayp_subject_id = sub.ayp_subject_id
                AND     sub.ayp_subject_code like ('mnNWEA%')
        JOIN    c_student_year AS sty
                ON      sty.student_id = ss.student_id
                AND     sty.school_year_id = ss.school_year_id
        JOIN    c_grade_level AS gl
                ON      gl.grade_level_id = sty.grade_level_id
        LEFT  JOIN    c_color_ayp_subject AS csub
                ON      csub.ayp_subject_id = ss.ayp_subject_id
                AND     ss.school_year_id between csub.begin_year AND csub.end_year
                AND     gl.grade_sequence between csub.begin_grade_sequence AND csub.end_grade_sequence
                AND     round(ss.alt_ayp_score,0) between csub.min_score AND csub.max_score
        LEFT  JOIN    pmi_color AS clr
                ON      clr.color_id = csub.color_id
        SET     ss.alt_ayp_score_color = coalesce(clr.moniker, 'white')
                ,ss.ayp_score_color = coalesce(clr.moniker, 'white')
        ;

        
        UPDATE c_ayp_subject_student AS ss
        JOIN    c_ayp_subject AS sub
                ON      ss.ayp_subject_id = sub.ayp_subject_id
                AND     sub.ayp_subject_code not like ('mnNWEA%')
        JOIN    c_student_year AS sty
                ON      sty.student_id = ss.student_id
                AND     sty.school_year_id = ss.school_year_id
        JOIN    c_grade_level AS gl
                ON      gl.grade_level_id = sty.grade_level_id
        LEFT  JOIN    c_color_ayp_subject AS csub
                ON      csub.ayp_subject_id = ss.ayp_subject_id
                AND     ss.school_year_id between csub.begin_year AND csub.end_year
                AND     gl.grade_sequence between csub.begin_grade_sequence AND csub.end_grade_sequence
                AND     round(ss.ayp_score,0) between csub.min_score AND csub.max_score
        LEFT  JOIN    pmi_color AS clr
                ON      clr.color_id = csub.color_id
        SET     ss.ayp_score_color = coalesce(clr.moniker, 'white')
        ;
        
    ELSEIF @is_ky_client > 0 THEN 
        
        UPDATE c_ayp_subject_student AS ss
        JOIN    c_ayp_subject AS sub
                ON      ss.ayp_subject_id = sub.ayp_subject_id
                AND     sub.ayp_subject_code like ('kyNWEA%')
        JOIN    c_student_year AS sty
                ON      sty.student_id = ss.student_id
                AND     sty.school_year_id = ss.school_year_id
        JOIN    c_grade_level AS gl
                ON      gl.grade_level_id = sty.grade_level_id
        LEFT  JOIN    c_color_ayp_subject AS csub
                ON      csub.ayp_subject_id = ss.ayp_subject_id
                AND     ss.school_year_id between csub.begin_year AND csub.end_year
                AND     gl.grade_sequence between csub.begin_grade_sequence AND csub.end_grade_sequence
                AND     round(ss.alt_ayp_score,0) between csub.min_score AND csub.max_score
        LEFT  JOIN    pmi_color AS clr
                ON      clr.color_id = csub.color_id
        SET     ss.alt_ayp_score_color = coalesce(clr.moniker, 'white')
                ,ss.ayp_score_color = coalesce(clr.moniker, 'white')
        ;

        
        UPDATE c_ayp_subject_student AS ss
        JOIN    c_ayp_subject AS sub
                ON      ss.ayp_subject_id = sub.ayp_subject_id
                AND     sub.ayp_subject_code not like ('kyNWEA%')
        JOIN    c_student_year AS sty
                ON      sty.student_id = ss.student_id
                AND     sty.school_year_id = ss.school_year_id
        JOIN    c_grade_level AS gl
                ON      gl.grade_level_id = sty.grade_level_id
        LEFT  JOIN    c_color_ayp_subject AS csub
                ON      csub.ayp_subject_id = ss.ayp_subject_id
                AND     ss.school_year_id between csub.begin_year AND csub.end_year
                AND     gl.grade_sequence between csub.begin_grade_sequence AND csub.end_grade_sequence
                AND     round(ss.ayp_score,0) between csub.min_score AND csub.max_score
        LEFT  JOIN    pmi_color AS clr
                ON      clr.color_id = csub.color_id
        SET     ss.ayp_score_color = coalesce(clr.moniker, 'white')
        ;
    
    ELSE
        
        UPDATE c_ayp_subject_student AS ss
        JOIN    c_student_year AS sty
                ON      sty.student_id = ss.student_id
                AND     sty.school_year_id = ss.school_year_id
        JOIN    c_grade_level AS gl
                ON      gl.grade_level_id = sty.grade_level_id
        LEFT  JOIN    c_color_ayp_subject AS csub
                ON      csub.ayp_subject_id = ss.ayp_subject_id
                AND     ss.school_year_id between csub.begin_year AND csub.end_year
                AND     gl.grade_sequence between csub.begin_grade_sequence AND csub.end_grade_sequence
                AND     round(ss.ayp_score,0) between csub.min_score AND csub.max_score
        LEFT  JOIN    pmi_color AS clr
                ON      clr.color_id = csub.color_id
        SET     ss.ayp_score_color = coalesce(clr.moniker, 'white')
        ;
    
    END IF;

    
    UPDATE  c_ayp_strand_student AS ss
    JOIN    c_student_year AS sty
            ON      sty.student_id = ss.student_id
            AND     sty.school_year_id = ss.school_year_id
    JOIN    c_grade_level AS gl
            ON      gl.grade_level_id = sty.grade_level_id
    LEFT  JOIN   c_color_ayp_strand AS cstr
            ON      cstr.ayp_subject_id = ss.ayp_subject_id
            AND     cstr.ayp_strand_id = ss.ayp_strand_id
            AND     ss.school_year_id between cstr.begin_year AND cstr.end_year
            AND     gl.grade_sequence between cstr.begin_grade_sequence AND cstr.end_grade_sequence
            AND     round(ss.ayp_score,1) between cstr.min_score AND cstr.max_score
    LEFT  JOIN    pmi_color AS clr
            ON      clr.color_id = cstr.color_id
    SET     ss.ayp_score_color = coalesce(clr.moniker, 'white')
    ;
    
    
    UPDATE  c_student st 
    JOIN    c_ayp_subject_student cass
            ON    st.student_id = cass.student_id
    JOIN    c_ayp_subject s
            ON    cass.ayp_subject_id = s.ayp_subject_id
            AND   s.grad_report_flag = 1
    SET st.grad_eligible_flag = 1
    ;
    
    
    INSERT  c_ayp_year_class (class_id, school_year_id, last_user_id, create_timestamp)
    SELECT  cle.class_id, ss.school_year_id, 1234, now()
    FROM    c_ayp_subject_student AS ss
    JOIN    c_class_enrollment AS cle
            ON      ss.student_id = cle.student_id
    GROUP BY cle.class_id, ss.school_year_id
    ON duplicate KEY UPDATE last_user_id = 1234
    ;

end proc ;
//
