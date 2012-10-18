DROP PROCEDURE IF EXISTS etl_c_ayp_subject_student_update_al //

CREATE definer=`dbadmin`@`localhost` procedure etl_c_ayp_subject_student_update_al()
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
CONTAINS SQL
SQL SECURITY INVOKER

/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_c_ayp_subject_student_update_al.sql $
$Id: etl_c_ayp_subject_student_update_al.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

BEGIN

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    SELECT  count(case when st.state_abbr = 'fl' then st.state_abbr end)
    INTO    @is_fl_client
    FROM    pmi_admin.pmi_state AS st
    WHERE   st.state_id = @state_id
    AND     st.state_abbr in ('fl');

    IF @is_fl_client > 0 then
    
        update  c_ayp_subject_student as subst
        join    c_student_year as sty
                on    sty.student_id = subst.student_id
                and   sty.school_year_id = subst.school_year_id
        join    c_grade_level as gl
                on    gl.grade_level_id = sty.grade_level_id
        join    c_color_ayp_subject as csub
                on    csub.ayp_subject_id = subst.ayp_subject_id
                and   gl.grade_sequence between csub.begin_grade_sequence and csub.end_grade_sequence
                and   subst.school_year_id between csub.begin_year and csub.end_year
                and   coalesce(subst.alt_ayp_score, subst.ayp_score) between csub.min_score and csub.max_score
        join    c_ayp_subject as sub
                on    sub.ayp_subject_id = csub.ayp_subject_id
                and   sub.ayp_subject_code in ('fcatScience','fcatWriting','fleocAlgebra1','fleocGeometry','fleocBiology','flpertMath','flpertReading','flpertWriting')
        join    c_ayp_test_type_al as atta
                on    atta.ayp_test_type_id = sub.ayp_test_type_id
                and   atta.color_id = csub.color_id
        set     subst.al_id = atta.al_id
        ;

        update  c_ayp_subject_student as subst
        join    c_student_year as sty
                on    sty.student_id = subst.student_id
                and   sty.school_year_id = subst.school_year_id
        join    c_grade_level as gl
                on    gl.grade_level_id = sty.grade_level_id
        join    c_color_ayp_subject as csub
                on    csub.ayp_subject_id = subst.ayp_subject_id
                and   gl.grade_sequence between csub.begin_grade_sequence and csub.end_grade_sequence
                and   subst.school_year_id between csub.begin_year and csub.end_year
                and   coalesce(subst.alt_ayp_score, subst.ayp_score) between csub.min_score and csub.max_score
        join    c_ayp_subject as sub
                on    sub.ayp_subject_id = csub.ayp_subject_id
                and   sub.ayp_subject_code in ('fcatMath', 'fcatReading')
        join    c_ayp_test_type_al as atta
                on    atta.ayp_test_type_id = sub.ayp_test_type_id
                and   atta.color_id = csub.color_id
        set     subst.al_id = atta.al_id
        WHERE subst.school_year_id < 2011
        ;

        update  c_ayp_subject_student as subst
        join    c_student_year as sty
                on    sty.student_id = subst.student_id
                and   sty.school_year_id = subst.school_year_id
        join    c_grade_level as gl
                on    gl.grade_level_id = sty.grade_level_id
        join    c_color_ayp_subject AS csub
                ON      csub.ayp_subject_id = subst.ayp_subject_id
                AND     case when subst.alt_ayp_score > 500 then 2010 between csub.begin_year AND csub.end_year
                            when subst.alt_ayp_score <= 500 then subst.school_year_id  between csub.begin_year AND csub.end_year
                        end 
                AND     gl.grade_sequence between csub.begin_grade_sequence AND csub.end_grade_sequence
                AND     subst.alt_ayp_score between csub.min_score AND csub.max_score   ### Only check for alt ayp score b/c ayp score now contains old dev score
        join    c_ayp_subject as sub
                on    sub.ayp_subject_id = csub.ayp_subject_id
                and   sub.ayp_subject_code in ('fcatMath', 'fcatReading')
        join    c_ayp_test_type_al as atta
                on    atta.ayp_test_type_id = sub.ayp_test_type_id
                and   atta.color_id = csub.color_id
        set     subst.al_id = atta.al_id
        WHERE subst.school_year_id >= 2011
        ;
        
        update  c_ayp_subject_student as subst
        join    c_student_year as sty
                on    sty.student_id = subst.student_id
                and   sty.school_year_id = subst.school_year_id
        join    c_grade_level as gl
                on    gl.grade_level_id = sty.grade_level_id
        join    c_color_ayp_subject as csub
                on    csub.ayp_subject_id = subst.ayp_subject_id
                and   gl.grade_sequence between csub.begin_grade_sequence and csub.end_grade_sequence
                and   subst.school_year_id between csub.begin_year and csub.end_year
                and   coalesce(subst.alt_ayp_score, subst.ayp_score) between csub.min_score and csub.max_score
        join    c_ayp_subject as sub
                on    sub.ayp_subject_id = csub.ayp_subject_id
                and   sub.ayp_subject_code like ('faa%')
        join    c_ayp_test_type_al as atta
                on    atta.ayp_test_type_id = sub.ayp_test_type_id
                and   atta.color_id = csub.color_id
        set     subst.al_id = atta.al_id
        ;
    
    ELSE
    
        update  c_ayp_subject_student as subst
        join    c_student_year as sty
                on    sty.student_id = subst.student_id
                and   sty.school_year_id = subst.school_year_id
        join    c_grade_level as gl
                on    gl.grade_level_id = sty.grade_level_id
        join    c_color_ayp_subject as csub
                on    csub.ayp_subject_id = subst.ayp_subject_id
                and   gl.grade_sequence between csub.begin_grade_sequence and csub.end_grade_sequence
                and   subst.school_year_id between csub.begin_year and csub.end_year
                and   coalesce(subst.alt_ayp_score, subst.ayp_score) between csub.min_score and csub.max_score
        join    c_ayp_subject as sub
                on    sub.ayp_subject_id = csub.ayp_subject_id
        join    c_ayp_test_type_al as atta
                on    atta.ayp_test_type_id = sub.ayp_test_type_id
                and   atta.color_id = csub.color_id
        set     subst.al_id = atta.al_id
        ; 
    END IF;
END;
//
