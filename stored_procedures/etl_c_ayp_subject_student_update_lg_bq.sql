/*
$Rev: 7410 $ 
$Author: randall.stanley $ 
$Date: 2009-07-20 11:04:59 -0400 (Mon, 20 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_c_ayp_subject_student_update_lg_bq.sql $
$Id: etl_c_ayp_subject_student_update_lg_bq.sql 7410 2009-07-20 15:04:59Z randall.stanley $ 
 */

drop procedure if exists etl_c_ayp_subject_student_update_lg_bq//

create definer=`dbadmin`@`localhost` procedure etl_c_ayp_subject_student_update_lg_bq (
    p_school_year_id int(10)
)
contains sql
sql security invoker
comment '$Rev: 7410 $ $Date: 2009-07-20 11:04:59 -0400 (Mon, 20 Jul 2009) $'


proc: begin 

    declare v_fcat_math_id      int(11) default '0';
    declare v_fcat_read_id      int(11) default '0';

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    select  count(*)
    into    @is_fl_client
    from    pmi_admin.pmi_state as st
    where   st.state_id = @state_id
    and     st.state_abbr = 'fl'
    ;

    if @is_fl_client > 0 then
    
        select  max(case when ayp_subject_code = 'fcatMath' then ayp_subject_id end)
                ,max(case when ayp_subject_code = 'fcatReading' then ayp_subject_id end)
        into    v_fcat_math_id
                ,v_fcat_read_id
        from    c_ayp_subject
        where   ayp_subject_code in ('fcatMath','fcatReading')
        ;
        
        # reset flags for invoked year
        update  c_ayp_subject_student as ss
        set     ss.lrn_gain_flag = 0
                ,ss.bottom_quartile_flag = 0
        where   ss.school_year_id = p_school_year_id
        ;

        # update learning gain, bottom quartile for Math
        update  c_ayp_subject_student as ss
        join    l_school_grade_stu_results as sr
                on      ss.student_id = sr.student_id
                and     ss.school_year_id = sr.school_year_id
        set     ss.lrn_gain_flag = coalesce(sr.math_lg_flag, 0)
                ,ss.bottom_quartile_flag = coalesce(sr.math_low_quartile_flag, 0)
        where   ss.ayp_subject_id = v_fcat_math_id
        and     ss.school_year_id = p_school_year_id
        and     ss.score_record_flag = 1
        ;

        # update learning gain, bottom quartile for Reading
        update  c_ayp_subject_student as ss
        join    l_school_grade_stu_results as sr
                on      ss.student_id = sr.student_id
                and     ss.school_year_id = sr.school_year_id
        set     ss.lrn_gain_flag = coalesce(sr.reading_lg_flag, 0)
                ,ss.bottom_quartile_flag = coalesce(sr.reading_low_quartile_flag, 0)
        where   ss.ayp_subject_id = v_fcat_read_id
        and     ss.school_year_id = p_school_year_id
        and     ss.score_record_flag = 1
        ;
        
    end if;

end proc;
//
