/*
$Rev: 8019 $ 
$Author: randall.stanley $ 
$Date: 2009-12-14 15:18:55 -0500 (Mon, 14 Dec 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_c_ayp_sub_stu_upd_sor_first.sql $
$Id: etl_c_ayp_sub_stu_upd_sor_first.sql 8019 2009-12-14 20:18:55Z randall.stanley $ 
 */

drop procedure if exists etl_c_ayp_sub_stu_upd_sor_first//

create definer=`dbadmin`@`localhost` procedure etl_c_ayp_sub_stu_upd_sor_first(p_ayp_subject_id int(11))
contains sql
sql security invoker
comment '$Rev: 8019 $ $Date: 2009-12-14 15:18:55 -0500 (Mon, 14 Dec 2009) $'


proc: begin 

    # reset score_record_flag for this subject
    update  c_ayp_subject_student
    set     score_record_flag = 0
    where   ayp_subject_id = p_ayp_subject_id
    ;

    # set score_record_flag to the first
    # score per acadmic month order for the year
    update  c_ayp_subject_student as ss
    join    c_school_year_month as sym
            on      ss.month_id = sym.month_id
    join    (
                select  ss2.student_id
                    ,ss2.ayp_subject_id
                    ,ss2.school_year_id
                    ,min(sym.academic_year_order) as acad_year_order
                    
                from    c_ayp_subject_student as ss2
                join    c_school_year_month as sym
                        on      ss2.month_id = sym.month_id
                where   ss2.ayp_subject_id = p_ayp_subject_id
                group by ss2.student_id
                    ,ss2.ayp_subject_id
                    ,ss2.school_year_id
            ) as dt
            on      ss.student_id = dt.student_id
            and     ss.ayp_subject_id = dt.ayp_subject_id
            and     ss.school_year_id = dt.school_year_id
            and     sym.academic_year_order = dt.acad_year_order
    set     ss.score_record_flag = 1
    where   ss.ayp_subject_id = p_ayp_subject_id
    ;

end proc;
//
