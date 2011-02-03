/*
$Rev$ 
$Author$ 
$Date$
$HeadURL$
$Id$ 
 */

drop procedure if exists etl_hst_post_load_update_grad_ga//

create definer=`dbadmin`@`localhost` procedure etl_hst_post_load_update_grad_ga()
contains sql
sql security invoker
comment '$Rev$ $Date$'


proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    # GA Graduation Logic
    #   If a student passes all five graduation subjects (4 GHSGT and 1 GHSWT), then they are 'Meets Requirements'
    #   else they are 'Does Not Meet Requirements'
    
    # We can't use c_ayp_subject.grad_report_flag b/c EOCT is set to 1 and included in the report, but has no impact on grad reqmts.
    # Additionally, we must use c_ayp_test_type_al to determine passing grade.

    update c_student as s
    left join
        (
          select  stu.student_id
                  , count( distinct dt.ayp_subject_id) as subs_passed
          from    c_student stu
          join (
                select  ss.student_id
                        ,sub.ayp_subject_id
                from    c_ayp_subject_student ss
                join    c_ayp_subject sub
                  on          ss.ayp_subject_id = sub.ayp_subject_id
                join    c_ayp_test_type tt
                  on          sub.ayp_test_type_id = tt.ayp_test_type_id
                 and          tt.moniker in ('GHSGT', 'GHSWT') 
                join    c_ayp_test_type_al ttal
                  on          ttal.ayp_test_type_id = tt.ayp_test_type_id
                and           ss.al_id = ttal.al_id
                where   ttal.on_grade_level_code in ('o','a')
                group by ss.student_id, sub.ayp_subject_id
              ) dt on         stu.student_id = dt.student_id
          group by stu.student_id
        ) as dt_passing_students
        on      dt_passing_students.student_id = s.student_id
    set s.grad_reqs_met_flag = case when dt_passing_students.subs_passed >= 5 then 1 else 0 end
    ;
    
    

end proc;
//
