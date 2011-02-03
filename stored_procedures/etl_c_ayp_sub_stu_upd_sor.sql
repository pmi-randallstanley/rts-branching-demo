/*
$Rev: 9335 $ 
$Author: randall.stanley $ 
$Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_c_ayp_sub_stu_upd_sor.sql $
$Id: etl_c_ayp_sub_stu_upd_sor.sql 9335 2010-10-03 18:10:23Z randall.stanley $ 
 */

drop procedure if exists etl_c_ayp_sub_stu_upd_sor//

create definer=`dbadmin`@`localhost` procedure etl_c_ayp_sub_stu_upd_sor(p_test_type_name varchar(50))
contains sql
sql security invoker
comment '$Rev: 9335 $ $Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $'


proc: begin 

    declare no_more_rows            boolean; 
    declare v_ayp_subject_id        int(11) default '0';
    declare v_sor_method_code       char(1);

           
    declare cur_1 cursor for 
        select  sub.ayp_subject_id
            ,score_record_method_code
            
        from    c_ayp_subject as sub
        join    c_ayp_test_type as tt
                on      sub.ayp_test_type_id = tt.ayp_test_type_id
        where   tt.moniker = p_test_type_name
        and exists  (   select  *
                        from    c_ayp_subject_student as ss
                        where   ss.ayp_subject_id = sub.ayp_subject_id
                    )
        ;
    
    declare continue handler for not found 
    set no_more_rows = true;

    # Populate table - looping by subject
    open cur_1;
    loop_cur_1: loop
    
        fetch  cur_1 
        into   v_ayp_subject_id, v_sor_method_code;
               
        if no_more_rows then
            close cur_1;
            leave loop_cur_1;
        end if;

        # Expected score of record methods are:
        # b = Best
        # f = First
        # l = last
        if v_sor_method_code = 'b' then
        
            call etl_c_ayp_sub_stu_upd_sor_best(v_ayp_subject_id);

        elseif  v_sor_method_code = 'f' then 
            call etl_c_ayp_sub_stu_upd_sor_first(v_ayp_subject_id);

        elseif  v_sor_method_code = 'l' then 
            call etl_c_ayp_sub_stu_upd_sor_last(v_ayp_subject_id);

        end if;

        # set strand level score_record_flag equal to parent sub level value
        update  c_ayp_strand_student as str
        join    c_ayp_subject_student as sub
                on      str.student_id = sub.student_id
                and     str.ayp_subject_id = sub.ayp_subject_id
                and     str.school_year_id = sub.school_year_id
                and     str.month_id = sub.month_id
                and     sub.ayp_subject_id = v_ayp_subject_id
        set     str.score_record_flag = sub.score_record_flag
        ;

    end loop loop_cur_1;

end proc;
//
