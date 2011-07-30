/*
$Rev: 9281 $ 
$Author: randall.stanley $ 
$Date: 2010-09-28 12:11:40 -0400 (Tue, 28 Sep 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_ayp_enrollment.sql $
$Id: etl_imp_ayp_enrollment.sql 9281 2010-09-28 16:11:40Z randall.stanley $ 
*/

DROP PROCEDURE IF EXISTS etl_imp_ayp_enrollment //

create definer=`dbadmin`@`localhost` procedure etl_imp_ayp_enrollment()
CONTAINS SQL
COMMENT '$Rev: 9281 $ $Date: 2010-09-28 12:11:40 -0400 (Tue, 28 Sep 2010) $'
SQL SECURITY INVOKER

begin

    declare no_more_rows boolean; 
    declare v_school_year_id int;
    declare v_use_stu_state_code char(1) default 'n';    
    
    declare cur_1 cursor for 
    select school_year from v_pmi_ods_ayp_enrollment r
    group by  r.school_year; 
    
    declare continue handler for not found 
    set no_more_rows = true;
        
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set @use_stu_state_code := pmi_f_get_etl_setting('aypEnrollUseStuStateCode');
    if @use_stu_state_code is not null then
        set v_use_stu_state_code = @use_stu_state_code;
    end if;

    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_ayp_enrollment';

    if @view_exists > 0 then
        
        open cur_1;
        
        loop_cur_1: loop
       
            fetch  cur_1
            into   v_school_year_id;
           
            if no_more_rows then
                close cur_1;
                leave loop_cur_1;
            end if;
            
            update  c_student_year
            set     ayp_school_id = null
                    ,ayp_lep_flag = 0
                    ,ayp_swd_flag = 0
                    ,ayp_ed_flag = 0
                    ,ayp_ethnicity_id = null
                    ,ayp_enrollment_flag = 0
            where   school_year_id = v_school_year_id
            ;
               
            if v_use_stu_state_code = 'y' then
                update      c_student_year sy
                join        c_student s
                        on      sy.student_id = s.student_id
                        and     s.active_flag = 1
                join        v_pmi_ods_ayp_enrollment o
                        on      o.student_id = s.student_state_code
                        and     sy.active_flag = 1
                left join   c_school as sch
                        on    sch.school_code = o.ayp_school_id
                left join   v_pmi_xref_lep lep
                        on      lep.client_lep_code = o.ayp_lep_code
                left join   v_pmi_xref_swd swd
                        on      swd.client_swd_code = o.ayp_swd_code
                left join   v_pmi_xref_econ_disadv ed
                        on      ed.client_econ_disadv_code = o.ayp_ed_code
                left join   v_pmi_xref_race as xr
                        on      xr.client_race_code = o.ayp_race_code
                left join    c_ethnicity as e
                        on      e.ethnicity_code = xr.pmi_race_code
                set sy.ayp_school_id = sch.school_id,
                    sy.ayp_lep_flag = coalesce(lep.lep_flag,0),
                    sy.ayp_swd_flag = coalesce(swd.swd_flag,0), 
                    sy.ayp_ed_flag = coalesce(ed.econ_disadv_flag,0), 
                    sy.ayp_ethnicity_id = e.ethnicity_id,
                    sy.ayp_enrollment_flag = 1
                where    o.student_id is not null and
                              sy.school_year_id = v_school_year_id;

            else
                update      c_student_year sy
                join        c_student s
                        on      sy.student_id = s.student_id
                        and     s.active_flag = 1
                join        v_pmi_ods_ayp_enrollment o
                        on      o.student_id = s.student_code
                        and     sy.active_flag = 1
                left join   c_school as sch
                        on    sch.school_code = o.ayp_school_id
                left join   v_pmi_xref_lep lep
                        on      lep.client_lep_code = o.ayp_lep_code
                left join   v_pmi_xref_swd swd
                        on      swd.client_swd_code = o.ayp_swd_code
                left join   v_pmi_xref_econ_disadv ed
                        on      ed.client_econ_disadv_code = o.ayp_ed_code
                left join   v_pmi_xref_race as xr
                        on      xr.client_race_code = o.ayp_race_code
                left join    c_ethnicity as e
                        on      e.ethnicity_code = xr.pmi_race_code
                set sy.ayp_school_id = sch.school_id,
                    sy.ayp_lep_flag = coalesce(lep.lep_flag,0),
                    sy.ayp_swd_flag = coalesce(swd.swd_flag,0), 
                    sy.ayp_ed_flag = coalesce(ed.econ_disadv_flag,0), 
                    sy.ayp_ethnicity_id = e.ethnicity_id,
                    sy.ayp_enrollment_flag = 1
                where    o.student_id is not null and
                              sy.school_year_id = v_school_year_id;
            
            end if;

        end loop loop_cur_1;

    end if;                

end;
//
