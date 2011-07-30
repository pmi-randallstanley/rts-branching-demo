/*
$Rev: 9335 $ 
$Author: randall.stanley $ 
$Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_student_login.sql $
$Id: etl_imp_student_login.sql 9335 2010-10-03 18:10:23Z randall.stanley $ 
 */

drop procedure if exists etl_imp_student_login//

create definer=`dbadmin`@`localhost` procedure etl_imp_student_login()
contains sql
sql security invoker
comment '$Rev: 9335 $ $Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $'


proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name
    and     t.table_name = 'v_pmi_ods_student_login';

    if @view_exists > 0 then
    
        update  c_student as st
        join    v_pmi_ods_student_login as ods
                on      st.student_code = ods.student_id
                and     ods.student_login is not null
        set     login = ods.student_login
                ,`password` = ods.student_password
        ;
        
        -- Update imp_upload_log
        set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_student_login\', \'P\', \'ETL Load Successful\')');
        
        prepare sql_string from @sql_string;
        execute sql_string;
        deallocate prepare sql_string;     
        

    end if;
    
end proc;
//
