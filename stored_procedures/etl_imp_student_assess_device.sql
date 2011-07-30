/*
$Rev$ 
$Author$ 
$Date$
$HeadURL$
$Id$ 
*/

drop procedure if exists etl_imp_student_assess_device//

create definer=`dbadmin`@`localhost` procedure etl_imp_student_assess_device()
contains sql
sql security invoker
comment '$Rev$ $Date$'


proc: begin 

    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_view_exists tinyint(1);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_stu_assess_device';
    set v_ods_view = concat('v_', v_ods_table);

    select  count(*)
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;
    

    if v_view_exists > 0 then

        update  c_student as st
        join    v_pmi_ods_stu_assess_device as ods
                on      st.student_code = ods.sis_student_code
                and     ods.assess_device_code is not null
        set     st.assess_device_code = ods.assess_device_code
        ;

        #################
        ## Update Log
        #################
        set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', v_ods_table, '\', \'P\', \'ETL Load Successful\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;

    end if;

end proc;
//
