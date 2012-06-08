

drop procedure if exists etl_hst_load_ca_star//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_ca_star()
contains sql
sql security invoker
comment 'zendesk ticket 19687'

proc: begin


    declare v_view_exists int(10);
    declare v_ods_view varchar(64);
    declare v_ods_table varchar(64);
    
    set v_ods_view := 'v_pmi_ods_ca_star';
    set v_ods_table := 'pmi_ods_ca_star';

    select  count(*)
    into    v_view_exists
    from    information_schema.views as t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;


    if v_view_exists > 0  then

        call etl_hst_load_ca_star_capa();
        call etl_hst_load_ca_star_cma();
        call etl_hst_load_ca_star_eoc();
        call etl_hst_load_ca_star_cst();

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