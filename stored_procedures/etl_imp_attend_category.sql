/*
$Rev$ 
$Author$ 
$Date$
$HeadURL$
$Id$ 
*/

drop procedure if exists etl_imp_attend_category//

create definer=`dbadmin`@`localhost` procedure etl_imp_attend_category()
contains sql
sql security invoker
comment '$Rev$ $Date$'


proc: begin 

    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_view_exists tinyint(1);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_attend_category';
    set v_ods_view = concat('v_', v_ods_table);

    drop table if exists `tmp_id_assign`;
    
    create table `tmp_id_assign` (
      `new_id` int(11) not null,
      `base_code` varchar(50) not null,
      primary key  (`new_id`),
      unique key `uq_tmp_id_assign` (`base_code`)
    )
    ;

    select  count(*)
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;
    

    if v_view_exists > 0 then

        insert tmp_id_assign (
            new_id
            ,base_code
        )

        select  pmi_f_get_next_sequence_app_db('c_attendance_category', 1), ods.cat_code
        from    v_pmi_ods_attend_category as ods
        left join   c_attendance_category as tar
                on      ods.cat_code = tar.cat_code
        where   tar.cat_id is null
        ;

        insert c_attendance_category (
            cat_id
            ,cat_code
            ,moniker
            ,description
            ,at_risk_flag
            ,active_flag
            ,create_user_id
            ,create_timestamp
            ,last_user_id
        )

        select  coalesce(tmpid.new_id, tar.cat_id) as cat_id
            ,ods.cat_code
            ,ods.cat_title
            ,ods.cat_description
            ,case when ods.at_risk_indicator in ('y', '1') then 1 else 0 end
            ,1
            ,1234
            ,now()
            ,1234
            
        from    v_pmi_ods_attend_category as ods
        left join   c_attend_category as tar
                on      ods.cat_code = tar.cat_code
        left join   tmp_id_assign as tmpid
                on      ods.cat_code = tmpid.base_code
        on duplicate key update moniker = values(moniker)
            ,description = values(description)
            ,at_risk_flag = values(at_risk_flag)
            ,active_flag = values(active_flag)
        ;


        ####  Cleanup
        drop table if exists `tmp_id_assign`;

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
