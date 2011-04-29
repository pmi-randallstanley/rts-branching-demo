/*
$Rev$ 
$Author$
$Date$
$HeadURL$
$Id$
*/

drop procedure if exists etl_color_snap//

create definer=`dbadmin`@`localhost` procedure etl_color_snap()
contains sql
sql security invoker
comment '$Rev$ $Date$'


proc: begin 

    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_view_exists tinyint(1);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    set v_ods_table = 'pmi_ods_color_snap';
    set v_ods_view = concat('v_', v_ods_table);

    select  count(*)
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;
    
    if v_view_exists > 0 then

        truncate table pm_color_snap;
        
        insert pm_color_snap (
            measure_period_id
           ,color_id
           ,min_score
           ,max_score
           ,last_user_id
           ,create_timestamp
        )
        select smp.measure_period_id
              ,c.color_id
              ,ods.min_score
              ,ods.max_score
              ,1234
              ,NOW()
        from v_pmi_ods_color_snap ods
        join c_grade_level gl
        join pmi_color c
          on c.moniker = ods.color_name
        join pm_snap_measure sm
          on sm.measure_code = ods.measure_code
        join pm_snap_assess_freq_period safp
          on safp.period_code = ods.period_code
        join pm_snap_measure_period smp
          on smp.period_id = safp.period_id
         and smp.measure_id = sm.measure_id
         and smp.grade_level_id = gl.grade_level_id
        where gl.grade_code = 'KG'
        order by 1,2
        on duplicate key update last_user_id = values(last_user_id)
            ,min_score = values(min_score)
            ,max_score = values(max_score)
        ;

        delete  csl.*
        from    c_color_swatch_list as csl
        join    c_color_swatch as cs
                on      csl.swatch_id = cs.swatch_id
                and     cs.swatch_code = 'snap'
        ;

        set @rownum = 0;
        insert into c_color_swatch_list (
            swatch_id
           ,client_id
           ,color_id
           ,sort_order
           ,last_user_id
           ,create_timestamp
           ,last_edit_timestamp
        ) 
        select  dt.swatch_id
            ,@client_id
            ,dt.color_id
            ,@rownum := @rownum + 1 as sort_order
            ,1234
            ,now()
            ,now()
        
        from    (
                    select  cs.swatch_id, c.color_id
                    from    pmi_color as c
                    join    pm_color_snap as csrc
                            on      c.color_id = csrc.color_id
                    join  c_color_swatch as cs
                            on      cs.swatch_code = 'snap'
                    where   c.active_flag = 1
                    group by cs.swatch_id, c.color_id
                ) as dt
        order by dt.color_id
        on duplicate key update last_user_id = 1234
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
