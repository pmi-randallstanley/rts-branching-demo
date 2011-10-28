drop procedure if exists etl_fast_set_table_column_unique_counts //
create definer=`dbadmin`@`localhost` procedure etl_fast_set_table_column_unique_counts ()

contains sql
sql security invoker
comment '$rev: etl_fast_set_table_column_unique_counts $'

proc: begin

    declare v_no_more_rows      boolean; 
    declare v_table_exists      tinyint; 
    declare v_table_id          int(11);
    declare v_column_id         int(11);
    declare v_phys_table_name   varchar(64);
    declare v_phys_col_name     varchar(64);

    declare cur_col_values cursor for 
        select  table_id
            ,column_id
            ,phys_table_name
            ,phys_column_name
            
        from    tmp_fast_table_column_value
        where   valid_column_flag = 1
        ;

    declare continue handler for not found set v_no_more_rows = true;

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    drop table if exists `tmp_fast_table_column_info_schema`;
    drop table if exists `tmp_fast_table_column_value`;
    
    CREATE TABLE `tmp_fast_table_column_value` (
      `table_id` int(11) NOT NULL,
      `column_id` int(11) NOT NULL,
      `phys_table_name` varchar(64) NOT NULL,
      `phys_column_name` varchar(64) NOT NULL,
      `valid_column_flag` tinyint(1) NOT NULL default '0',
      `unique_dw_value_cnt` int(11) NOT NULL default '0',
      `last_user_id` int(11) NOT NULL,
      `create_timestamp` datetime NOT NULL default '1980-01-01 00:00:00',
      `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
      PRIMARY KEY  (`table_id`,`column_id`),
      UNIQUE KEY `uq_tmp_fast_table_column_value` (`phys_table_name`,`phys_column_name`),
      KEY `ind_tmp_fast_table_column_value_ak1` (`table_id`,`phys_column_name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1
    ;

    create table `tmp_fast_table_column_info_schema` (
      `phys_table_name` varchar(64) not null,
      `phys_column_name` varchar(64) not null,
      `column_order` int(10) not null,
      `last_edit_timestamp` timestamp not null default current_timestamp on update current_timestamp,
      unique key `uq_tmp_fast_table_column_info_schema` (`phys_table_name`,`phys_column_name`)
    ) engine=innodb default charset=latin1
    ;

    insert tmp_fast_table_column_value (
        table_id
        ,column_id
        ,phys_table_name
        ,phys_column_name
        ,valid_column_flag
        ,unique_dw_value_cnt
        ,last_user_id
        ,create_timestamp
    )
    
    select  ftc.table_id
        ,ftc.column_id
        ,ft.phys_table_name
        ,ftc.phys_column_name
        ,0
        ,0
        ,1234
        ,now()
    
    from    fast_table as ft
    join    fast_table_column as ftc
            on      ft.table_id = ftc.table_id
            and     ftc.nav_use_flag = 1
    where   ft.nav_use_flag = 1
    and     ft.active_flag = 1
    ;

    insert tmp_fast_table_column_info_schema (
        phys_table_name
        ,phys_column_name
        ,column_order
    )
    
    select  table_name
        ,column_name
        ,ordinal_position
        
    from    information_schema.columns
    where   table_schema = @db_name_dw
    and     (table_name like 'f%'
            or  table_name like 'd%'
            )
    ;

    update  tmp_fast_table_column_value as upd
    join    tmp_fast_table_column_info_schema as info
            on      upd.phys_table_name = info.phys_table_name
            and     upd.phys_column_name = info.phys_column_name
    set     upd.valid_column_flag = 1
    ;

    set v_no_more_rows = false;
    open cur_col_values;
    loop_cur_col_values: loop


        fetch  cur_col_values 
        into   v_table_id, v_column_id, v_phys_table_name, v_phys_col_name
        ;
               
        if v_no_more_rows then
            close cur_col_values;
            leave loop_cur_col_values;
        end if;

        set @sql_string := concat('select count(distinct ', v_phys_col_name, ') into @val_cnt '
                                ,' from ', @db_name_dw, '.', v_phys_table_name )
        ;
        
        
        prepare sql_string from @sql_string;
        execute sql_string;
        deallocate prepare sql_string;

        
        update  tmp_fast_table_column_value
        set     unique_dw_value_cnt = @val_cnt
        where   table_id = v_table_id
        and     column_id = v_column_id
        ;


    end loop loop_cur_col_values;
        

    update  fast_table_column
    set     unique_dw_value_cnt = 0
    ;
    
    update  fast_table_column as upd
    join    tmp_fast_table_column_value as src
            on      upd.table_id = src.table_id
            and     upd.column_id = src.column_id
    set     upd.unique_dw_value_cnt = src.unique_dw_value_cnt
    ;

    drop table if exists `tmp_fast_table_column_value`;

end proc;
//
