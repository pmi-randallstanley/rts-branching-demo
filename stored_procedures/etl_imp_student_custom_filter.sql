/*
$Rev: 9935 $ 
$Author: randall.stanley $ 
$Date: 2011-01-26 13:06:12 -0500 (Wed, 26 Jan 2011) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_student_custom_filter.sql $
$Id: etl_imp_student_custom_filter.sql 9935 2011-01-26 18:06:12Z randall.stanley $ 
*/

drop procedure if exists etl_imp_student_custom_filter//

create definer=`dbadmin`@`localhost` procedure etl_imp_student_custom_filter()
contains sql
sql security invoker
comment '$Rev: 9935 $ $Date: 2011-01-26 13:06:12 -0500 (Wed, 26 Jan 2011) $'


proc: begin 

    declare v_ods_table                 varchar(64);
    declare v_ods_view                  varchar(64);
    declare v_view_exists               tinyint(1);
    declare v_gen_type_code             varchar(15);
    declare v_ods_col_name              varchar(64);
    declare v_no_more_rows              boolean;
    declare v_fl_bq_lg_table_exists     tinyint(1);

    # data delivered by customer thru ODS
    declare cursor_col_map_customer cursor for
        select  generic_type_code, ods_col_name
        from    tmp_gen_code_table_map
        where   pmi_data_flag = 0
        ;

    # data generate internally within app db as source for filtering
    declare cursor_col_map_pmi_data cursor for
        select  generic_type_code, ods_col_name
        from    tmp_gen_code_table_map
        where   pmi_data_flag = 1
        ;

    declare continue handler for not found 
        set v_no_more_rows = true;

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_student_custom_filter';
    set v_ods_view = concat('v_', v_ods_table);

    select  count(*)
    into    v_view_exists
    from    information_schema.views as t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view
    ;

    select  count(*)
    into    v_fl_bq_lg_table_exists
    from    information_schema.tables as t
    where   t.table_schema = database()
    and     t.table_name = 'tmp_student_lg_bq'
    ;
    

    # only proceed if this dataset is implemented for site
    if v_view_exists > 0 or v_fl_bq_lg_table_exists > 0 then

        drop table if exists `tmp_stu_cust_values`;
        drop table if exists `tmp_gen_code_table_map`;
        drop table if exists `tmp_cust_values_list`;
        drop table if exists `tmp_id_assign_value`;
        
        CREATE TABLE `tmp_stu_cust_values` (
          `generic_type_code` varchar(15) NOT NULL,
          `student_code` varchar(15) NOT NULL,
          `value_text` varchar(50) NOT NULL,
          `last_user_id` int(11) NOT NULL,
          `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
          UNIQUE KEY `uq_tmp_stu_cust_values` (`generic_type_code`,`student_code`),
          UNIQUE KEY `ind_tmp_stu_cust_values_stu` (`student_code`,`generic_type_code`,`value_text`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_gen_code_table_map` (
          `generic_type_code` varchar(15) NOT NULL,
          `ods_col_name` varchar(64) NOT NULL,
          `default_unassigned_items_flag` tinyint(1) NOT NULL default '0',
          `has_incoming_data_flag` tinyint(1) NOT NULL default '0',
          `pmi_data_flag` tinyint(1) NOT NULL default '0',
          `last_user_id` int(11) NOT NULL,
          `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
          UNIQUE KEY `uq_tmp_gen_code_table_map` (`generic_type_code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_cust_values_list` (
          `generic_type_code` varchar(15) NOT NULL,
          `value_sort_order` smallint(6) default NULL,
          `value_text` varchar(50) NOT NULL,
          `existing_value_type_id` int(11) default NULL,
          `active_flag` tinyint(1) NOT NULL,
          `create_timestamp` datetime NOT NULL default '1980-12-31 00:00:00',
          `last_user_id` int(11) NOT NULL,
          `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
          UNIQUE KEY `uq_tmp_cust_values_list_order` (`generic_type_code`,`value_sort_order`),
          UNIQUE KEY `uq_tmp_cust_values_list_text` (`generic_type_code`,`value_text`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        create table `tmp_id_assign_value` (
          `parent_id` int(11) NOT NULL,
          `new_id` int(11) not null,
          `base_code` varchar(50) not null,
          primary key  (`parent_id`,`new_id`),
          unique key `uq_tmp_id_assign_value` (`parent_id`,`base_code`)
        );

        select  count(case when st.state_abbr = 'fl' then st.state_abbr end)
        into    @is_fl_client
        from    pmi_state_info as st
        where   st.state_id = @state_id
        and     st.state_abbr in ('fl')
        ;

        # Construct mapping of available student customizable attributes to 
        # the uploader table column name
        insert tmp_gen_code_table_map (
            generic_type_code
            ,ods_col_name
            ,last_user_id
        )
        
        values ('genStuFltr1', 'filter_01', 1234)
                ,('genStuFltr2', 'filter_02', 1234)
                ,('genStuFltr3', 'filter_03', 1234)
                ,('genStuFltr4', 'filter_04', 1234)
                ,('genStuFltr5', 'filter_05', 1234)
                ,('genStuFltr6', 'filter_06', 1234)
                ,('genStuFltr7', 'filter_07', 1234)
                ,('genStuFltr8', 'filter_08', 1234)
                ,('genStuFltr9', 'filter_09', 1234)
                ,('genStuFltr10', 'filter_10', 1234)
        ;

        if @is_fl_client > 0 then
        
            insert tmp_gen_code_table_map (
                generic_type_code
                ,ods_col_name
                ,pmi_data_flag
                ,last_user_id
            )
            
            values ('pmiStuFltr1', 'math_bq', 1, 1234)
                    ,('pmiStuFltr2', 'reading_bq', 1, 1234)
                    ,('pmiStuFltr3', 'math_lg', 1, 1234)
                    ,('pmiStuFltr4', 'reading_lg', 1, 1234)
            ;
        
        end if;

        # determine which generic attributes are designated
        # to default all students to "unassigned" that are
        # not explicitly provided a value.
        update  tmp_gen_code_table_map as upd
        join    c_generic_type as gt
                on      upd.generic_type_code = gt.generic_type_code
        set     upd.default_unassigned_items_flag = gt.default_unassigned_items_flag
        ;

        # Fetch the list of values customer has delivered for each generic attribute.
        if v_view_exists > 0 then

            open cursor_col_map_customer;
            loop_cursor_col_map_customer: loop
    
            
                fetch  cursor_col_map_customer 
                into   v_gen_type_code, v_ods_col_name;
                       
                if v_no_more_rows then
                    close cursor_col_map_customer;
                    leave loop_cursor_col_map_customer;
                end if;
    
                set @sql_text :=    concat('insert tmp_stu_cust_values (generic_type_code, student_code, value_text, last_user_id) '
                                    , ' select  \'', v_gen_type_code, '\', sis_student_id, ', v_ods_col_name, ', 1234 '
                                    , ' from    v_pmi_ods_student_custom_filter ' 
                                    , ' where   ', v_ods_col_name, ' is not null '
                                    , ' on duplicate key update last_user_id = values(last_user_id) ')
                ;
    
                prepare stmt from @sql_text;
                execute stmt;
                deallocate prepare stmt;
    
            end loop loop_cursor_col_map_customer;

        end if;

        # Fetch the list of values pmi has generated for each attribute.
        if v_fl_bq_lg_table_exists > 0 then
        
            set v_no_more_rows = false;
            open cursor_col_map_pmi_data;
            loop_cursor_col_map_pmi_data: loop
    
            
                fetch  cursor_col_map_pmi_data 
                into   v_gen_type_code, v_ods_col_name;
                       
                if v_no_more_rows then
                    close cursor_col_map_pmi_data;
                    leave loop_cursor_col_map_pmi_data;
                end if;
    
                set @sql_text :=    concat('insert tmp_stu_cust_values (generic_type_code, student_code, value_text, last_user_id) '
                                    , ' select  \'', v_gen_type_code, '\', student_code, ', v_ods_col_name, ', 1234 '
                                    , ' from    tmp_student_lg_bq ' 
                                    , ' where   ', v_ods_col_name, ' is not null '
                                    , ' on duplicate key update last_user_id = values(last_user_id) ')
                ;
    
                prepare stmt from @sql_text;
                execute stmt;
                deallocate prepare stmt;
    
            end loop loop_cursor_col_map_pmi_data;

        end if;


        # Get list of pre-existing generic attibute values.
        # Need to merge existing values with incoming values to
        # minimize id churn.
        insert tmp_cust_values_list (
            generic_type_code
            ,value_text
            ,existing_value_type_id
            ,active_flag
            ,last_user_id
            ,create_timestamp
        )

        select  gt.generic_type_code
            ,gtvl.value_text
            ,gtvl.generic_type_value_id
            ,0
            ,gtvl.last_user_id
            ,gtvl.create_timestamp

        from    c_generic_type_value_list as gtvl
        join    c_generic_type as gt
                on      gt.generic_type_id = gtvl.generic_type_id
#                and     gt.classification_code = 's'
        ;
        
        # Add the list of newly delivered values per attribute (flatfile column).
        insert tmp_cust_values_list (
            generic_type_code
            ,value_text
            ,active_flag
            ,last_user_id
            ,create_timestamp
        )
        
        select  generic_type_code
            ,value_text
            ,1
            ,1234
            ,now()

        from    tmp_stu_cust_values
        group by generic_type_code, value_text
        on duplicate key update active_flag = values(active_flag)
        ;

        # Now need to assign (or re-assign) the order values should 
        # be displayed in the app. This is done currently with alpha sort
        # with the exception of "unassigned" which gets value_sort_order = 0.
        select  min(generic_type_code)
        into    v_gen_type_code
        from    tmp_cust_values_list
        ;
        
        while v_gen_type_code is not null do

            set @row_num := 0;

            update  tmp_cust_values_list as upd
            join    (
                        select  generic_type_code, value_text, @row_num := @row_num + 1 as value_order
                        from    tmp_cust_values_list as vl
                        where   generic_type_code = v_gen_type_code
                        order by value_text
                    ) as dt
                    on      upd.generic_type_code = dt.generic_type_code
                    and     upd.value_text = dt.value_text
            set     upd.value_sort_order = dt.value_order
            ;

            # Need to account for "unassigned" record
            insert tmp_cust_values_list (
                generic_type_code
                ,value_sort_order
                ,value_text
                ,active_flag
                ,last_user_id
                ,create_timestamp
            )

            select  ticvl.generic_type_code
                ,0
                ,'Unassigned'
                ,max(ticvl.active_flag) as active_flag
                ,1234
                ,now()
            
            from    tmp_cust_values_list as ticvl
            join    tmp_gen_code_table_map as tgctm
                    on      ticvl.generic_type_code = tgctm.generic_type_code
                    and     tgctm.default_unassigned_items_flag = 1
            where   ticvl.generic_type_code = v_gen_type_code
            group by ticvl.generic_type_code
            on duplicate key update active_flag = values(active_flag)
            ;

            select  min(generic_type_code)
            into    v_gen_type_code
            from    tmp_cust_values_list
            where   generic_type_code > v_gen_type_code
            ;
        
        end while;

        # Fetch id's for new values
        insert  tmp_id_assign_value (parent_id, new_id, base_code)
        select  gt.generic_type_id, pmi_f_get_next_sequence_app_db('c_generic_type_value_list', 1), src.value_text
        from    tmp_cust_values_list as src
        join    c_generic_type as gt
                on      src.generic_type_code = gt.generic_type_code
        left join   c_generic_type_value_list as tar
                on      gt.generic_type_id = tar.generic_type_id
                and     src.value_text = tar.value_text
        where   tar.generic_type_value_id is null
        ;

        # Determine which generic attributes (incoming columns)
        # have incoming data associated.
        update tmp_gen_code_table_map as upd
        join    (
                    select  vl.generic_type_code, max(vl.active_flag) as active_flag
                    from    tmp_cust_values_list as vl
                    group by vl.generic_type_code
        
                ) as dt
                on      upd.generic_type_code = dt.generic_type_code
        set     upd.has_incoming_data_flag = dt.active_flag
        ;

        # update value sort order for existing valus based on new 
        # domain of values accounting for this import
        # must do 2 step process to work around sort order re-assignment
        # conflicting with unique key
        update  c_generic_type_value_list as upd
        join    tmp_cust_values_list as src
                on      upd.generic_type_value_id = src.existing_value_type_id
        set     upd.value_sort_order = abs(upd.value_sort_order) * -1
        ;

        update  c_generic_type_value_list as upd
        join    tmp_cust_values_list as src
                on      upd.generic_type_value_id = src.existing_value_type_id
        set     upd.value_sort_order = src.value_sort_order
        ;

        # Add new values. This stmt also deactivates pre-existing records
        # for which it is not present in the instance being processed.
        insert c_generic_type_value_list (
            generic_type_value_id
            ,generic_type_id
            ,value_sort_order
            ,value_text
            ,active_flag
            ,last_user_id
            ,create_timestamp
        )

        select  coalesce(tar.generic_type_value_id, tmpid.new_id)
            ,gt.generic_type_id
            ,src.value_sort_order
            ,src.value_text
            ,src.active_flag
            ,src.last_user_id
            ,src.create_timestamp
            
        from    tmp_cust_values_list as src
        join    c_generic_type as gt
                on      src.generic_type_code = gt.generic_type_code
        left join   tmp_id_assign_value as tmpid
                on      gt.generic_type_id = tmpid.parent_id
                and     src.value_text = tmpid.base_code
        left join   c_generic_type_value_list as tar
                on      gt.generic_type_id = tar.generic_type_id
                and     src.value_text = tar.value_text
        on duplicate key update active_flag = values(active_flag)
            ,last_user_id = values(last_user_id)
        ;

        # populate the student to generic attribute value table.
        truncate table `c_student_generic_type_value_list`;

        insert  c_student_generic_type_value_list (
            student_id
            ,generic_type_value_id
            ,last_user_id
            ,create_timestamp
        )

        select  s.student_id
            ,gtvl.generic_type_value_id
            ,1234
            ,now()
        
        from    tmp_stu_cust_values as src
        join    c_student as s
                on      src.student_code = s.student_code
        join    c_generic_type as gt
                on      src.generic_type_code = gt.generic_type_code
        join    c_generic_type_value_list as gtvl
                on      gt.generic_type_id = gtvl.generic_type_id
                and     src.value_text = gtvl.value_text
        ;

        # For generic attributes designated for "default to unassigned",
        # add record for studnents not defined explicitly in incoming data file.
        insert  c_student_generic_type_value_list (
            student_id
            ,generic_type_value_id
            ,last_user_id
            ,create_timestamp
        )

        select  s.student_id
            ,gtvl.generic_type_value_id
            ,1234
            ,now()

        from    tmp_gen_code_table_map as src
        join    c_generic_type as gt
                on      src.generic_type_code = gt.generic_type_code
        join    c_generic_type_value_list as gtvl
                on      gt.generic_type_id = gtvl.generic_type_id
                and     gtvl.value_text = 'Unassigned'
        cross join  c_student as s
        left join   tmp_stu_cust_values as srcstu
                on      srcstu.generic_type_code = gt.generic_type_code
                and     srcstu.student_code = s.student_code
        where   src.has_incoming_data_flag = 1
        and     src.default_unassigned_items_flag = 1
        and     srcstu.student_code is null
        ;

        drop table if exists `tmp_stu_cust_values`;
        drop table if exists `tmp_gen_code_table_map`;
        drop table if exists `tmp_cust_values_list`;
        drop table if exists `tmp_id_assign_value`;

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
