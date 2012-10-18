drop procedure if exists fast_nav_add_node //
create definer=`dbadmin`@`localhost` procedure fast_nav_add_node 
   (
    p_nav_code varchar(50)
    ,p_parent_nav_code varchar(50)
    ,p_type_code varchar(50)
    ,p_map_to_type_code varchar(50)
    ,p_window_class varchar(100)
    ,p_ref_to_table_name varchar(64)
    ,p_dim_key varchar(64)
    ,p_fact_key varchar(64)
    ,p_level_sort_order tinyint(4)
    ,p_display_text varchar(1000)
    ,p_hover_text varchar(100)
    ,p_count_column varchar(64)
    ,p_filter_set_name varchar(255)
    ,p_initial_filter_string varchar(1000)
   )

contains sql
comment ''
sql security invoker


proc: begin


    declare  v_insert_right             int(11);
    declare  v_lft                      int(11);
    declare  v_rgt                      int(11);
    declare  v_msg_id                   int(11);
    declare  v_not_found                boolean default '0';
    declare  v_parent_id                int(11);
    declare  v_nav_id                   int(11) default '0';
    declare  v_msg_moniker              varchar(1000);
    declare  v_ref_table_id             int(11);
    declare  v_filter_set_id            int(11);
    declare  v_msg_exists               tinyint(1);
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND 
        SET v_not_found = TRUE;

    select   nav_id
            ,rgt 
    into     v_parent_id
            ,v_insert_right
    from     fast_nav
    where    nav_code = p_parent_nav_code
    ;
   
    IF !v_not_found THEN
    
        set v_msg_moniker = concat('fn',p_nav_code);
        
        select  table_id
        into    v_ref_table_id
        from    fast_table
        where   phys_table_name = p_ref_to_table_name
        ;

        select  msg_id 
        into    v_msg_id
        from    pmi_sys_message
        where   msg_moniker = v_msg_moniker
        ;

        select  filter_set_id 
        into    v_filter_set_id
        from    faste_filter_set
        where   filter_set_name = p_filter_set_name
        ;

        select  nav_id
            ,lft
            ,rgt
            
        into    v_nav_id
            ,v_lft
            ,v_rgt
            
        from    fast_nav
        where   nav_code = p_nav_code
        ;
        
        if v_nav_id = 0 then
        
            load_data: begin
                declare exit handler for sqlexception rollback;
                start transaction;            
            
                set v_nav_id = pmi_f_get_next_sequence_app_db('fast_nav', 1);
           
                update  fast_nav
                set     lft =   case    when    lft > v_insert_right then lft + 2
                                    else lft
                              end,
                        rgt =   case    when    rgt >= v_insert_right then rgt + 2
                                    else rgt
                              end 
                where    rgt >= v_insert_right
                ;

                update  fast_nav
                set     level_sort_order = level_sort_order + 1
                where   parent_id = v_parent_id
                and     level_sort_order >= p_level_sort_order
                ;

                insert fast_nav (
                    nav_id
                    ,nav_code
                    ,type_code
                    ,map_to_type_code
                    ,msg_id
                    ,lft
                    ,rgt
                    ,parent_id
                    ,level_sort_order
                    ,filter_set_id
                    ,table_id
                    ,d_key
                    ,f_key
                    ,window_class
                    ,hover_text
                    ,count_column
                    ,initial_filter_string
                    ,active_flag
                    ,create_user_id
                    ,last_user_id
                    ,create_timestamp
                )

                values( 
                    v_nav_id
#                     ,case when p_filter_set_name is not null and p_nav_code is null then cast(v_nav_id as char)
#                             else p_nav_code
#                     end
                    ,p_nav_code
                    ,p_type_code
                    ,p_map_to_type_code
                    ,v_msg_id
                    ,v_insert_right
                    ,v_insert_right + 1
                    ,v_parent_id
                    ,coalesce(p_level_sort_order, 0)
                    ,v_filter_set_id
                    ,v_ref_table_id
                    ,p_dim_key
                    ,p_fact_key
                    ,p_window_class
                    ,p_hover_text
                    ,p_count_column
                    ,p_initial_filter_string
                    ,1
                    ,1234
                    ,1234
                    ,now()
                )
                on duplicate key update last_user_id = values(last_user_id)
                ;

                commit;

            end load_data;

        elseif v_nav_id > 0 then

                insert fast_nav (
                    nav_id
                    ,nav_code
                    ,type_code
                    ,map_to_type_code
                    ,msg_id
                    ,lft
                    ,rgt
                    ,parent_id
                    ,level_sort_order
                    ,filter_set_id
                    ,table_id
                    ,d_key
                    ,f_key
                    ,window_class
                    ,hover_text
                    ,count_column
                    ,initial_filter_string
                    ,active_flag
                    ,create_user_id
                    ,last_user_id
                    ,create_timestamp
                )

                values( 
                    v_nav_id
                    ,p_nav_code
                    ,p_type_code
                    ,p_map_to_type_code
                    ,v_msg_id
                    ,v_lft
                    ,v_rgt
                    ,v_parent_id
                    ,coalesce(p_level_sort_order, 0)
                    ,v_filter_set_id
                    ,v_ref_table_id
                    ,p_dim_key
                    ,p_fact_key
                    ,p_window_class
                    ,p_hover_text
                    ,p_count_column
                    ,p_initial_filter_string
                    ,1
                    ,1234
                    ,1234
                    ,now()
                )
                on duplicate key update type_code = values(type_code)
                    ,map_to_type_code = values(map_to_type_code)
                    ,msg_id = values(msg_id)
                    ,level_sort_order = values(level_sort_order)
                    ,filter_set_id = values(filter_set_id)
                    ,table_id = values(table_id)
                    ,d_key = values(d_key)
                    ,f_key = values(f_key)
                    ,window_class = values(window_class)
                    ,hover_text = values(hover_text)
                    ,count_column = values(count_column)
                    #,initial_filter_string = values(initial_filter_string)
                    #,active_flag = values(active_flag)
                    ,last_user_id = values(last_user_id)
                ;
        
        end if;

    END IF;

end proc;
//
