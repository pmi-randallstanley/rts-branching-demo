drop procedure if exists etl_color_bbcard_pmrn//

create definer=`dbadmin`@`localhost` procedure etl_color_bbcard_pmrn()
contains sql
sql security invoker
comment '$Rev$ $Date$'


proc: begin 

    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_view_exists tinyint(1);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_color_fair';
    set v_ods_view = concat('v_', v_ods_table);

    select  count(*)
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;
    

    if v_view_exists > 0 then

        select  bb_group_id
        into    @bb_group_id
        from    pm_bbcard_group
        where   bb_group_code = 'pmrn'
        ;

        select  swatch_id
        into    @swatch_id
        from    c_color_swatch
        where   swatch_code = 'fair'
        ;
    
        truncate table pm_bbcard_color_pmrn;
        
        insert pm_bbcard_color_pmrn (
            bb_group_id
            ,bb_measure_id
            ,bb_measure_item_id
            ,grade_level_id
            ,color_id
            ,min_score
            ,max_score
            ,last_user_id
            ,create_timestamp
        )

        select  bmi.bb_group_id
            ,bmi.bb_measure_id
            ,bmi.bb_measure_item_id
            ,gl.grade_level_id
            ,c.color_id
            ,ods.min_score
            ,ods.max_score
            ,1234
            ,now()
        
        from    v_pmi_ods_color_fair as ods
        join    pm_bbcard_measure as bm
                on      bm.bb_group_id = @bb_group_id
                and     bm.bb_measure_code = ods.measure_code
        join    pm_bbcard_measure_item as bmi
                on      bmi.bb_group_id = bm.bb_group_id
                and     bmi.bb_measure_id = bm.bb_measure_id
                and     bmi.bb_measure_item_code = ods.period_code
        join    c_grade_level as gl
                on      gl.grade_code = ods.grade_code
        join    pmi_color as c
                on      c.moniker = ods.color_name
        on duplicate key update last_user_id = values(last_user_id)
            ,min_score = values(min_score)
            ,max_score = values(max_score)
        ;

        delete  csl.*
        from    c_color_swatch_list as csl
        where   csl.swatch_id = @swatch_id
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
        select  @swatch_id
            ,@client_id
            ,dt.color_id
            ,@rownum := @rownum + 1 as sort_order
            ,1234
            ,now()
            ,now()
        
        from    (
                    select  c.color_id
                    from    pmi_color as c
                    join    pm_bbcard_color_pmrn as csrc
                            on      c.color_id = csrc.color_id
                    where   c.active_flag = 1
                    group by c.color_id
                ) as dt
        order by dt.color_id
        on duplicate key update last_user_id = 1234
        ;
        
        
        ## Update Data with new color file
        update rpt_bbcard_detail_pmrn as rpt
        join    c_student_year sy
                on    rpt.student_id = sy.student_id
                and   rpt.school_year_id = sy.school_year_id
        join    c_grade_level gl
                on    sy.grade_level_id = gl.grade_level_id
        join    pm_bbcard_color_pmrn as cs
                on    rpt.bb_group_id = cs.bb_group_id
                and   rpt.bb_measure_id = cs.bb_measure_id
                and   rpt.bb_measure_item_id = cs.bb_measure_item_id 
                and   gl.grade_level_id = cs.grade_level_id
                and   rpt.score between cs.min_score and cs.max_score
        join    pmi_color as c
                on    c.color_id = cs.color_id
        set rpt.score_color = c.moniker
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
