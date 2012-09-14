drop procedure if exists etl_color_star//

create definer=`dbadmin`@`localhost` procedure etl_color_star()
contains sql
sql security invoker
comment '$Rev$ $Date$'


proc: begin 

    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_view_exists tinyint(1);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_color_star';
    set v_ods_view = concat('v_', v_ods_table);

    select  count(*)
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;
    

    if v_view_exists > 0 then

        select  count(*) 
        into    @view_count
        from    v_pmi_ods_color_star;
               
        if @view_count > 0 then

            truncate TABLE pm_color_star;
            
            INSERT pm_color_star (bb_group_id, bb_measure_id, bb_measure_item_id, begin_year, end_year, min_score, max_score, color_id, begin_grade_sequence, end_grade_sequence, last_user_id, create_timestamp, last_edit_timestamp)
                        SELECT  g.bb_group_id
                   ,m.bb_measure_id
                   ,mi.bb_measure_item_id
                   ,ods.begin_year
                   ,ods.end_year
                   ,ods.min_score
                   ,ods.max_score
                   ,c.color_id 
                   ,glbeg.grade_sequence as begin_grade_sequence
                   ,glend.grade_sequence as end_grade_sequence
                   ,1234
                   ,now()
                   ,now()  
            FROM v_pmi_ods_color_star ods
            JOIN  pm_bbcard_measure m
              ON    ods.bb_measure_code = m.bb_measure_code
            JOIN pm_bbcard_measure_item mi 
              ON m.bb_measure_id = mi.bb_measure_id
             and ods.bb_measure_item_code = mi.bb_measure_item_code
            JOIN  pm_bbcard_group g
              ON  g.bb_group_id = m.bb_group_id
              and g.bb_group_code = 'star' 
            JOIN  pmi_color c
              ON    c.moniker = ods.color_name
            JOIN  v_pmi_xref_grade_level xbeg
              ON    ods.begin_grade = xbeg.client_grade_code
            JOIN  c_grade_level glbeg
              ON    xbeg.pmi_grade_code = glbeg.grade_code
            JOIN  v_pmi_xref_grade_level xend
              ON    ods.end_grade = xend.client_grade_code
            JOIN  c_grade_level glend
              ON    xend.pmi_grade_code = glend.grade_code
            ON DUPLICATE key UPDATE last_user_id = 1234, min_score = ods.min_score, max_score= ods.max_score;

            delete  csl.*
            from    c_color_swatch_list as csl
            join    c_color_swatch as cs
                    on      csl.swatch_id = cs.swatch_id
                    and     cs.swatch_code = 'star'
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
                        select  cs.swatch_id, c.color_id,  min(csrc.min_score) as min_score
                        from    pmi_color as c
                        join    pm_color_star as csrc
                                on      c.color_id = csrc.color_id
                        join  c_color_swatch as cs
                                on      cs.swatch_code = 'star'
                        where   c.active_flag = 1
                        group by cs.swatch_id, c.color_id
                    ) as dt
            order by dt.min_score, dt.color_id
            on duplicate key update last_user_id = 1234
            ;
            
            
            ## Update Data with new color file
            update rpt_bbcard_detail_star as rpt
            join    c_student_year sy
                    on    rpt.student_id = sy.student_id
                    and   rpt.school_year_id = sy.school_year_id
            join    c_grade_level gl
                    on    sy.grade_level_id = gl.grade_level_id
            join    pm_color_star as cs
                    on    rpt.bb_group_id = cs.bb_group_id
                    and   rpt.bb_measure_id = cs.bb_measure_id
                    and   rpt.bb_measure_item_id = cs.bb_measure_item_id 
                    and   rpt.score between cs.min_score and cs.max_score
                    and   rpt.school_year_id BETWEEN cs.begin_year AND cs.end_year
                    and   gl.grade_sequence between cs.begin_grade_sequence and cs.end_grade_sequence
            join    pmi_color as c
                    on    c.color_id = cs.color_id
            set rpt.score_color = c.moniker
            ;
            
            set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', v_ods_table, '\', \'P\', \'ETL Load Successful\')');
        
            prepare sql_scan_log from @sql_scan_log;
            execute sql_scan_log;
            deallocate prepare sql_scan_log;

        end if;
    
    end if;

end proc;
//
