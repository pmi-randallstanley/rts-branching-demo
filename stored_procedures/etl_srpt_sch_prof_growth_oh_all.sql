
drop procedure if exists etl_srpt_sch_prof_growth_oh_all //

CREATE DEFINER=`dbadmin`@`localhost` PROCEDURE etl_srpt_sch_prof_growth_oh_all()
        
    SQL SECURITY INVOKER
    COMMENT 'zendesk ticket 19687'
proc: begin 
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);
    
    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name
    and     t.table_name = 'v_pmi_ods_state_data_grade_prof_oh_all';
    
    if @view_exists > 0 then
        select  count(*)
        into    @view_count
        from    v_pmi_ods_state_data_grade_prof_oh_all
        ;
        
        if @view_count > 0 then 
            truncate table srpt_dst_sch_grade_prof_growth;
            insert  srpt_dst_sch_grade_prof_growth (
                district_id
                ,grade_level_id
                ,school_id
                ,ayp_subject_id
                ,school_year_id
                ,student_count
                ,proficient_pct
                ,proficient_count
                ,last_user_id
                ,create_timestamp
            )
            
            select  d.district_id
                ,gl.grade_level_id
                ,sch.school_id
                ,sub.ayp_subject_id
                ,sy.school_year_id
                ,cast(ods.student_count as signed)
                ,round(((cast(ods.proficient_count as decimal)/cast(ods.student_count as decimal)) * 100),0)
                ,round(cast(proficient_count as decimal),0) 
                ,1234
                ,now()
                
            from    v_pmi_ods_state_data_grade_prof_oh_all as ods
            join    s_district as d
                    on      ods.district_state_code = d.state_district_code
            join    grade_level as gl
                    on      ods.grade_code = gl.grade_code
            join    s_school as sch
                    on      d.district_id = sch.district_id
                    and     ods.school_state_code = sch.school_state_code
            join    s_ayp_subject as sub
                    on      ods.subject_code = sub.ayp_subject_code
            join    s_school_year as sy
                    on      ods.school_year = sy.school_year_code
            where   ods.student_count is not null and ods.student_count > 0
            on duplicate key update student_count = values(student_count)
                ,proficient_pct = values(proficient_pct)
                ,proficient_count = values(proficient_count)
            ;
            
           call etl_srpt_sch_prof_growth_agg();
     
            set @sql_string := '';
            set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_state_data_grade_prof_oh_all', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
            
            prepare sql_string from @sql_string;
            execute sql_string;
            deallocate prepare sql_string;     
        end if;
        
    end if;
end proc ;
//
