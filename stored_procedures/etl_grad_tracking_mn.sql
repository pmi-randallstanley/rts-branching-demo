
DROP PROCEDURE IF EXISTS etl_grad_tracking_mn //

create definer=`dbadmin`@`localhost` procedure etl_grad_tracking_mn()
contains sql
sql security invoker
comment '$Load MN Grad Tracking Report $'

proc: begin

    declare v_ayp_subject_id_reading int(10);
    declare v_ayp_subject_id_writing int(10);
    declare v_ayp_subject_id_math int(10);
    declare v_fail_color varchar(50);
    declare v_pass_color varchar(50);
    
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*) 
    into    @view_data
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_mn_grad_tracking';
    
    if @view_data > 0 then

        select  coalesce(count(*),0)
        into    @record_count
        from    v_pmi_ods_mn_grad_tracking;
        
        if @record_count > 0 Then
            
            # Get AYP Subject ID's
            select  ayp_subject_id into v_ayp_subject_id_reading
            from    c_ayp_subject sub
            where   sub.ayp_subject_code = 'gradRead'
            ;
            
            select  ayp_subject_id into v_ayp_subject_id_math
            from    c_ayp_subject sub
            where   sub.ayp_subject_code = 'gradMath'
            ;
            
            select  ayp_subject_id into v_ayp_subject_id_writing
            from    c_ayp_subject sub
            where   sub.ayp_subject_code = 'gradWriting'
            ;
            
            # Get Colors
            select clr.moniker 
            into   v_fail_color
            from c_color_swatch sw
            join  c_color_swatch_list swl
                  on    swl.swatch_id = sw.swatch_id
                  and   sw.swatch_code = 'gradReqMet'
            join  pmi_color clr
                  on    swl.color_id = clr.color_id
            where swl.sort_order = 1
            ;
            
            select clr.moniker 
            into   v_pass_color
            from c_color_swatch sw
            join  c_color_swatch_list swl
                  on    swl.swatch_id = sw.swatch_id
                  and   sw.swatch_code = 'gradReqMet'
            join  pmi_color clr
                  on    swl.color_id = clr.color_id
            where swl.sort_order = 2
            ;

            ## Math
            insert into rpt_grad_sub_stu_req_met (
                  ayp_subject_id
                  ,student_id
                  ,req_met_flag
                  ,req_met_color
                  ,last_user_id
                  ,create_timestamp)
            select  v_ayp_subject_id_math as ayp_subject_id
                  , st.student_id
                  , case
                        when v.pass_or_method is null then null
                        when v.pass_or_method = 'N' then 0
                        else '1'
                    end as req_met_flag
                  , case
                        when v.pass_or_method is null then null
                        when v.pass_or_method = 'N' then v_fail_color
                        else v_pass_color
                    end as req_met_color
                  , 1234
                  , now()
            from c_student st    
            join v_pmi_ods_mn_grad_tracking v
                  on    st.student_state_code = v.student_id 
                  and   v.subject_code = 'M'  
            order by v.student_id, v.subject_code, req_met_flag ## The Order by is important.  We want req_met_flag = 'Y' if any admin is passed by stu/sub
            on duplicate key update req_met_flag = values(req_met_flag)
                    ,req_met_color = values(req_met_color)
                    ,last_user_id = values(last_user_id)
                ;
                
            ## Reading
            insert into rpt_grad_sub_stu_req_met (
                  ayp_subject_id
                  ,student_id
                  ,req_met_flag
                  ,req_met_color
                  ,last_user_id
                  ,create_timestamp)
            select  v_ayp_subject_id_reading as ayp_subject_id
                  , st.student_id
                  , case
                        when v.pass_or_method is null then null
                        when v.pass_or_method = 'N' then 0
                        else '1'
                    end as req_met_flag
                  , case
                        when v.pass_or_method is null then null
                        when v.pass_or_method = 'N' then v_fail_color
                        else v_pass_color
                    end as req_met_color
                  , 1234
                  , now()
            from c_student st    
            join v_pmi_ods_mn_grad_tracking v
                  on    st.student_state_code = v.student_id 
                  and   v.subject_code = 'R'  
            order by v.student_id, v.subject_code, req_met_flag  ## The Order by is important.  We want req_met_flag = 'Y' if any admin is passed by stu/sub
            on duplicate key update req_met_flag = values(req_met_flag)
                    ,req_met_color = values(req_met_color)
                    ,last_user_id = values(last_user_id)
                ;
                
            ## Writing
            insert into rpt_grad_sub_stu_req_met (
                  ayp_subject_id
                  ,student_id
                  ,req_met_flag
                  ,req_met_color
                  ,last_user_id
                  ,create_timestamp)
            select  v_ayp_subject_id_writing as ayp_subject_id
                  , st.student_id
                  , case
                        when v.pass_or_method is null then null
                        when v.pass_or_method = 'N' then 0
                        else '1'
                    end as req_met_flag
                  , case
                        when v.pass_or_method is null then null
                        when v.pass_or_method = 'N' then v_fail_color
                        else v_pass_color
                    end as req_met_color
                  , 1234
                  , now()
            from c_student st    
            join v_pmi_ods_mn_grad_tracking v
                  on    st.student_state_code = v.student_id 
                  and   v.subject_code = 'W'  
            order by v.student_id, v.subject_code, req_met_flag ## The Order by is important.  We want req_met_flag = 'Y' if any admin is passed by stu/sub
            on duplicate key update req_met_flag = values(req_met_flag)
                    ,req_met_color = values(req_met_color)
                    ,last_user_id = values(last_user_id)
                ;
            
    
            #Update imp_upload_log
            
            set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_mn_grad_tracking\', \'P\', \'ETL Load Successful\')');
           
            prepare sql_string from @sql_string;
            execute sql_string;
            deallocate prepare sql_string;
                
        end if;
    
    end if;
    
end proc;
//
