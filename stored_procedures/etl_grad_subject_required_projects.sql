/*
$Rev: 7973 $ 
$Author: randall.stanley $ 
$Date: 2009-12-09 08:03:30 -0500 (Wed, 09 Dec 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_grad_subject_required_projects.sql $
$Id: etl_grad_subject_required_projects.sql 7973 2009-12-09 13:03:30Z randall.stanley $ 
*/

DROP PROCEDURE IF EXISTS etl_grad_subject_required_projects //

create definer=`dbadmin`@`localhost` procedure etl_grad_subject_required_projects()
contains sql
sql security invoker
comment '$Rev: 7973 $ $Date: 2009-12-09 08:03:30 -0500 (Wed, 09 Dec 2009) $'

proc: begin

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
  
    select  count(*) 
    into    @view_data
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_grad_sub_req_proj';
    
    if @view_data > 0 then

        select coalesce(count(1),0) 
        into @record_count
        from v_pmi_ods_color_grad_status;
        
        if @record_count > 0 Then
        
            truncate table c_grad_subject_required_projects;
        
            insert into c_grad_subject_required_projects (
                ayp_subject_id, num_projects, 
                begin_year, end_year, 
                min_score, max_score, 
                last_user_id, 
                create_timestamp
            )
            
            select c.ayp_subject_id
                ,v.num_projects
                ,v.begin_year
                ,v.end_year
                ,v.min_score
                ,v.max_score
                ,1234 as last_user_id
                ,current_timestamp as create_timestamp
            from v_pmi_ods_grad_sub_req_proj v
            join    c_ayp_subject c
                    on      v.ayp_subject_code = c.ayp_subject_code
                    and     c.ayp_subject_code != 'hsaGovernment'   ########### Fix for transition away from HSA Government
            ;
        
            #Cleanup and post processing
        
            #Update imp_upload_log
            
            set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_grad_sub_req_proj\', \'P\', \'ETL Load Successful\')');
                       
            prepare sql_string from @sql_string;
            execute sql_string;
            deallocate prepare sql_string;
        
        end if;
    
    end if;
    
end proc;
//
