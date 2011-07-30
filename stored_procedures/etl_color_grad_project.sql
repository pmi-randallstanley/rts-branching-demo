/*
$Rev: 8471 $ 
$Author: randall.stanley $ 
$Date: 2010-04-29 16:00:11 -0400 (Thu, 29 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_color_grad_project.sql $
$Id: etl_color_grad_project.sql 8471 2010-04-29 20:00:11Z randall.stanley $ 
 */

drop procedure if exists etl_color_grad_project //
####################################################################
# Insert c_color_grad_projects data # 
####################################################################
# call etl_color_grad_project();
# select * from c_color_grad_projects limit 10;

create definer=`dbadmin`@`localhost` procedure etl_color_grad_project()
contains sql
sql security invoker
comment '$Rev: 8471 $ $Date: 2010-04-29 16:00:11 -0400 (Thu, 29 Apr 2010) $'

begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
  
    select  count(*) 
    into    @view_color
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_color_grad_proj';
    
    
    if @view_color > 0 then

        select  count(*) 
        into    @view_count
        from    v_pmi_ods_color_grad_proj;
        
        select  count(*) 
        into    @table_count
        from    c_color_grad_projects;
        
        if @view_count > 0 then

            truncate TABLE c_color_grad_projects;
            
            INSERT c_color_grad_projects (
                ayp_subject_id
                ,color_id
                ,min_projects
                ,max_projects
                ,last_user_id
                ,create_timestamp
            )
                
            SELECT  coalesce(sub.ayp_subject_id,0)
                ,c.color_id
                ,os.min_projects
                ,os.max_projects
                ,1234 as last_user_id
                ,current_timestamp as create_timestamp
            
            FROM        v_pmi_ods_color_grad_proj os
            left JOIN   c_ayp_subject sub
                        ON sub.ayp_subject_code = os.ayp_subject_code
            JOIN        pmi_color c
                        ON c.moniker = os.color_name
            
            ON DUPLICATE key UPDATE min_projects = values(min_projects),
                        max_projects = values(max_projects),
                        last_user_id = values(last_user_id);
 
 
            delete  csl.*
            from    c_color_swatch_list as csl
            join    c_color_swatch as cs
                    on      csl.swatch_id = cs.swatch_id
                    and     cs.swatch_code = 'gradProjects'
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
                        select  cs.swatch_id, c.color_id, min_projects
                        from    pmi_color as c
                        join    c_color_grad_projects as csrc
                                on      c.color_id = csrc.color_id
                        join    c_color_swatch as cs
                                on      cs.swatch_code = 'gradProjects'
                        where   c.active_flag = 1
                        group by cs.swatch_id, c.color_id
                    ) as dt
            order by dt.min_projects, dt.color_id
            on duplicate key update last_user_id = values(last_user_id)
            ;
            

            #Clean-up and post processing
     
            set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_color_grad_proj\', \'P\', \'ETL Load Successful\')');
            prepare sql_scan_log from @sql_string;
            execute sql_scan_log;
            deallocate prepare sql_scan_log;

        end if;
    
    end if;
  
  
end 
//
