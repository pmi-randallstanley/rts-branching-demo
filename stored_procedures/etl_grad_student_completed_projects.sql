/*
$Rev: 7973 $ 
$Author: randall.stanley $ 
$Date: 2009-12-09 08:03:30 -0500 (Wed, 09 Dec 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_grad_student_completed_projects.sql $
$Id: etl_grad_student_completed_projects.sql 7973 2009-12-09 13:03:30Z randall.stanley $ 
*/

DROP PROCEDURE IF EXISTS etl_grad_student_completed_projects //

create definer=`dbadmin`@`localhost` procedure etl_grad_student_completed_projects()
contains sql
sql security invoker
comment '$Rev: 7973 $ $Date: 2009-12-09 08:03:30 -0500 (Wed, 09 Dec 2009) $'

proc: begin

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*) 
    into    @view_data
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_grad_stu_comp_proj';
    
    if @view_data > 0 then

        select  coalesce(count(*),0)
        into    @record_count
        from    v_pmi_ods_grad_stu_comp_proj;
        
        if @record_count > 0 Then

            #### restructure source data   ####      

            drop table if exists tmp_grad_stu_comp_proj;

            create table tmp_grad_stu_comp_proj (
                `student_sis_code` int(11) not null
                ,`ayp_subject_code` varchar(20) not null
                ,comp_projects smallint(3)
                ,primary key (student_sis_code, ayp_subject_code)
                );
        
            insert into tmp_grad_stu_comp_proj (
                student_sis_code
                ,ayp_subject_code
                ,comp_projects
            )
            
            select ods.student_sis_code
                , ods.ayp_subject_code
                , ods.comp_projects
            from (
                select student_sis_code
                    ,'hsaAlgebra' as ayp_subject_code
                    ,hsa_algebra_comp_projects as comp_projects
                from v_pmi_ods_grad_stu_comp_proj
                where hsa_algebra_comp_projects is not null
                
                union all
                
                select student_sis_code
                    ,'hsaBiology' as ayp_subject_code
                    ,hsa_biology_comp_projects as comp_projects
                from v_pmi_ods_grad_stu_comp_proj
                where hsa_biology_comp_projects is not null
                
                union all
                
                select student_sis_code
                    ,'hsaEnglish' as ayp_subject_code
                    ,hsa_english_comp_projects as comp_projects
                from v_pmi_ods_grad_stu_comp_proj
                where hsa_english_comp_projects is not null
                
                union all
                
                select student_sis_code
                    ,'hsaGovernment' as ayp_subject_code
                    ,hsa_government_comp_projects as comp_projects
                from v_pmi_ods_grad_stu_comp_proj
                where hsa_government_comp_projects is not null
            ) as ods
            on duplicate key update comp_projects = values(comp_projects)
            ;


            insert into c_grad_subject_student_projects (
                student_id
                ,ayp_subject_id
                ,projects_required
                ,projects_completed
                ,last_user_id
                ,create_timestamp
            )
            
            select  student_id
                ,ayp_subject_id
                ,min_num_proj
                ,max_comp_projects
                ,1234 as last_user_id
                ,current_timestamp as create_timestamp
            from (

                select substu.student_id
                    ,substu.ayp_subject_id
                    ,min(coalesce(num_projects,0)) as min_num_proj
                    ,count(1) as test_taken
                    ,max(substu.ayp_score)
                    ,coalesce(max(scp.comp_projects),0) as max_comp_projects
                from c_ayp_subject_student substu
                join c_ayp_subject  sub
                    on  substu.ayp_subject_id = sub.ayp_subject_id
                join c_student stu
                    on  substu.student_id = stu.student_id
                left join v_pmi_ods_grad_sub_req_proj ods
                    on  sub.ayp_subject_code = ods.ayp_subject_code
                    and (substu.school_year_id between ods.begin_year and ods.end_year)
                    and (substu.ayp_score between ods.min_score and ods.max_score)
                left join tmp_grad_stu_comp_proj scp
                        on  sub.ayp_subject_code = scp.ayp_subject_code
                        and stu.student_code = scp.student_sis_code
                        
                where sub.grad_report_flag = 1
                group by substu.student_id
                    ,substu.ayp_subject_id
                having count(*) > 1
                and min(coalesce(num_projects,0)) > 0

            ) base
            
            on duplicate key update projects_required = values(projects_required)
                ,projects_completed = values(projects_completed)
            ;
           
            # Cleanup and post processing
    
            #Update imp_upload_log
            
            set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_grad_stu_comp_proj\', \'P\', \'ETL Load Successful\')');
           
            prepare sql_string from @sql_string;
            execute sql_string;
            deallocate prepare sql_string;
                
        end if;
    
    end if;
    
end proc;
//
