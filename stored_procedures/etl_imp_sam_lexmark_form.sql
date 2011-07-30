/*
$Rev: 8498 $
$Author: randall.stanley $
$Date: 2010-05-03 08:27:27 -0400 (Mon, 03 May 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_sam_lexmark_form.sql $
$Id: etl_imp_sam_lexmark_form.sql 8498 2010-05-03 12:27:27Z randall.stanley $
 */

drop procedure if exists etl_imp_sam_lexmark_form //
####################################################################
# Insert values into sam_lexmark_form data # 
####################################################################

create definer=`dbadmin`@`localhost` procedure etl_imp_sam_lexmark_form()
contains sql
sql security invoker
comment '$Rev: 8498 $ $Date: 2010-05-03 08:27:27 -0400 (Mon, 03 May 2010) $'

proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    select  count(*) 
    into    @view_exists
    from    information_schema.tables as t
    where   t.table_schema = @db_name
    and     t.table_name = 'v_pmi_ods_sam_lexmark_form'
    ;
          
    if @view_exists > 0 then
    
        #### exec only if records exist
        
        select count(*)
        into @record_count
        from v_pmi_ods_sam_lexmark_form
        ;
        
        if @record_count > 0 then

            drop table if exists tmp_id_assign;
            
            create table tmp_id_assign (
                new_id int(11) not null,
                client_id int(11) not null,
                base_code varchar(50) not null,
                primary key  (`new_id`),
                unique key `uq_tmp_id_assign` (`client_id`,`base_code`)
            );
            
            ####  Obtain a new id only for records that are not already in the target table.
            
            insert tmp_id_assign (
                new_id
                ,client_id
                ,base_code
            )
            
            select  pmi_f_get_next_sequence_app_db('sam_lexmark_form', 1), ods.client_id, ods.form_name
            from    v_pmi_ods_sam_lexmark_form as ods
            left join sam_lexmark_form as tar
                    on      ods.form_name = tar.form_code
            where   ods.client_id is not null
            and     tar.lexmark_form_id is null
            ;      
    
            #### Set active_flag to 0 of it does not exist in current file.
            update sam_lexmark_form as a
            left join v_pmi_ods_sam_lexmark_form as b
                on a.form_code = b.form_name
            set a.active_flag = 0
            where b.form_name is null
            ;
    
            #### insert records or update if they exist.
            insert into sam_lexmark_form (
                client_id
                ,lexmark_form_id
                ,form_code
                ,moniker
                ,sr_total_number
                ,answer_set_code
                ,cr_total_number
                ,max_cr_points
                ,cr_mapped_end_flag
                ,active_flag
                ,last_user_id
                ,create_timestamp 
            )
                
            select ods.client_id
                ,coalesce(tmpid.new_id, tar.lexmark_form_id) as lexmark_form_id
                ,ods.form_name
                ,ods.display_name
                ,cast(ods.sr_total_nbr as unsigned) as sr_total_nbr
                ,ods.bubble_code
                ,cast(ods.cr_total_nbr as unsigned) as cr_total_nbr
                ,cast(ods.max_cr_points as unsigned) as max_cr_points
                ,case when ods.cr_mapped_end_flag = 'n' then 0
                      else 1
                end as cr_mapped_end_flag
                ,1 as active_flag
                ,1234 as last_user_id
                ,now() as create_timestamp
                
            from    v_pmi_ods_sam_lexmark_form as ods
            left join   sam_lexmark_form as tar
                    on      ods.form_name = tar.form_code
            left join   tmp_id_assign tmpid
                    on      ods.form_name = tmpid.base_code
            
            on duplicate key update
                moniker = values(moniker)
                ,sr_total_number = values(sr_total_number)
                ,answer_set_code = values(answer_set_code)
                ,cr_total_number = values(cr_total_number)
                ,max_cr_points = values(max_cr_points)
                ,cr_mapped_end_flag = values(cr_mapped_end_flag)
                ,active_flag = values(active_flag)
                ,last_user_id = values(last_user_id)
                ;
                    
            ####  Cleanup
            drop table if exists tmp_id_assign;
    
            #### Update imp_upload_log
            set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_sam_lexmark_form\', \'P\', \'ETL Load Successful\')');
            
            prepare sql_string from @sql_string;
            execute sql_string;
            deallocate prepare sql_string; 
            
        end if;

    end if; 
    
end proc;
//
