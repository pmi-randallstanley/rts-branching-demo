/*
$Rev: 8019 $ 
$Author: randall.stanley $ 
$Date: 2009-12-14 15:18:55 -0500 (Mon, 14 Dec 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_color_ayp_tchr_perf_pass_pct.sql $
$Id: etl_color_ayp_tchr_perf_pass_pct.sql 8019 2009-12-14 20:18:55Z randall.stanley $ 
 */

drop procedure if exists etl_color_ayp_tchr_perf_pass_pct//

create definer=`dbadmin`@`localhost` procedure etl_color_ayp_tchr_perf_pass_pct()
contains sql
sql security invoker
comment '$Rev: 8019 $ $Date: 2009-12-14 15:18:55 -0500 (Mon, 14 Dec 2009) $'


proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);
    
    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name
    and     t.table_name = 'v_pmi_ods_color_tchr_perf_pass_pct';

    if @view_exists > 0 then
    
        select  count(*) 
        into    @view_count
        from    v_pmi_ods_color_tchr_perf_pass_pct;
        
        if @view_count > 0 then
    
            truncate table c_color_ayp_tchr_perf_pass_pct;
            
            insert  c_color_ayp_tchr_perf_pass_pct (
                ayp_subject_id
                ,color_id
                ,min_score
                ,max_score
                ,last_user_id
                ,create_timestamp
            )
            
            select  sub.ayp_subject_id
                    ,clr.color_id
                    ,ods.min_score
                    ,ods.max_score
                    ,1234
                    ,now()
                    
            from    v_pmi_ods_color_tchr_perf_pass_pct as ods
            join    c_ayp_subject as sub
                    on      ods.state_subject_code = sub.ayp_subject_code
            join    pmi_color as clr
                    on      ods.color_name = clr.moniker
            ;

            -- Update imp_upload_log
            set @sql_string := '';
            set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_color_tchr_perf_pass_pct', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
            
            prepare sql_string from @sql_string;
            execute sql_string;
            deallocate prepare sql_string;     
            
        end if;

    end if;
    
end proc;
//
