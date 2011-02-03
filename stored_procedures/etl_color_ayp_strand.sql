/*
$Rev: 8471 $ 
$Author: randall.stanley $ 
$Date: 2010-04-29 16:00:11 -0400 (Thu, 29 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_color_ayp_strand.sql $
$Id: etl_color_ayp_strand.sql 8471 2010-04-29 20:00:11Z randall.stanley $ 
 */


DROP PROCEDURE IF EXISTS etl_color_ayp_strand //
####################################################################
# Insert ayp strand color data # 
####################################################################


create definer=`dbadmin`@`localhost` procedure etl_color_ayp_strand()
contains sql
sql security invoker
comment '$Rev: 8471 $ $Date: 2010-04-29 16:00:11 -0400 (Thu, 29 Apr 2010) $'
begin

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);
  
    select  count(*) 
    into    @view_color
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_color_ayp_strand';

    if @view_color > 0 then

        select  count(*) 
        into    @view_count
        from    v_pmi_ods_color_ayp_strand;
        
        select  count(*) 
        into    @table_count
        from    c_color_ayp_strand;

        if @view_count > 0 then
    
            truncate TABLE c_color_ayp_strand;
        
            INSERT c_color_ayp_strand (ayp_subject_id, ayp_strand_id, color_id, begin_year, begin_grade_sequence, end_year, end_grade_sequence, min_score, max_score, last_user_id)
            SELECT     sub.ayp_subject_id, 
                    s.ayp_strand_id,
                    c.color_id,
                    os.begin_year,
                    os.begin_grade_sequence,
                    os.end_year,
                    os.end_grade_sequence,
                    os.min_score,
                    os.max_score,
                    1234          
            FROM v_pmi_ods_color_ayp_strand os
            JOIN  c_ayp_subject sub
              ON    sub.ayp_subject_code = os.ayp_subject_code
            JOIN  c_ayp_strand s
              ON    sub.ayp_subject_id = s.ayp_subject_id
              AND    s.ayp_strand_code = os.ayp_strand_code
            JOIN  pmi_color c
              ON    c.moniker = os.color
            ON DUPLICATE key UPDATE last_user_id = 1234, min_score = os.min_score, max_score= os.max_score;
    
            delete  csl.*
            from    c_color_swatch_list as csl
            join    c_color_swatch as cs
                    on      csl.swatch_id = cs.swatch_id
                    and     cs.swatch_code = 'aypstrand'
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
                        select  cs.swatch_id, c.color_id, min(csrc.min_score) as min_score
                        from    pmi_color as c
                        join    c_color_ayp_strand as csrc
                                on      c.color_id = csrc.color_id
                        cross join  c_color_swatch as cs
                                on      cs.swatch_code = 'aypstrand'
                        where   c.active_flag = 1
                        group by cs.swatch_id, c.color_id
                    ) as dt
            order by dt.min_score, dt.color_id
            on duplicate key update last_user_id = 1234
            ;
                
            SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_color_ayp_strand', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
            
            -- select @sql_scan_log;
            
            prepare sql_scan_log from @sql_scan_log;
            execute sql_scan_log;
            deallocate prepare sql_scan_log;
      
        end if;
    
    end if;
  
end 
//
