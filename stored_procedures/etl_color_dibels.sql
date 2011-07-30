/*
$Rev: 8471 $ 
$Author: randall.stanley $ 
$Date: 2010-04-29 16:00:11 -0400 (Thu, 29 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_color_dibels.sql $
$Id: etl_color_dibels.sql 8471 2010-04-29 20:00:11Z randall.stanley $ 
 */

drop procedure if exists etl_color_dibels //

create definer=`dbadmin`@`localhost` procedure etl_color_dibels()
contains sql
sql security invoker
comment '$Rev: 8471 $ $Date: 2010-04-29 16:00:11 -0400 (Thu, 29 Apr 2010) $'
begin

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_color_dibels';

    if @view_exists > 0 then
  
        select count(*)
        into @view_count
        from v_pmi_ods_color_dibels;
    
        if @view_count > 0 then 
  
            truncate TABLE pm_color_dibels;
        
            insert  pm_color_dibels (
                measure_period_id
                ,color_id
                ,min_score
                ,max_score
                ,client_id
                ,last_user_id
                ,create_timestamp
            )
            select  dmp.measure_period_id
                ,clr.color_id
                ,ods.min_score
                ,ods.max_score
                ,@client_id
                ,1234
                ,now()
                
            from    v_pmi_ods_color_dibels as ods
            join    pm_dibels_assess_freq as af
                    on      ods.freq_code = af.freq_code
            join    pm_dibels_assess_freq_period as afp
                    on      af.freq_id = afp.freq_id
                    and     ods.period_code = afp.period_code
            join    pm_dibels_measure as dm
                    on      ods.measure_code = dm.measure_code
            join    v_pmi_xref_grade_level as xgl
                    on      ods.grade_code = xgl.client_grade_code
            join    c_grade_level as gl
                    on      xgl.pmi_grade_code = gl.grade_code
            join    pm_dibels_measure_period as dmp
                    on      dm.measure_id = dmp.measure_id
                    and     afp.freq_id = dmp.freq_id
                    and     afp.period_id = dmp.period_id
                    and     gl.grade_level_id = dmp.grade_level_id
            join    pmi_color as clr
                    on      ods.color = clr.moniker
            on duplicate key update last_user_id = values(last_user_id)
                ,min_score = values(min_score)
                ,max_score = values(max_score)
            ;

            delete  csl.*
            from    c_color_swatch_list as csl
            join    c_color_swatch as cs
                    on      csl.swatch_id = cs.swatch_id
                    and     cs.swatch_code = 'dibels'
            ;
    
            set @rownum = 0;
            insert into c_color_swatch_list (
                swatch_id
                ,client_id
                , color_id
                , sort_order
                , last_user_id
                , create_timestamp
                , last_edit_timestamp
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
                        join    pm_color_dibels as csrc
                                on      c.color_id = csrc.color_id
                        cross join  c_color_swatch as cs
                                on      cs.swatch_code = 'dibels'
                        where   c.active_flag = 1
                        group by cs.swatch_id, c.color_id
                    ) as dt
            order by dt.min_score, dt.color_id
            on duplicate key update last_user_id = 1234
            ;

     
            #################
            ## update log
            #################
                
            set @sql_scan_log := '';
            set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_color_dibels', '\',', '\'', 'p', '\',', '\'', 'etl load successful', '\')');
        
            prepare sql_scan_log from @sql_scan_log;
            execute sql_scan_log;
            deallocate prepare sql_scan_log;
            
        end if;  # end if rows in view
         
  end if; # end if view exisits

end;
//
