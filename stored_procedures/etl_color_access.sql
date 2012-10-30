
/*
$Rev: 8471 $ 
$Author: randall.stanley $ 
$Date: 2010-04-29 16:00:11 -0400 (Thu, 29 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_color_access.sql $
$Id: etl_color_access.sql 8471 2010-04-29 20:00:11Z randall.stanley $ 
*/

drop procedure if exists etl_color_access//

create definer=`dbadmin`@`localhost` procedure etl_color_access()
contains sql
sql security invoker
comment '$Rev: 8471 $ $Date: 2010-04-29 16:00:11 -0400 (Thu, 29 Apr 2010) $'


proc: begin 

    declare v_view_color smallint(4);
    declare v_view_count smallint(4);

 
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
  
    select  count(*) 
    into    v_view_color
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_color_access';
    
    
    if v_view_color > 0 then

        select  count(*) 
        into    v_view_count
        from    v_pmi_ods_color_access;
                
        if v_view_count > 0 then

            truncate TABLE pm_color_access;
            
            INSERT pm_color_access (
                color_id
                ,begin_year
                ,end_year
                ,begin_grade_sequence
                ,end_grade_sequence
                ,min_score
                ,max_score
                ,last_user_id
                ,create_timestamp
            )
                
            SELECT 
                c.color_id
                ,begin_year
                ,end_year
                ,begin_grade_sequence
                ,end_grade_sequence
                ,min_score
                ,max_score
                ,1234 as last_user_id
                ,now() as create_timestamp
            
            FROM        v_pmi_ods_color_access as os
            JOIN        pmi_color as c
                        ON c.moniker = os.color_name
            
            ON DUPLICATE key UPDATE min_score = values(min_score),
                        max_score = values(max_score),
                        last_user_id = values(last_user_id);
 
 
            delete  csl.*
            from    c_color_swatch_list as csl
            join    c_color_swatch as cs
                    on      csl.swatch_id = cs.swatch_id
                    and     cs.swatch_code = 'access'
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
                        select  cs.swatch_id, c.color_id, min_score
                        from    pmi_color as c
                        join    pm_color_access as csrc
                                on      c.color_id = csrc.color_id
                        join    c_color_swatch as cs
                                on      cs.swatch_code = 'access'
                        where   c.active_flag = 1
                        group by cs.swatch_id, c.color_id
                    ) as dt
            order by dt.min_score, dt.color_id
            on duplicate key update last_user_id = values(last_user_id)
            ;
        
            ## New Color File means we should update bbcard (BBCv2 tables)
            ## Only update proficiency level scores
            update rpt_bbcard_detail_access as rpt
            join c_student_year as sy
                    on rpt.student_id = sy.student_id
                    and rpt.school_year_id = sy.school_year_id
            join c_grade_level as g
                    on sy.grade_level_id = g.grade_level_id
            join pm_bbcard_measure_item mi
                    on  rpt.bb_group_id = mi.bb_group_id
                    and rpt.bb_measure_id = mi.bb_measure_id
                    and rpt.bb_measure_item_id = mi.bb_measure_item_id
                    and mi.bb_measure_item_code like '%Prof%'
            join pm_color_access as c
                    on sy.school_year_id between c.begin_year and c.end_year
                    and g.grade_sequence between c.begin_grade_sequence and c.end_grade_sequence
                    and rpt.score between c.min_score and c.max_score
            join pmi_color as pmic
                  on c.color_id = pmic.color_id
            set score_color = pmic.moniker
            ;    
            
            #### Update imp_upload_log
            set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_color_access\', \'P\', \'ETL Load Successful\')');
            
            prepare sql_string from @sql_string;
            execute sql_string;
            deallocate prepare sql_string; 

        else
            #### Update imp_upload_log
            set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_color_access\', \'F\', \'No rows in source file.\')');
            
            prepare sql_string from @sql_string;
            execute sql_string;
            deallocate prepare sql_string; 

        end if;
    
    end if;
    
end proc;
//