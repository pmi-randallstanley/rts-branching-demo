/*
$Rev: 7380 $ 
$Author: randall.stanley $ 
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_color_l_matrix.sql $
$Id: etl_color_l_matrix.sql 7380 2009-07-16 14:23:58Z randall.stanley $ 
 */

drop procedure if exists etl_color_l_matrix //

create definer=`dbadmin`@`localhost` procedure etl_color_l_matrix()
contains sql
sql security invoker
comment '$Rev: 7380 $ $Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $'
begin

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);
    
    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_color_matrix';

    if @view_exists > 0 then
  
        select count(*)
        into @view_count
        from v_pmi_ods_color_matrix;
    
        if @view_count > 0 then 
  
            truncate TABLE l_color_matrix;
        
            insert l_color_matrix (
                ayp_subject_id
                ,grouping_id
                ,measure_id
                ,color_id
                ,begin_year
                ,begin_grade_sequence
                ,end_year
                ,end_grade_sequence
                ,min_range
                ,max_range
                ,last_user_id
                ,last_edit_timestamp
              )
            select  sub.ayp_subject_id
                ,mg.grouping_id
                ,mm.measure_id
                ,c.color_id
                ,os.begin_year
                ,os.begin_grade_sequence
                ,os.end_year
                ,os.end_grade_sequence
                ,os.min_range
                ,os.max_range
                ,1234
                ,now()
                
            from    v_pmi_ods_color_matrix as os
            join    c_ayp_subject as sub
                    on      sub.moniker = os.ayp_subject_code
            join    pmi_color as c
                    on      c.moniker = os.color
            join    l_matrix_measure as mm
                    on      mm.moniker = os.measure
            join    l_matrix_grouping as mg
                    on      mg.moniker = os.grouping
            on duplicate key update last_user_id = values(last_user_id)
                ,end_year = values(end_year)
                ,end_grade_sequence = values(end_grade_sequence)
                ,min_range = values(min_range)
                ,max_range= values(max_range)
            ;

     
            #################
            ## update log
            #################
                
            set @sql_scan_log := '';
            set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_color_matrix', '\',', '\'', 'p', '\',', '\'', 'etl load successful', '\')');
        
            prepare sql_scan_log from @sql_scan_log;
            execute sql_scan_log;
            deallocate prepare sql_scan_log;
            
        end if;  # end if rows in view
         
  end if; # end if view exisits

end;
//
