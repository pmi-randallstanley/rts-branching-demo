
/*
$Rev: 7380 $ 
$Author: randall.stanley $ 
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_color_lship_dynamics.sql $
$Id: etl_color_lship_dynamics.sql 7380 2009-07-16 14:23:58Z randall.stanley $ 
 */


####################################################################
#  loads colors in the l_color_dynamics
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_color_lship_dynamics//

CREATE definer=`dbadmin`@`localhost` procedure etl_color_lship_dynamics()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7380 $ $Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $'

PROC: BEGIN 


  call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);
  
  SELECT  COUNT(*) 
  INTO    @view_color
  FROM    information_schema.tables t
  WHERE   t.table_schema = @db_name_core
  AND     t.table_name = 'v_pmi_ods_color_lship_dynamics';

  IF @view_color > 0 THEN

    SELECT  COUNT(*) 
    INTO    @view_count
    FROM    v_pmi_ods_color_lship_dynamics;
    
    SELECT  COUNT(*) 
    INTO    @table_count
    FROM    l_color_dynamics;

    IF @view_count > 0 THEN

      truncate TABLE l_color_dynamics;
      
      INSERT INTO l_color_dynamics (color_id, ayp_subject_id, score_type_code, begin_year, end_year, min_score, max_score, last_user_id,create_timestamp)
      SELECT  pc.color_id
        ,m.ayp_subject_id
        ,c.score_type_code
        ,c.begin_year
        ,c.end_year
        ,c.min_value
        ,c.max_value
        ,1234
        ,current_timestamp
      FROM v_pmi_ods_color_lship_dynamics as c
        INNER JOIN c_ayp_subject as m
          ON c.ayp_subject_code = m.ayp_subject_code
        INNER JOIN pmi_color as pc
          ON pc.moniker = c.color_name

      ON DUPLICATE key UPDATE last_user_id = 1234, min_score = c.min_value, max_score = c.max_value,last_edit_timestamp = current_timestamp;
      
      SET @sql_scan_log := '';
      SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_color_lship_dynamics', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
  
      -- select @sql_scan_log;
  
      prepare sql_scan_log from @sql_scan_log;
      execute sql_scan_log;
      deallocate prepare sql_scan_log;
      

    END IF;
    
  END IF;
  
END 
//
