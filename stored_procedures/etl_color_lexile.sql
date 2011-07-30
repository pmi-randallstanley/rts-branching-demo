/*
$Rev: 7380 $ 
$Author: randall.stanley $ nei
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_color_lexile.sql $
$Id: etl_color_lexile.sql 7380 2009-07-16 14:23:58Z randall.stanley $ 
 */


DROP PROCEDURE IF EXISTS etl_color_lexile //
####################################################################
# Insert LEXILE color data # 
####################################################################


CREATE definer=`dbadmin`@`localhost` procedure etl_color_lexile()
BEGIN 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
  
  SELECT  COUNT(*) 
  INTO    @view_color
  FROM    information_schema.tables t
  WHERE   t.table_schema = @db_name_core
  AND     t.table_name = 'v_pmi_ods_color_lexile';

  IF @view_color > 0 THEN

    SELECT  COUNT(*) 
    INTO    @view_count
    FROM    v_pmi_ods_color_lexile;
    
    SELECT  COUNT(*) 
    INTO    @table_count
    FROM    pm_color_lexile;

    IF @view_count > 0 THEN

      truncate TABLE pm_color_lexile;
      
      INSERT INTO pm_color_lexile (color_id,begin_year,end_year,begin_grade_sequence,end_grade_sequence,min_score,max_score,last_user_id,create_timestamp)
      SELECT c.color_id,
        os.begin_year,
        os.end_year,
        os.begin_grade_sequence,
        os.end_grade_sequence,
        os.min_score,
        os.max_score,
        1234,
        current_timestamp
      FROM v_pmi_ods_color_lexile os
        JOIN pmi_color c
          ON c.moniker = os.color
          OR c.color_id = os.color
      ON DUPLICATE key UPDATE last_user_id = 1234, min_score = os.min_score, max_score= os.max_score,last_edit_timestamp = current_timestamp;
      
      SET @sql_scan_log := '';
      SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_color_lexile', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
  
      -- select @sql_scan_log;
  
      prepare sql_scan_log from @sql_scan_log;
      execute sql_scan_log;
      deallocate prepare sql_scan_log;

    END IF;
    
  END IF;
  
END 
//
