/*
$Rev: 7380 $ 
$Author: randall.stanley $ 
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_color_idel.sql $
$Id: etl_color_idel.sql 7380 2009-07-16 14:23:58Z randall.stanley $ 
 */


DROP PROCEDURE IF EXISTS etl_pm_color_idel //
####################################################################
# Insert idel color data # 
####################################################################
 /* List of test measures...
    ? fsi:    Initial Sounds Fluency (isf)
    + fnl:    Letter Naming Fluency (lnf)
    + fsftlp: Phoneme Segmentation Fluency (psf)
    + fpstsl: Nonsense Word Fluency (nwf)
    + flo:    DIBELS Oral Reading Fluency (orf)
    * fro:    Retell Fluency (rtf)
    * fup:    Word Use Fluency (fup)
 */


CREATE definer=`dbadmin`@`localhost` procedure etl_pm_color_idel()
BEGIN 

  call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);
  
  SELECT  COUNT(*) 
  INTO    @view_color
  FROM    information_schema.tables t
  WHERE   t.table_schema = @db_name_core
  AND     t.table_name = 'v_pmi_ods_color_idel';

  IF @view_color > 0 THEN

    SELECT  COUNT(*) 
    INTO    @view_count
    FROM    v_pmi_ods_color_idel;
    
    SELECT  COUNT(*) 
    INTO    @table_count
    FROM    pm_color_idel;

    IF @view_count > 0 THEN

      truncate TABLE pm_color_idel;
      
      INSERT INTO pm_color_idel (measure_period_id,color_id,min_score,max_score,client_id,last_user_id,create_timestamp)
      SELECT mp.measure_period_id
        ,pc.color_id
        ,c.min_score
        ,c.max_score
        ,@client_id
        ,1234
        ,current_timestamp
      FROM v_pmi_ods_color_idel as c
        INNER JOIN pm_idel_assess_freq as f
          ON c.freq_code = f.freq_code
        INNER JOIN pm_idel_measure as m
          ON c.measure_code = m.measure_code
        INNER JOIN pm_idel_assess_freq_period as p
          ON c.period_code = p.period_code
          AND f.freq_id = p.freq_id
        INNER JOIN c_grade_level gl
          ON c.grade_code = gl.grade_code
        INNER JOIN pmi_color as pc
          ON pc.moniker = c.color_name
        INNER JOIN pm_idel_measure_period as mp
          ON  mp.measure_id     = m.measure_id
          AND mp.freq_id        = p.freq_id 
          AND mp.period_id      = p.period_id
          AND mp.grade_level_id = gl.grade_level_id
      ON DUPLICATE key UPDATE last_user_id = 1234, min_score = c.min_score, max_score = c.max_score,last_edit_timestamp = current_timestamp;
      
      SET @sql_scan_log := '';
      SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_color_idel', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
  
      -- select @sql_scan_log;
  
      prepare sql_scan_log from @sql_scan_log;
      execute sql_scan_log;
      deallocate prepare sql_scan_log;
      
        -- update score colors
        call etl_rpt_idel_scores_color_update();

    END IF;
    
  END IF;
  
END 
//
