drop procedure if exists etl_hst_fl//

CREATE DEFINER=`dbadmin`@`localhost` PROCEDURE `etl_hst_fl`()
    SQL SECURITY INVOKER
BEGIN

        DECLARE v_data_change                   tinyint(1) default '0';
        DECLARE v_etl_rpt_flag                  tinyint(1) default '0';
        DECLARE v_etl_bm_build_flag             tinyint(1) default '0';
        DECLARE v_etl_pm_flag                   tinyint(1) default '0';
        DECLARE v_etl_grad_rpt_flag             tinyint(1) default '0';
        DECLARE v_etl_baseball_rebuild_flag     tinyint(1) default '0';
        DECLARE v_etl_lag_color_update_flag     tinyint(1) default '0';
        DECLARE v_etl_exec_stu_cust_fltr_flag   tinyint(1) default '0';
        DECLARE v_fcat_flag                     tinyint(1) default '0';
        DECLARE v_no_more_rows                  boolean;
        DECLARE v_test_type_moniker             varchar(100);
        DECLARE v_process_test_type             int(10);

        
        declare v_test_type_cursor cursor for
                select distinct test_type_moniker
                from tmp_test_type;
       
        declare continue handler for not found 
        set v_no_more_rows = true;

        call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

        SELECT pmi_admin.pmi_f_get_next_sequence('etl_imp_id', 1) INTO @etl_imp_id;
        SET @begin_time := now();
        SELECT convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
        SET v_etl_rpt_flag = 0;
        SET v_etl_bm_build_flag = 0;
        INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
        SELECT @etl_imp_id, @client_id, 'etl_hst_fl', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
        drop table if exists `tmp_test_type`;
            
        CREATE TABLE `tmp_test_type` (
          `test_type_moniker` varchar(100) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
               
        

        INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
       
        SELECT @etl_imp_id, @client_id, 'imp_process_upload_log()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
        select concat(database(), '_ods') into @db;
    
        SET @sqltext := CONCAT('call ', @db, '.imp_process_upload_log()');
    
            prepare sqltext from @sqltext;
                
                execute sqltext;
                    
                deallocate prepare sqltext;  
                
        INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
        SELECT @etl_imp_id, @client_id, 'imp_process_upload_log()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;        
            
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_ayp_strand');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'AYP Strand Color Data - New File' AS Uploader_Color_ayp_strand, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_ayp_strand()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_color_ayp_strand();
                        SET v_etl_rpt_flag = 1;
                        set v_etl_lag_color_update_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_ayp_strand()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New AYP Strand Color File' AS Uploader_Color_ayp_strand, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No AYP Strand Color File' AS Uploader_Color_ayp_strand;
        END IF; 

        
        
        
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_ayp_subject');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'AYP Subject Color Data - New File' AS Uploader_Color_ayp_subject, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_ayp_subject()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_color_ayp_subject();
                        SET v_etl_rpt_flag = 1;
                        set v_etl_lag_color_update_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_ayp_subject()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New AYP Subject Color File' AS Uploader_Color_ayp_subject, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No AYP Subject Color File' AS Uploader_Color_ayp_subject;
        END IF; 
        
        
        
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_fl_eoc_algebra_1');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New FL EOC Algebra 1 record' AS Uploader_FL_EOC_Algebra1, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_eoc_algebra_1()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                    call etl_hst_load_fl_eoc_algebra_1();
                    
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code = 'fleocAlgebra1'
                    ;
                    
                    
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_eoc_algebra1_log := '';
                    SET @sql_eoc_algebra1_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_fl_eoc_algebra_1', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_eoc_algebra1_log from @sql_eoc_algebra1_log;
                                
                        execute sql_eoc_algebra1_log;
                            
                        deallocate prepare sql_eoc_algebra1_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_eoc_algebra_1()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New FL EOC Algebra 1  record' AS Uploader_FL_EOC_Algebra1, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new FL EOC Algebra 1 record' AS Uploader_FL_EOC_Algebra1;
        END IF;
        
        

        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_fcat_math_reading');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New fcat Math Reading record' AS Uploader_fcat_math_reading, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_fcat_math_reading()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                    call etl_hst_load_fl_fcat_math_reading();
                    
                    SET v_fcat_flag = 1;
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code = 'fcatMath'
                    ;
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_math_reading_log := '';
                    SET @sql_math_reading_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_fl_fcat_math_reading', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_math_reading_log from @sql_math_reading_log;
                                
                        execute sql_math_reading_log;
                            
                        deallocate prepare sql_math_reading_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_fcat_math_reading()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New fcat Math Reading record' AS Uploader_fcat_math_reading, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new fcat Math Reading record' AS Uploader_fcat_math_reading;
        END IF;
        

        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_fcat_math_reading_20');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New fcat Math Reading_20 record' AS Uploader_fcat_math_reading_20, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_fcat_math_reading_20()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                    call etl_hst_load_fl_fcat_math_reading_20();
                    
                    SET v_fcat_flag = 1;
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code = 'fcatMath'
                    ;                    
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_math_reading_20_log := '';
                    SET @sql_math_reading_20_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_fl_fcat_math_reading_20', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_math_reading_20_log from @sql_math_reading_20_log;
                                
                        execute sql_math_reading_20_log;
                            
                        deallocate prepare sql_math_reading_20_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_fcat_math_reading_20()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New fcat Math Reading_20 record' AS Uploader_fcat_math_reading_20, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new fcat Math Reading_20 record' AS Uploader_fcat_math_reading_20;
        END IF;
        
        
        
        
        
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_fcat_science_20');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New fcat Science_20 record' AS Uploader_fcat_science_20, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fcat_science_20()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                    call etl_hst_load_fl_fcat_science_20();
                    
                    SET v_fcat_flag = 1;

                    Insert into tmp_test_type(test_type_moniker)
                         select tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code = 'fcatScience'
                    ;
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_science_20_log := '';
                    SET @sql_science_20_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_fl_fcat_science_20', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_science_20_log from @sql_science_20_log;
                                
                        execute sql_science_20_log;
                            
                        deallocate prepare sql_science_20_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_fcat_science_20()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New fcat Science_20 record' AS Uploader_fcat_Science_20, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new fcat Science_20 record' AS Uploader_fcat_science_20;
        END IF;
        

        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_fcat_science');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New fcat Science record' AS Uploader_fcat_science, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_fcat_science()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                    call etl_hst_load_fl_fcat_science();

                    SET v_fcat_flag = 1;
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code = 'fcatScience'
                    ;

                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_science_log := '';
                    SET @sql_science_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_fl_fcat_science', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_science_log from @sql_science_log;
                                
                        execute sql_science_log;
                            
                        deallocate prepare sql_science_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_fcat_science()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New fcat Science record' AS Uploader_fcat_Science, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new fcat Science record' AS Uploader_fcat_science;
        END IF;

        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_fl_eoc_biology');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New fl eoc biology record' AS Uploader_fl_eoc_biology, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_eoc_biology()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                    call etl_hst_load_fl_eoc_biology();
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code = 'fleocBiology'
                    ;

                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_biology_log := '';
                    SET @sql_biology_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_fl_eoc_biology', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_biology_log from @sql_biology_log;
                                
                        execute sql_biology_log;
                            
                        deallocate prepare sql_biology_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_eoc_biology()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New fl eoc biology record' AS Uploader_fl_eoc_biology, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new fl eoc biology record' AS Uploader_fl_eoc_biology;
        END IF;
        

        
  
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_fl_eoc_geometry');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New fl eoc geometry record' AS Uploader_fl_eoc_geometry, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_eoc_geometry()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                    call etl_hst_load_fl_eoc_geometry();
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code = 'fleocGeometry'
                    ;

                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_geometry_log := '';
                    SET @sql_geometry_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_fl_eoc_geometry', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_geometry_log from @sql_geometry_log;
                                
                        execute sql_geometry_log;
                            
                        deallocate prepare sql_geometry_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_eoc_geometry()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New fl eoc geometry record' AS Uploader_fl_eoc_geometry, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new fl eoc geometry record' AS Uploader_fl_eoc_geometry;
        END IF;
        
        
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_fcat_writing');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New fcat writing record' AS Uploader_fcat_writing, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_fcat_writing()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                    call etl_hst_load_fl_fcat_writing();
                    
                    SET v_fcat_flag = 1;
 
                    Insert into tmp_test_type(test_type_moniker)
                         select tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code = 'fcatWriting'
                    ;
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_writing_log := '';
                    SET @sql_writing_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_fl_fcat_writing', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_writing_log from @sql_writing_log;
                                
                        execute sql_writing_log;
                            
                        deallocate prepare sql_writing_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_fcat_writing()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New fcat writing record' AS Uploader_fcat_writing, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new fcat writing record' AS Uploader_fcat_writing;
        END IF;
    
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_fl_pert');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New fl Pert record' AS Uploader_fl_pert, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_pert()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                    call etl_hst_load_fl_pert();
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code = 'flpertMath'
                    ;
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_pert_log := '';
                    SET @sql_pert_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_fl_pert', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_writing_log from @sql_writing_log;
                                
                        execute sql_pert_log;
                            
                        deallocate prepare sql_pert_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_pert()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New fl Pert record' AS Uploader_fl_pert, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new fl Pert record' AS Uploader_fl_pert;
        END IF;    
              

        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_fl_faa');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New fl faa record' AS Uploader_fl_faa, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_faa()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                    call etl_hst_load_fl_faa();
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                    where sub.ayp_subject_code = 'faaMath'
                    ;
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_faa_log := '';
                    SET @sql_faa_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_fl_faa', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_writing_log from @sql_writing_log;
                                
                        execute sql_faa_log;
                            
                        deallocate prepare sql_faa_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_fl_faa()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New fl faa record' AS Uploader_fl_faa, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new fl faa record' AS Uploader_fl_faa;
        END IF; 
        
        IF v_fcat_flag = 1
           THEN
               call etl_hst_post_load_fcat20_conversion_fl();               
        END IF;

        select count(*)
        into   v_process_test_type
        from   tmp_test_type
        ;

        IF v_process_test_type > 0  or v_etl_lag_color_update_flag = 1
           THEN
               call etl_c_ayp_subject_student_update_al();
               call etl_hst_post_load_update_color();
               call etl_c_ayp_test_type_year();
        END IF;

              
        
        
        Open v_test_type_cursor;
        
        loop_test_type_cursor: loop

        Fetch v_test_type_cursor 
        into  v_test_type_moniker;
        
        
            if v_no_more_rows then
                close v_test_type_cursor;
                leave loop_test_type_cursor;
            end if;
        
            call etl_c_ayp_sub_stu_upd_sor(v_test_type_moniker);
         
        end loop loop_test_type_cursor;

        set v_no_more_rows = false;
        
       
        SELECT convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        SELECT TIMEDIFF(now(), @begin_time) AS Elapsed_Time;
        INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
        SELECT @etl_imp_id, @client_id, 'etl_hst_fl', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;

        drop table if exists `tmp_test_type`;
                
END//

