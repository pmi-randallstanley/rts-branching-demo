drop procedure if exists etl_hst_mn//

CREATE DEFINER=`dbadmin`@`localhost` PROCEDURE `etl_hst_mn`()
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
        SELECT @etl_imp_id, @client_id, 'etl_hst_mn', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
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
        
        
        
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_mn_grad_test');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New MN grad test record' AS Uploader_MN_grad_test, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_grad()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;

                    call etl_hst_load_mn_grad();
                    
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select distinct tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code in ('gradMath','gradRead','gradWriting')                      
                    ;                    
                    
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_MN_grad_test_log := '';
                    SET @sql_MN_grad_test_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_mn_grad', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_MN_grad_test_log from @sql_MN_grad_test_log;
                                
                        execute sql_MN_grad_test_log;
                            
                        deallocate prepare sql_MN_grad_test_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_grad()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New MN grad test record' AS Uploader_MN_grad_test, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new MN grad test record' AS Uploader_MN_grad_test;
        END IF;

        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_mn_mca_ii');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New MN mca ii record' AS Uploader_MN_mca_ii, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_mca_ii()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;

                    call etl_hst_load_mn_mca_ii();
                    
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select distinct tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code in ('mcaiiMath','mcaiiReading','mcaiiScience')                      
                    ;                    
                    
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_MN_mca_ii_log := '';
                    SET @sql_MN_mca_ii_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_mn_mca_ii', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_MN_mca_ii_log from @sql_MN_mca_ii_log;
                                
                        execute sql_MN_mca_ii_log;
                            
                        deallocate prepare sql_MN_mca_ii_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_mca_ii()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New MN mca ii record' AS Uploader_MN_mca_ii, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new MN mca ii record' AS Uploader_MN_mca_ii;
        END IF;


        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_mn_mca_iii');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New MN mca iii record' AS Uploader_MN_mca_iii, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_mca_iii()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;

                    call etl_hst_load_mn_mca_iii();
                    
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select distinct tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code in ('mcaiiiMath','mcaiiiScience')                      
                    ;                    
                    
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_MN_mca_iii_log := '';
                    SET @sql_MN_mca_iii_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_mn_mca_iii', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_MN_mca_iii_log from @sql_MN_mca_iii_log;
                                
                        execute sql_MN_mca_iii_log;
                            
                        deallocate prepare sql_MN_mca_iii_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_mca_iii()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New MN mca iii record' AS Uploader_MN_mca_iii, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new MN mca iii record' AS Uploader_MN_mca_iii;
        END IF;
        
        
        
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_mn_mod_ii');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New MN mod ii record' AS Uploader_MN_MOD_II, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_mod_ii()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;

                    call etl_hst_load_mn_mod_ii();
                    
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select distinct tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code like 'mcamodII%'
                    ;                    
                    
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_MN_mod_ii_log := '';
                    SET @sql_MN_mod_ii_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_mn_mod_ii', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_MN_mod_ii_log from @sql_MN_mod_ii_log;
                                
                        execute sql_MN_mod_ii_log;
                            
                        deallocate prepare sql_MN_mod_ii_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_mod_ii()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New MN mod ii record' AS Uploader_MN_MOD_II, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No New MN mod ii record' AS Uploader_MN_MOD_II;
        END IF;
        
        
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_mn_mod_iii');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New MN mod iii record' AS Uploader_MN_MOD_III, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_mod_iii()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;

                    call etl_hst_load_mn_mod_iii();
                    
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select distinct tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code like 'mcamodIII%'
                    ;                    
                    
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_MN_mod_iii_log := '';
                    SET @sql_MN_mod_iii_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_mn_mod_iii', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_MN_mod_iii_log from @sql_MN_mod_iii_log;
                                
                        execute sql_MN_mod_iii_log;
                            
                        deallocate prepare sql_MN_mod_iii_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_mod_iii()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New MN mod iii record' AS Uploader_MN_MOD_III, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No New MN mod iui record' AS Uploader_MN_MOD_III;
        END IF;
        
        
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_mn_mtas');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New MN MTAS record' AS Uploader_MN_MTAS, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_mtas()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;

                    call etl_hst_load_mn_mtas();
                    
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select distinct tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code like 'mtas%'
                    ;                    
                    
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_MN_MTAS_log := '';
                    SET @sql_MN_MTAS_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_mn_mtas', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_MN_MTAS_log from @sql_MN_mod_iii_log;
                                
                        execute sql_MN_MTAS_log;
                            
                        deallocate prepare sql_MN_MTAS_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_mtas', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New MN MTAS' AS Uploader_MN_MTAS, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No New MN MTAS' AS Uploader_MN_MTAS;
        END IF;
        
        
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_mn_mtas_iii');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New MN MTAS III record' AS Uploader_MN_MTAS, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_mtas_iii()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;

                    call etl_hst_load_mn_mtas_iii();
                    
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select distinct tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code like 'mtas%'
                    ;                    
                    
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_MN_MTAS_iii_log := '';
                    SET @sql_MN_MTAS_iii_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_mn_mtas_iii', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_MN_MTAS_III_log from @sql_MN_mod_iii_log;
                                
                        execute sql_MN_MTAS_III_log;
                            
                        deallocate prepare sql_MN_MTAS_III_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_mtas_iii', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New MN MTAS III' AS Uploader_MN_MTAS_iii, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No New MN MTAS III' AS Uploader_MN_MTAS_iii;
        END IF;
        
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_mn_aa');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New MN AA record' AS Uploader_MN_MTAS, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_aa()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;

                    call etl_hst_load_mn_aa();
                    
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select distinct tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code like 'mnaa%'
                    ;                    
                    
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_MN_AA_log := '';
                    SET @sql_MN_AA_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_mn_aa', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_MN_AA_log from @sql_MN_AA_log;
                                
                        execute sql_MN_AA_log;
                            
                        deallocate prepare sql_MN_AA_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_aa', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New MN AA' AS Uploader_MN_AA, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No New MN AA' AS Uploader_MN_AA;
        END IF;
        

        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_nwea');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New MN nwea record' AS Uploader_MN_nwea, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_nwea()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;

                    call etl_hst_load_mn_nwea();
                    
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select distinct tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code in ('mnNWEAMath','mnNWEAReading')                      
                    ;                    
                    
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_MN_nwea_log := '';
                    SET @sql_MN_nwea_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_mn_nwea', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_MN_nwea_log from @sql_MN_nwea_log;
                                
                        execute sql_MN_nwea_log;
                            
                        deallocate prepare sql_MN_nwea_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_mn_nwea()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New MN nwea record' AS Uploader_MN_nwea, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new MN nwea record' AS Uploader_MN_nwea;
        END IF;
        

        select count(*)
        into   v_process_test_type
        from   tmp_test_type
        ;

        IF v_process_test_type > 0 or v_etl_lag_color_update_flag = 1
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
        SELECT @etl_imp_id, @client_id, 'etl_hst_mn', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;

        drop table if exists `tmp_test_type`;
                
END//

