drop procedure if exists etl_hst_wy//

CREATE DEFINER=`dbadmin`@`localhost` PROCEDURE `etl_hst_wy`()
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
        SELECT @etl_imp_id, @client_id, 'etl_hst_wy', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
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
        
        
        
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_wy_paws');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New WY paws record' AS Uploader_WY_paws, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_wy_paws()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;

                    call etl_hst_load_wy_paws();
                    
                    
                    Insert into tmp_test_type(test_type_moniker)
                         select distinct tt.moniker
                           from c_ayp_subject sub
                           join c_ayp_test_type tt
                             on sub.ayp_test_type_id = tt.ayp_test_type_id
                          where sub.ayp_subject_code in ('pawsMath','pawsReading','pawsScience')                      
                    ;                    
                    
                    
                    SET v_etl_rpt_flag = 1;
                    SET @sql_WY_paws_log := '';
                    SET @sql_WY_paws_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'etl_hst_load_wy_paws', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_WY_paws_log from @sql_WY_paws_log;
                                
                        execute sql_WY_paws_log;
                            
                        deallocate prepare sql_WY_paws_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_hst_load_wy_paws()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New WY paws record' AS Uploader_WY_paws, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new WY paws record' AS Uploader_WY_paws;
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
        SELECT @etl_imp_id, @client_id, 'etl_hst_wy', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;

        drop table if exists `tmp_test_type`;
                
END//

