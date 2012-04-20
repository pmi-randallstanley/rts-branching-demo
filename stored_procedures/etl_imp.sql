/*
$Rev: 9892 $ 
$Author: randall.stanley $ 
$Date: 2011-01-18 09:09:26 -0500 (Tue, 18 Jan 2011) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp.sql $
$Id: etl_imp.sql 9892 2011-01-18 14:09:26Z randall.stanley $ 
 */

######################################
## Summary Proc to process etl_imp* ##
######################################

DROP PROCEDURE IF EXISTS etl_imp //

CREATE definer=`dbadmin`@`localhost` procedure etl_imp()
CONTAINS SQL
COMMENT '$Rev: 9892 $ $Date: 2011-01-18 09:09:26 -0500 (Tue, 18 Jan 2011) $'
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

        call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

        SELECT pmi_admin.pmi_f_get_next_sequence('etl_imp_id', 1) INTO @etl_imp_id;
        SET @begin_time := now();
        SELECT convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
        SET v_etl_rpt_flag = 0;
        SET v_etl_bm_build_flag = 0;
        INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
        SELECT @etl_imp_id, @client_id, 'etl_imp', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        

        select  count(case when st.state_abbr = 'fl' then st.state_abbr end)
        into    @is_fl_client
        from    pmi_state_info as st
        where   st.state_id = @state_id
        and     st.state_abbr in ('fl')
        ;


    ###############################
    ## Current School Year Check ##
    ###############################
    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
    SELECT @etl_imp_id, @client_id, 'sql:check school year', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;

    SELECT  count(*)
    INTO    @current_school_year_valid
    FROM    c_school_year
    WHERE   now() between begin_date and end_date
    AND     active_flag = 1
    ;
    
    # Only run ETL load if current school year is valid
    IF @current_school_year_valid > 0 THEN

        SELECT 'School Year Check - Passed' AS Uploader_School_Year_Check;
        INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
        SELECT @etl_imp_id, @client_id, 'sql:check school year', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;

        ##############################
        ## Process the Uploader Log ##
        ##############################

        INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
        SELECT @etl_imp_id, @client_id, 'imp_process_upload_log()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
        select concat(database(), '_ods') into @db;
    
        SET @sqltext := CONCAT('call ', @db, '.imp_process_upload_log()');
    
            prepare sqltext from @sqltext;
                
                execute sqltext;
                    
                deallocate prepare sqltext;  
                
        INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
        SELECT @etl_imp_id, @client_id, 'imp_process_upload_log()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;        
            
        ########################
        ## Run etl_imp_school ##
        ########################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_school');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New school record' AS Uploader_School, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_school()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                    call etl_imp_school();
                    SET v_etl_rpt_flag = 1;
                    SET @sql_school_log := '';
                    SET @sql_school_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_school', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_school_log from @sql_school_log;
                                
                        execute sql_school_log;
                            
                        deallocate prepare sql_school_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_school()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New school record' AS Uploader_School, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new school record' AS Uploader_School;
        END IF;
        
        ################################
        ## Run etl_imp_school_cluster ##
        ################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_cluster');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New school cluster record' AS Uploader_School_Cluster, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_school_cluster()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                    call etl_imp_school_cluster();
                    SET v_etl_rpt_flag = 1;
                    SET @sql_school_cluster_log := '';
                    SET @sql_school_cluster_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_cluster', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_school_cluster_log from @sql_school_cluster_log;
                                
                        execute sql_school_cluster_log;
                            
                        deallocate prepare sql_school_cluster_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_school_cluster()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New school cluster record' AS Uploader_School_Cluster, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new school cluster record' AS Uploader_School_Cluster;
        END IF;

        #########################
        ## Run etl_imp_teacher ##
        #########################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_teacher');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New teacher record' AS Uploader_Teacher, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_teacher()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                    call etl_imp_teacher();
                    SET v_etl_rpt_flag = 1;
                    SET @sql_teacher_log := '';
                    SET @sql_teacher_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_teacher', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                        prepare sql_teacher_log from @sql_teacher_log;
                       
                        execute sql_teacher_log;
                            
                        deallocate prepare sql_teacher_log; 
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_teacher()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New teacher record' AS Uploader_Teacher, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
                        ELSE SELECT 'No new teacher record' AS Uploader_Teacher;     
        END IF;
        
        ###########################
        ## Run etl_imp_principal ##
        ###########################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_principal');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New principal record' AS Uploader_Principal, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_principal()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_imp_principal();
                        SET @sql_principle_log := '';
                        SET @sql_principle_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_principal', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                            prepare sql_principle_log from @sql_principle_log;
                            
                            execute sql_principle_log;
                            
                            deallocate prepare sql_principle_log;   
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_principal()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New principal record' AS Uploader_Principal, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
                        ELSE SELECT 'No new principal record' AS Uploader_Principal;     
        END IF;
        
        ################################
        ## Run etl_imp_central_office ##
        ################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_central_office');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New central office record' AS Uploader_CO, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_central_office()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_imp_central_office();
                        SET @sql_central_office_log := '';
                        SET @sql_central_office_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_central_office', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                            prepare sql_central_office_log from @sql_central_office_log;
                            
                            execute sql_central_office_log;
                            
                            deallocate prepare sql_central_office_log;  
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_central_office()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New central office record' AS Uploader_CO, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new central office record' AS Uploader_CO;                      
        END IF;
        
        
        ################################
        ## Run etl_imp_district_admin ##
        ################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_district_admin');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New district_admin record' AS Uploader_CO, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_district_admin()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_imp_district_admin();
                        SET @sql_district_admin_log := '';
                        SET @sql_district_admin_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_district_admin', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                            prepare sql_district_admin_log from @sql_district_admin_log;
                            
                            execute sql_district_admin_log;
                            
                            deallocate prepare sql_district_admin_log; 
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_district_admin()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New district_admin record' AS Uploader_CO, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new district_admin record' AS Uploader_CO;                      
        END IF;

        #######################################
        ## Run etl_imp_custom_user_role      ##
        #######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_custom_user_role');
    
        if  @upload_id > 1 then 
            select 'New Custom User Role File' as Upload_Custom_User_Role, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_imp_custom_user_role()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_imp_custom_user_role();
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_imp_custom_user_role()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Custom User Role File' as Upload_Custom_User_Role, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Custom User Role File' as Upload_Custom_User_Role;
        end if;    
                
        #########################
        ## Run etl_imp_student ##
        #########################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_student');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New student record' AS Uploader_Student, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_student()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_imp_student();
                        SET v_etl_rpt_flag = 1;
                        SET v_etl_exec_stu_cust_fltr_flag = 1;
                        SET @sql_student_log := '';
                        SET @sql_student_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_student', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                            prepare sql_student_log from @sql_student_log;
                            
                            execute sql_student_log;
                            
                            deallocate prepare sql_student_log;  
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_student()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New student record' AS Uploader_Student, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new student record' AS Uploader_Student;
        END IF;
        
        ########################
        ## Run etl_imp_course ##
        ########################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_course');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New course record' AS Uploader_Course, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_course()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_imp_course();
                        SET v_etl_rpt_flag = 1;
                        SET @sql_course_log := '';
                        SET @sql_course_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_course', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                            prepare sql_course_log from @sql_course_log;
                            
                            execute sql_course_log;
                            
                            deallocate prepare sql_course_log; 
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_course()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New course record' AS Uploader_Course, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
                        ELSE SELECT 'No new course record' AS Uploader_Course;
        END IF;

        #######################################
        ## Run etl_imp_course_type_override  ##
        #######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_course_type_override');
    
        if  @upload_id > 1 then 
            select 'New Course Type Override File' as Upload_Course_Type_Override, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_imp_course_type_override()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_imp_course_type_override();
            set v_etl_rpt_flag = 1;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_imp_course_type_override()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Course Type Override File' as Upload_Course_Type_Override, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Course Type Override File' as Upload_Course_Type_Override;
        end if;    
        
        ##########################
        ## Run etl_imp_schedule ##
        ##########################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_schedule');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New schedule record' AS Uploader_Schedule, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_class()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_imp_class();
                        SET v_etl_rpt_flag = 1;
                        SET @sql_schedule_log := '';
                        SET @sql_schedule_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_schedule', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                            prepare sql_schedule_log from @sql_schedule_log;
                            
                            execute sql_schedule_log;
                            
                            deallocate prepare sql_schedule_log; 
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_class()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New schedule record' AS Uploader_Schedule, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new schedule record' AS Uploader_Schedule;
        END IF;

        ###############################
        ## Run etl_imp_student_login ##
        ###############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_student_login');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New student login record' AS Uploader_Student_Login, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_student_login()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_imp_student_login();
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_student_login()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New student login record' AS Uploader_Student_Login, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new student login record' AS Uploader_Student_Login;
        END IF;
        
        ##################################
        ## Run etl_color_lship_dynamics ##
        ##################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_lship_dynamics');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Color File - Leadership Dynamics' AS Uploader_Color_File_lship_dynamic, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_lship_dynamics()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_color_lship_dynamics();
                        SET v_etl_rpt_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_lship_dynamics()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Color File - Leadership Dynamics' AS Uploader_Color_File_lship_dynamic, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Color File - Leadership Dynamics' AS Uploader_Color_File_lship_dynamic;
        END IF;    


        #############################
        ## Run etl_color_idel      ##
        #############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_idel');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'IDEL Color Data - New File' AS Uploader_Color_IDEL, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_pm_color_idel()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_pm_color_idel();
                        SET v_etl_rpt_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_pm_color_idel()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New IDEL Color File - IDEL' AS Uploader_Color_IDEL, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No IDEL Color File - IDEL' AS Uploader_Color_IDEL;
        END IF; 

        ##############################
        ## Run etl_color_ayp_strand ##
        ##############################
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

        ##############################
        ## Run etl_color_ayp_subject ##
        ##############################
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
            
        ##############################
        ## Run etl_color_ayp_benchmark ##
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_ayp_benchmark');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'AYP Benchmark Color Data - New File' AS Uploader_Color_ayp_benchmark, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_ayp_benchmark()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_color_ayp_benchmark();
                        SET v_etl_rpt_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_ayp_benchmark()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New AYP Benchmark Color File' AS Uploader_Color_ayp_benchmark, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No AYP Benchmark Color File' AS Uploader_Color_ayp_benchmark;
        END IF; 
            
            
        ##############################
        ## Run etl_color_dibels ##
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_dibels');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New DIBELS Color File' AS Uploader_Color_DIBELS, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_dibels()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_color_dibels();
                        SET v_etl_rpt_flag = 1;
                        SET v_etl_pm_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_dibels()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New DIBELS Color File' AS Uploader_Color_DIBELS, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No DIBELS Color File' AS Uploader_Color_DIBELS;
        END IF; 

        ##############################
        ## Run etl_color_snap   ##
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_snap');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New SNAP Color File' AS Uploader_Color_SNAP, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_snap()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_color_snap();
                        SET v_etl_rpt_flag = 1;
                        SET v_etl_pm_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_snap()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New SNAP Color File' AS Uploader_Color_SNAP, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No SNAP Color File' AS Uploader_Color_SNAP;
        END IF; 

        ##############################
        ## Run etl_color_lexile ##
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_lexile');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'Lexile Color Data - New File' AS Uploader_Color_lexile, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_lexile()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_color_lexile();
                        SET v_etl_rpt_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_lexile()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Lexile Color File' AS Uploader_Color_lexile, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Lexile Color File' AS Uploader_Color_lexile;
        END IF; 
            
             
        ##############################
        ## Run etl_color_l_matrix ##
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_matrix');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'Matrix Color Data - New File' AS Uploader_Color_matrix, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_l_matrix()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_color_l_matrix();
                        SET v_etl_rpt_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_color_l_matrix()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Matrix Color File' AS Uploader_Color_matrix, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Matrix Color File' AS Uploader_Color_matrix;
        END IF; 
            

        #######################################
        ## Run etl_color_ayp_tchr_perf_delta ##
        #######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_tchr_perf_delta');
    
        if  @upload_id > 1 then 
            select 'New Color File - AYP Teacher Perf Delta' as Uploader_Color_AYP_Tch_Perf_Delta, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_ayp_tchr_perf_delta()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_color_ayp_tchr_perf_delta();
            set v_etl_rpt_flag = 1;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_ayp_tchr_perf_delta()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Color File - AYP Teacher Perf Delta' as Uploader_Color_AYP_Tch_Perf_Delta, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Color File - AYP Teacher Perf Delta' as Uploader_Color_AYP_Tch_Perf_Delta;
        end if;    

        ##########################################
        ## Run etl_color_ayp_tchr_perf_pass_pct ##
        ##########################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_tchr_perf_pass_pct');
    
        if  @upload_id > 1 then 
            select 'New Color File - AYP Teacher Perf Percent' as Uploader_Color_AYP_Tch_Perf_Pct, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_ayp_tchr_perf_pass_pct()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_color_ayp_tchr_perf_pass_pct();
            set v_etl_rpt_flag = 1;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_ayp_tchr_perf_pass_pct()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Color File - AYP Teacher Perf Percent' as Uploader_Color_AYP_Tch_Perf_Pct, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Color File - AYP Teacher Perf Percent' as Uploader_Color_AYP_Tch_Perf_Pct;
        end if;    

        #######################################
        ## Run etl_color_grad_status         ##
        #######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_grad_status');
    
        if  @upload_id > 1 then 
            select 'New Color File - Grad Status' as Uploader_Color_Grad_Status, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_grad_status()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_color_grad_status();
            set v_etl_grad_rpt_flag = 1;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_grad_status()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Color File - Grad Status' as Uploader_Color_Grad_Status, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Color File - Grad Status' as Uploader_Color_Grad_Status;
        end if;    

        #######################################
        ## Run etl_color_grad_project        ##
        #######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_grad_proj');
    
        if  @upload_id > 1 then 
            select 'New Color File - Grad Projects' as Uploader_Color_Grad_Projects, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_grad_project()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_color_grad_project();
            set v_etl_grad_rpt_flag = 1;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_grad_project()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Color File - Grad Projects' as Uploader_Color_Grad_Projects, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Color File - Grad Projects' as Uploader_Color_Grad_Projects;
        end if;    

        ##########################
        ## Run etl_color_access ##
        ##########################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_access');
    
        if  @upload_id > 1 then 
            select 'New Color File - Access' as Uploader_Color_Access, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_access()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_color_access();
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_access()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Color File - Access' as Uploader_Color_Access, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Color File - Access' as Uploader_Color_Access;
        end if;    

        #############################
        ## Run etl_color_terranova ##
        #############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_terranova');
    
        if  @upload_id > 1 then 
            select 'New Color File - TerraNova' as Uploader_Color_TerraNova, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_terranova()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_color_terranova();
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_terranova()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Color File - TerraNova' as Uploader_Color_TerraNova, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Color File - TerraNova' as Uploader_Color_TerraNova;
        end if;    

        ################################
        ## Run etl_color_smi_quantile ##
        ################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_smi_quantile');
    
        if  @upload_id > 1 then 
            select 'New Color File - SMI Quantile' as Uploader_Color_SMI_Quantile, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_smi_quantile()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_color_smi_quantile();
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_smi_quantile()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Color File - SMI Quantile' as Uploader_Color_SMI_Quantile, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Color File - SMI Quantile' as Uploader_Color_SMI_Quantile;
        end if;    

        #########################
        ## Run etl_color_cogat ##
        #########################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_color_cogat');
    
        if  @upload_id > 1 then 
            select 'New Color File - CogAT' as Uploader_Color_CogAT, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_cogat()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_color_cogat();
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_color_cogat()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Color File - CogAT' as Uploader_Color_CogAT, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Color File - CogAT' as Uploader_Color_CogAT;
        end if;    

        ################################
        ## Run etl_imp_ayp_enrollment ##
        ################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_ayp_enrollment');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New AYP enrollment record' AS Uploader_Schedule, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_ayp_enrollment()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_imp_ayp_enrollment();
                        SET v_etl_rpt_flag = 1;
                        SET @sql_schedule_log := '';
                        SET @sql_schedule_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_ayp_enrollment', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
                            prepare sql_schedule_log from @sql_schedule_log;
                            
                            execute sql_schedule_log;
                            
                            deallocate prepare sql_schedule_log; 
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_imp_ayp_enrollment()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New AYP enrollment record' AS Uploader_Schedule, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No new AYP enrollment record' AS Uploader_Schedule;
        END IF;

        ##############################################
        ## Run etl_grad_subject_required_projects  ##
        ##############################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_grad_sub_req_proj');
    
        if  @upload_id > 1 then 
            select 'New Grad Subject Required Projects File' as Uploader_Grad_Subject_Required_Proj, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_grad_subject_required_projects()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_grad_subject_required_projects();
            set v_etl_grad_rpt_flag = 1;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_grad_subject_required_projects()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Grad Subject Required Projects File' as Uploader_Grad_Subject_Required_Proj, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Grad Subject Required Projects File' as Uploader_Grad_Subject_Required_Proj;
        end if;    

        ##############################################
        ## Run etl_grad_student_completed_projects  ##
        ##############################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_grad_stu_comp_proj');
    
        if  @upload_id > 1 then 
            select 'New Grad Student Completed Projects File' as Uploader_Grad_Student_Comp_Projects, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_grad_student_completed_projects()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_grad_student_completed_projects();
            set v_etl_grad_rpt_flag = 1;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_grad_student_completed_projects()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Grad Student Completed Projects File' as Uploader_Grad_Student_Comp_Projects, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Grad Student Completed Projects File' as Uploader_Grad_Student_Comp_Projects;
        end if;    


        ##################################
        ## Purge Student Responses      ##
        ##################################
    
        INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
        SELECT @etl_imp_id, @client_id, 'etl_bm_purge_student_results()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
        call etl_bm_purge_student_results(@deletecount);
        if @deletecount > 0 then
            SELECT 'Selected Results Purged' AS Sam_student_responses_purge_status;
            SET v_data_change = 1;
        else
            SELECT 'No Results Purged' AS Sam_student_responses_purge_status;
        end if;
        INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
        SELECT @etl_imp_id, @client_id, 'etl_bm_purge_student_results()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;

        ##################################
        ## Score test due to key update ##
        ##################################
    
        INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
        SELECT @etl_imp_id, @client_id, 'etl_bm_rescore_tests_for_key_update()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
        call etl_bm_rescore_tests_for_key_update(@rescored);
        if @rescored > 0 then
            SELECT 'Tests Rescored' AS SAM_Test_Rescore_Status;
            SET v_data_change = 1;
        else
            SELECT 'No Tests Rescored' AS SAM_Test_Rescore_Status;
        end if;
        INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
        SELECT @etl_imp_id, @client_id, 'etl_bm_rescore_tests_for_key_update()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
 
 
        ##################################
        ## Run etl_bm_load_pmi_pend_ola ## 
        ##################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_ola');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Scan Files - OLA' AS Uploader_Scan_File_external, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_pmi_pend_ola()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_bm_load_pmi_pend_ola();
                        SET v_etl_rpt_flag = 1;
                        SET v_etl_bm_build_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_pmi_pend_ola()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Scan Files - OLA' AS Uploader_Scan_File_external, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Scan Files - OLA' AS Uploader_Scan_File_external;
        END IF;        

        #######################################
        ## Run etl_imp_sam_lexmark_form          ##
        #######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_sam_lexmark_form');
    
        if  @upload_id > 1 then 
            select 'New SAM Lexmark Form File' as Upload_SAM_Lexmark_Form, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_imp_sam_lexmark_form()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_imp_sam_lexmark_form();
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_imp_sam_lexmark_form()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New SAM Lexmark Form File' as Upload_SAM_Lexmark_Form, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No SAM Lexmark Form File' as Upload_SAM_Lexmark_Form;
        end if;    

        ######################################
        ## Run etl_bm_load_lexmark_tests    ##
        ######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_lexmark_test_ak');
    
        if  @upload_id > 1 then 
            select 'New Scan Files - Lexmark Test/AK' as Uploader_Scan_File_Lexmark_Tests_AK, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_lexmark_tests()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_bm_load_lexmark_tests();
            set v_etl_rpt_flag = 1;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_lexmark_tests()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Scan Files - Lexmark Test/AK' as Uploader_Scan_File_Lexmark_Tests_AK, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Scan Files - Lexmark Test/AK' as Uploader_Scan_File_Lexmark_Tests_AK;
        end if;    

        ######################################
        ## Run etl_bm_load_lexmark_results  ##
        ######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_lexmark');
    
        if  @upload_id > 1 then 
            select 'New Scan Files - Lexmark Results' as Uploader_Scan_File_Lexmark, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_lexmark_results()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_bm_load_lexmark_results();
            set v_etl_rpt_flag = 1;
            set v_etl_bm_build_flag = 1;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_lexmark_results()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Scan Files - Lexmark Results' as Uploader_Scan_File_Lexmark, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Scan Files - Lexmark Results' as Uploader_Scan_File_Lexmark;
        end if;    

        ######################################
        ## Run etl_bm_load_turning_tests    ##
        ######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_turning_test_ak');
    
        if  @upload_id > 1 then 
            select 'New Scan Files - Turning Test/AK' as Uploader_Scan_File_Turning_Tests_AK, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_turning_tests()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_bm_load_turning_tests();
            set v_etl_rpt_flag = 1;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_turning_tests()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Scan Files - Turning Test/AK' as Uploader_Scan_File_Turning_Tests_AK, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Scan Files - Turning Test/AK' as Uploader_Scan_File_Turning_Tests_AK;
        end if;    

        ######################################
        ## Run etl_bm_load_turning_results  ##
        ######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_turning');
    
        if  @upload_id > 1 then 
            select 'New Scan Files - Turning Results' as Uploader_Scan_File_Turning, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_turning_results()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_bm_load_turning_results();
            set v_etl_rpt_flag = 1;
            set v_etl_bm_build_flag = 1;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_turning_results()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Scan Files - Turning Results' as Uploader_Scan_File_Turning, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Scan Files - Turning Results' as Uploader_Scan_File_Turning;
        end if;    

        ###########################################
        ## Run etl_bm_load_pmi_scan_eng_results  ##
        ###########################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_pmi_scan_eng');
    
        if  @upload_id > 1 then 
            select 'New Scan Files - PM OMR Results' as Uploader_Scan_File_PMOMR, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_pmi_scan_eng_results()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_bm_load_pmi_scan_eng_results();
            set v_etl_rpt_flag = 1;
            set v_etl_bm_build_flag = 1;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_pmi_scan_eng_results()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Scan Files - PM OMR Results' as Uploader_Scan_File_PMOMR, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Scan Files - PM OMR Results' as Uploader_Scan_File_PMOMR;
        end if;    

        ############################
        ## Run etl_bm_load_abacus ##
        ############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_abacus');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Scan Files - Abacus' AS Uploader_Scan_File_Abacus, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_abacus()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_bm_load_abacus();
                        SET v_etl_rpt_flag = 1;
                        SET v_etl_bm_build_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_abacus()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Scan Files - Abacus' AS Uploader_Scan_File_Abacus, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Scan Files - Abacus' AS Uploader_Scan_File_Abacus;
        END IF;


        ############################
        ## Run etl_bm_load_edmin ##
        ############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_pivot_edmin');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Scan Files - edmin' AS Uploader_Scan_File_edmin, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_edmin()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_bm_load_edmin();
                        SET v_etl_rpt_flag = 1;
                        SET v_etl_bm_build_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_edmin()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Scan Files - edmin' AS Uploader_Scan_File_edmin, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Scan Files - edmin' AS Uploader_Scan_File_edmin;
        END IF;
        
        ###################################
        ## Run etl_bm_load_edusoft_tests ##
        ###################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_edusoft_test_ak');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Scan Files - Edusoft Test/AK' AS Uploader_Scan_File_Edusoft_Test, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_edusoft_tests()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_bm_load_edusoft_tests();
                        SET v_etl_rpt_flag = 1;
                        SET v_etl_bm_build_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_edusoft_tests()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Scan Files - Edusoft Test/AK' AS Uploader_Scan_File_Edusoft_Test, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Scan Files - Edusoft Test/AK' AS Uploader_Scan_File_Edusoft_Test;
        END IF;
        
        ###################################
        ## Run etl_bm_load_edusoft_align ##
        ###################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_sr_ak_bm_align_edusoft');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Scan Files - Edusoft Alignment' AS Uploader_Scan_File_Edusoft_Align, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_edusoft_align()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_bm_load_edusoft_align();
                        SET v_etl_rpt_flag = 1;
                        SET v_etl_bm_build_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_edusoft_align()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Scan Files - Edusoft Alignment' AS Uploader_Scan_File_Edusoft_Align, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Scan Files - Edusoft Alignment' AS Uploader_Scan_File_Edusoft_Align;
        END IF;

        #####################################
        ## Run etl_bm_load_edusoft_results ##
        #####################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_pivot_edusoft');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Scan Files - Edusoft Results' AS Uploader_Scan_File_Edusoft_Results, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_edusoft_results()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_bm_load_edusoft_results();
                        SET v_etl_rpt_flag = 1;
                        SET v_etl_bm_build_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_edusoft_results()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Scan Files - Edusoft Results' AS Uploader_Scan_File_Edusoft_Results, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Scan Files - Edusoft Results' AS Uploader_Scan_File_Edusoft_Results;
        END IF;

        ####################################
        ## Run etl_bm_load_kaplan_test_ak ##
        ####################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_sr_ak_kaplan');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Scan Files - Kaplan Test/AK' AS Uploader_Scan_File_Kaplan_Test_AK, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_kaplan_test_ak()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_bm_load_kaplan_test_ak();
                        SET v_etl_rpt_flag = 1;
                        SET v_etl_bm_build_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_kaplan_test_ak()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Scan Files -  Kaplan Test/AK' AS Uploader_Scan_File_Kaplan_Test_AK, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Scan Files -  Kaplan Test/AK' AS Uploader_Scan_File_Kaplan_Test_AK;
        END IF;

        ############################
        ## Run etl_bm_load_scantron ##
        ############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_pivot_scantron');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Scan Files - scantron' AS Uploader_Scan_File_scantron, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_scantron()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_bm_load_scantron();
                        SET v_etl_rpt_flag = 1;
                        SET v_etl_bm_build_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_scantron()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Scan Files - scantron' AS Uploader_Scan_File_scantron, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Scan Files - scantron' AS Uploader_Scan_File_scantron;
        END IF;
        
                
        ##############################
        ## Run etl_bm_load_external ##  New pre-pivoted external results
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_pivot_external');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Scan Files - External Pivot' AS Uploader_Scan_File_external, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_external_results()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_bm_load_external_results();
                        SET v_etl_rpt_flag = 1;
                        SET v_etl_bm_build_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_external_results()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Scan Files - External Pivot' AS Uploader_Scan_File_external, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Scan Files - External Pivot' AS Uploader_Scan_File_external;
        END IF;        

        ##############################
        ## Run etl_bm_load_non_pmi ##  Old denormalized external results
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_external');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Scan Files - External' AS Uploader_Scan_File_external, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_non_pmi()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_bm_load_non_pmi();
                        SET v_etl_rpt_flag = 1;
                        SET v_etl_bm_build_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_non_pmi()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Scan Files - External' AS Uploader_Scan_File_external, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Scan Files - External' AS Uploader_Scan_File_external;
        END IF;        

        ################################
        ## Run etl_bm_load_intel_test ##
        ################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_intel_assess_test');
    
        if  @upload_id > 1 then 
            select 'New Intel Assess Test File' as Upload_Custom_User_Role, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_intel_test()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_bm_load_intel_test();
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_intel_test()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Intel Assess Test File' as Upload_Custom_User_Role, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Intel Assess Test File' as Upload_Custom_User_Role;
        end if;    

        ##############################
        ## Run etl_bm_load_intel_ak ##
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_intel_assess_ak');
    
        if  @upload_id > 1 then 
            select 'New Intel Assess Ak File' as Upload_Custom_User_Role, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_intel_ak()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_bm_load_intel_ak();
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_intel_ak()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Intel Assess Ak File' as Upload_Custom_User_Role, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Intel Assess Ak File' as Upload_Custom_User_Role;
        end if;    

        #######################################
        ## Internal Scan Results - Pre-Pivoted
        #######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_pivot_internal');
        
        if  @upload_id > 1 then
            select 'New Scan Files - Internal Pivot' AS Scan_Results, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_internal_results()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_bm_load_internal_results();
            set v_etl_rpt_flag = 1;
            set v_etl_bm_build_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_bm_load_internal_results()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Scan Files - Internal Pivot' AS Scan_Results, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No Scan Files - Internal Pivot' AS Scan_Results;
        end if;        

        ##############################
        ## Run etl_bm_load_internal ##
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_internal');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Scan Files - internal' AS Uploader_Scan_File_internal, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_pmi()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_bm_load_pmi();
                        SET v_etl_rpt_flag = 1;
                        SET v_etl_bm_build_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_bm_load_pmi()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Scan Files - internal' AS Uploader_Scan_File_internal, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Scan Files - internal' AS Uploader_Scan_File_internal;
        END IF;          
        
        
        #############################
        ## Run etl_rpt_idel_scores ##
        #############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_dibels_idel');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'IDEL Data - New File' AS Uploader_IDEL, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_idel_scores(1,1)', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_rpt_idel_scores(1,1);
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_idel_scores(1,1)', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New IDEL File - IDEL' AS Uploader_IDEL, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No IDEL File - IDEL' AS Uploader_IDEL;
        END IF; 

        ###############################
        ## Run etl_rpt_dibels_scores ##
        ###############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_dibels');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'DIBELS Data - New File' AS Uploader_DIBELS, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_dibels_scores()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_rpt_dibels_scores(1, 1);
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_dibels_scores()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New DIBELS File' AS Uploader_DIBELS, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No DIBELS File' AS Uploader_DIBELS;
        END IF; 
 
        ################################################
        ## Run etl_rpt_dibels_scores_ltdb_pentamation ##
        ################################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_ltdb_pentamation');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'DIBELS Data LTDB Penatamation- New File' AS Uploader_DIBELS_Pentamation, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_dibels_scores_ltdb_pentamation()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_rpt_dibels_scores_ltdb_pentamation();
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_dibels_scores_ltdb_pentamation()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New DIBELS Penatamation File' AS Uploader_DIBELS_Pentamation, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No DIBELS PenatamationFile' AS Uploader_DIBELS_Pentamation;
        END IF; 
 
        #######################################
        ## Run etl_rpt_dibels_scores_ga_gcbe ##
        #######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_dibels_ga_gcbe');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'DIBELS - GAGCBE Data - New File' AS Uploader_DIBELS_GA_GCBE, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_dibels_score_ga_gcbe()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_rpt_dibels_score_ga_gcbe();
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_dibels_score_ga_gcbe()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New DIBELS - GAGCBE File - DIBELS' AS Uploader_DIBELS_GA_GCBE, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No DIBELS - GAGCBE File - DIBELS' AS Uploader_DIBELS_GA_GCBE;
        END IF; 
 
        #############################
        ## Run etl_rpt_dibels_forf ##
        #############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_forf');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'DIBELS Data FORF - New File' AS Uploader_FORF, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_dibels_forf()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_rpt_dibels_forf();
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_dibels_forf()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New DIBELS FORF File' AS Uploader_FORF, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No DIBELS FORF' AS Uploader_FORF;
        END IF; 

        ####################################
        ## Run etl_rpt_dibels_next_scores ##
        ####################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_dibels_next');
    
        if  @upload_id > 1 then 
            select 'New File - DIBELS Next' as DIBELS_Next_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_dibels_next_scores()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_rpt_dibels_next_scores();
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_dibels_next_scores()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New File - DIBELS Next' as DIBELS_Next_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No File - DIBELS Next' as DIBELS_Next_Scores;
        end if;    
 
        ##############################
        ## Lexile Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_lexile');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New PM Lexile Scores' AS PM_Lexile_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_pm_lexile_scores()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_pm_lexile_scores();
                        SET v_etl_rpt_flag = 1;
                        # SET v_etl_pm_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_pm_lexile_scores()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New PM Lexile Scores' AS PM_Lexile_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No PM Lexile Scores' AS PM_Lexile_Scores;
        END IF;        

        ####################################
        ## Run etl_pm_smi_quantile_scores ##
        ####################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_smi_quantile');
    
        if  @upload_id > 1 then 
            select 'New PM File - SMI Quantile' as PM_SMI_Quantile_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_smi_quantile_scores()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_pm_smi_quantile_scores();
            set v_etl_rpt_flag = 1;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_smi_quantile_scores()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New PM File - SMI Quantile' as PM_SMI_Quantile_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No PM File - SMI Quantile' as PM_SMI_Quantile_Scores;
        end if;    

        ##############################
        ## IRI Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_iri');
        
        if  @upload_id > 1 then
            select 'New IRI Scores' AS IRI_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_baseball_detail_iri()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_rpt_baseball_detail_iri();
            # set  v_etl_pm_flag = 1;
            call etl_rpt_bbcard_detail_iri();
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_baseball_detail_iri()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New IRI Scores' AS IRI_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No IRI Scores' AS IRI_Scores;
        end if;        

        ##############################
        ## ACT Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_act');
        
        if  @upload_id > 1 then
            select 'New ACT Scores' AS ACT_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_act()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_pm_natl_test_scores_act();
            # set v_etl_rpt_flag = 1;
            # set v_etl_pm_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_act()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New ACT Scores' AS ACT_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No ACT Scores' AS ACT_Scores;
        end if;        

        ##############################
        ## PSAT Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_psat');
        
        if  @upload_id > 1 then
            select 'New PSAT Scores' AS PSAT_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_psat()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_pm_natl_test_scores_psat();
            set v_etl_rpt_flag = 1;
            # set v_etl_pm_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_psat()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New PSAT Scores' AS PSAT_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No PSAT Scores' AS PSAT_Scores;
        end if;        

        ##############################
        ## SAT Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_sat');
        
        if  @upload_id > 1 then
            select 'New SAT Scores' AS SAT_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_sat()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_pm_natl_test_scores_sat();
            set v_etl_rpt_flag = 1;
            # set v_etl_pm_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_sat()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New SAT Scores' AS SAT_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No SAT Scores' AS SAT_Scores;
        end if;        

        ##############################
        ## Explorer Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_ga_explorer');
        
        if  @upload_id > 1 then
            select 'New Explorer Scores' AS Explorer_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_explore()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_pm_natl_test_scores_explore();
            # set v_etl_rpt_flag = 1;
            # set v_etl_pm_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_explore()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Explorer Scores' AS Explorer_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No Explorer Scores' AS Explorer_Scores;
        end if;        

        ##############################
        ## ITBS Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_itbs');
        
        if  @upload_id > 1 then
            select 'New ITBS Scores' AS ITBS_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_itbs()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_pm_natl_test_scores_itbs();
            # set v_etl_rpt_flag = 1;
            # set v_etl_pm_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_itbs()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New ITBS Scores' AS ITBS_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No ITBS Scores' AS ITBS_Scores;
        end if;        

        ##############################
        ## OLSAT Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_olsat');
        
        if  @upload_id > 1 then
            select 'New OLSAT Scores' AS OLSAT_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_stanford_olsat()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_pm_natl_test_scores_stanford_olsat();
            # set v_etl_rpt_flag = 1;
            # set v_etl_pm_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_stanford_olsat()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New OLSAT Scores' AS OLSAT_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No OLSAT Scores' AS OLSAT_Scores;
        end if;        

        ##############################
        ## PLAN Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_plan');
        
        if  @upload_id > 1 then
            select 'New PLAN Scores' AS PLAN_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_plan()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_pm_natl_test_scores_plan();
            # set v_etl_rpt_flag = 1;
            # set v_etl_pm_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_plan()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New PLAN Scores' AS PLAN_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No PLAN Scores' AS PLAN_Scores;
        end if;        

        ##############################
        ## STAN10 Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_stanford_10');
        
        if  @upload_id > 1 then
            select 'New STAN10 Scores' AS STAN10_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_stanford_10()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_pm_natl_test_scores_stanford_10();
            # set v_etl_rpt_flag = 1;
            # set v_etl_pm_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_pm_natl_test_scores_stanford_10()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New STAN10 Scores' AS STAN10_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No STAN10 Scores' AS STAN10_Scores;
        end if;        

        ##############################
        ## NWEA Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_nwea');
        
        if  @upload_id > 1 then
            select 'New NWEA Scores' AS NWEA_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_bbcard_detail_nwea()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_rpt_bbcard_detail_nwea();
            # set v_etl_rpt_flag = 1;
            # set v_etl_pm_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_bbcard_detail_nwea()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New NWEA Scores' AS NWEA_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No NWEA Scores' AS NWEA_Scores;
        end if;        

        ##############################
        ## Access Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_access');
        
        if  @upload_id > 1 then
            select 'New Access Scores' AS Access_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_baseball_detail_access()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_rpt_baseball_detail_access();
            set v_etl_baseball_rebuild_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_baseball_detail_access()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Access Scores' AS Access_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No Access Scores' AS Access_Scores;
        end if;        

        ##############################
        ## TerraNova Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_terranova');
        
        if  @upload_id > 1 then
            select 'New TerraNova Scores' AS TerraNova_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_baseball_detail_terranova()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_rpt_baseball_detail_terranova();
            set v_etl_baseball_rebuild_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_baseball_detail_terranova()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New TerraNova Scores' AS TerraNova_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No TerraNova Scores' AS TerraNova_Scores;
        end if;        

        ##############################
        ## Running Record Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_running_record');
        
        if  @upload_id > 1 then
            select 'New Running Record Scores' AS RunningRecord_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_bbcard_detail_run_record()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_rpt_bbcard_detail_run_record();
            set v_etl_baseball_rebuild_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_bbcard_detail_run_record()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Running Record Scores' AS RunningRecord_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No Running Record Scores' AS RunningRecord_Scores;
        end if;        

        ##############################
        ## PMRN Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_fl_pmrn');
        
        if  @upload_id > 1 then
            select 'New PMRN Scores' AS PMRN_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_bbcard_detail_pmrn()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_rpt_bbcard_detail_pmrn();
            set v_etl_baseball_rebuild_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_bbcard_detail_pmrn()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New PMRN Scores' AS PMRN_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No PMRN Scores' AS PMRN_Scores;
        end if;        

        ##############################
        ## CogAT Scores
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_cogat');
        
        if  @upload_id > 1 then
            select 'New CogAT Scores' AS CogAT_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_bbcard_detail_cogat()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
        
            call etl_rpt_bbcard_detail_cogat();
            set v_etl_baseball_rebuild_flag = 1;
        
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_rpt_bbcard_detail_cogat()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New CogAT Scores' AS CogAT_Scores, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else
            select 'No CogAT Scores' AS CogAT_Scores;
        end if;        

        ##############################
        ## Attendance
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_attendance');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Attendance' AS Attendance, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_attendance()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_rpt_attendance();
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_attendance()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Attendance' AS Attendance, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Attendance' AS Attendance;
        END IF;        
 
        ##############################
        ## Discipline
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_discipline');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Discipline' AS Discipline, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_discipline()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_rpt_discipline();
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_discipline()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Discipline' AS Discipline, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Discipline' AS Discipline;
        END IF;        
 
        ##############################
        ## Grades
        ##############################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_grades');
    
        IF  @upload_id > 1
            THEN 
                SELECT 'New Grades' AS Grades, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_grades()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                        call etl_rpt_grades();
                        set v_etl_baseball_rebuild_flag = 1;
                    INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                    SELECT @etl_imp_id, @client_id, 'etl_rpt_grades()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                SELECT 'New Grades' AS Grades, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
            ELSE SELECT 'No Grades' AS Grades;
        END IF;        


        #######################################
        ## Run etl_imp_student_custom_filter ##
        #######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_student_custom_filter');
    
        if  @upload_id > 1 or (v_etl_exec_stu_cust_fltr_flag = 1 and @is_fl_client > 0) then
        
            if v_etl_exec_stu_cust_fltr_flag = 1 and @is_fl_client > 0 then

                select 'Gen FL LG BQ Filter Data' as Gen_FL_LG_BQ_Filter_Data, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
                insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                select @etl_imp_id, @client_id, 'etl_imp_student_fl_lg_bq_filter()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
                call etl_imp_student_fl_lg_bq_filter();
                insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
                select @etl_imp_id, @client_id, 'etl_imp_student_fl_lg_bq_filter()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
                select 'Gen FL LG BQ Filter Data' as Gen_FL_LG_BQ_Filter_Data, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;

            end if;

            select 'New Student Custom Filter File' as Upload_Student_Custom_Filter, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_imp_student_custom_filter()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_imp_student_custom_filter();
            insert tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            select @etl_imp_id, @client_id, 'etl_imp_student_custom_filter()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            select 'New Student Custom Filter File' as Upload_Student_Custom_Filter, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        else 
            select 'No Student Custom Filter File' as Upload_Student_Custom_Filter;
        end if;    


        ############################################
        ## Call Grad Status Post Processing Procs ##
        ############################################
        if v_etl_grad_rpt_flag = 1 then
        
            call etl_grad_subj_stud_proj_upd_req_projs();
            call etl_hst_post_load_update_grad();
            call etl_rpt_grad_sub_stu_score_projects();
        
        end if;

        ##########################
        ## Call summary Procs   ##
        ##########################

        IF v_etl_lag_color_update_flag = 1 THEN
            SELECT 'v_etl_lag_color_update_flag is 1 - Now running etl_hst_post_load_update_color()' AS procs, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            SELECT @etl_imp_id, @client_id, 'etl_hst_post_load_update_color()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_hst_post_load_update_color();
            INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            SELECT @etl_imp_id, @client_id, 'etl_hst_post_load_update_color()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            SELECT 'v_etl_lag_color_update_flag is 1 - Now running etl_hst_post_load_update_color()' AS procs, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        END IF;

        IF v_etl_rpt_flag = 1 OR v_data_change > 0 THEN
            SELECT 'etl_rpt_flag is 1 - Now running etl_rpt()' AS procs, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            SELECT @etl_imp_id, @client_id, 'etl_rpt()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_rpt();
            INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            SELECT @etl_imp_id, @client_id, 'etl_rpt()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            SELECT 'etl_rpt_flag is 1 - Now running etl_rpt()' AS procs, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        END IF;

        IF v_etl_rpt_flag = 0 AND v_data_change = 0 AND v_etl_baseball_rebuild_flag = 1 THEN
            SELECT 'v_etl_baseball_rebuild_flag is 1 - Now running etl_rpt_baseball_rebuild()' AS procs, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
            INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            SELECT @etl_imp_id, @client_id, 'etl_rpt_baseball_rebuild()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
            call etl_rpt_baseball_rebuild();
            
            call etl_rpt_bbcard_rebuild();
            
            INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
            SELECT @etl_imp_id, @client_id, 'etl_rpt_baseball_rebuild()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
            SELECT 'v_etl_baseball_rebuild_flag is 1 - Now running etl_rpt_baseball_rebuild()' AS procs, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        END IF;

        # Reorganization of BB Card processes renders call to etl_pm() obsolete
#        IF v_etl_rpt_flag = 1 OR v_etl_pm_flag = 1 THEN
#            SELECT 'etl_pm_flag is 1 - Now running etl_pm()' AS procs, convert_tz(now(), 'UTC', 'US/Eastern') AS Begin_Time;
#            INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
#            SELECT @etl_imp_id, @client_id, 'etl_pm()', 'b', v_etl_rpt_flag, v_etl_bm_build_flag;
#            call etl_pm();
#            INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
#            SELECT @etl_imp_id, @client_id, 'etl_pm()', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;
#            SELECT 'etl_pm_flag is 1 - Now running etl_pm()' AS procs, convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
#        END IF;

        SELECT convert_tz(now(), 'UTC', 'US/Eastern') AS End_Time;
        SELECT TIMEDIFF(now(), @begin_time) AS Elapsed_Time;
        INSERT tmp.etl_imp_log (etl_imp_id, client_id, action, time_code, etl_rpt_flag, etl_bm_build_flag)
        SELECT @etl_imp_id, @client_id, 'etl_imp', 'c', v_etl_rpt_flag, v_etl_bm_build_flag;

    ELSE
        SELECT 'School Year Check - Failed' AS Uploader_School_Year_Check;
    END IF; # school year check
END;
//
