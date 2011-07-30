/*
$Rev: 8481 $ 
$Author: randall.stanley $ 
$Date: 2010-04-30 08:25:56 -0400 (Fri, 30 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_sbs_imp.sql $
$Id: etl_sbs_imp.sql 8481 2010-04-30 12:25:56Z randall.stanley $ 
*/

drop procedure if exists etl_sbs_imp//

create definer=`dbadmin`@`localhost` procedure etl_sbs_imp()
contains sql
sql security invoker
comment '$Rev: 8481 $ $Date: 2010-04-30 08:25:56 -0400 (Fri, 30 Apr 2010) $'


proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set @etl_imp_id := pmi_admin.pmi_f_get_next_sequence('etl_imp_id', 1);
    set @begin_time := now();
    select convert_tz(now(), 'UTC', 'US/Eastern') as begin_time;
    insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
    values (@etl_imp_id, @client_id, 'etl_sbs_imp', 'b', 0, 0);

    ###############################
    ## Current School Year Check ##
    ###############################
    insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
    values (@etl_imp_id, @client_id, 'sql:check school year', 'b', 0, 0);

    select  count(*)
    into    @current_school_year_valid
    from    c_school_year
    where   now() between begin_date and end_date
    and     active_flag = 1
    ;
    
    # Only run ETL load if current school year is valid
    IF @current_school_year_valid > 0 THEN

        select 'School Year Check - Passed' as uploader_school_year_check;
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'sql:check school year', 'c', 0, 0);

        ##############################
        ## Process the Uploader Log ##
        ##############################

        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'imp_process_upload_log()', 'b', 0, 0);
        
        set @sqltext := CONCAT('call ', @db_name_ods, '.imp_process_upload_log()');
    
        prepare sqltext from @sqltext;
        execute sqltext;
        deallocate prepare sqltext;  
                
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'imp_process_upload_log()', 'c', 0, 0);



        ##################################
        ## Purge Student Responses      ##
        ##################################
    
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_bm_purge_student_results()', 'b', 0, 0);
        
        call etl_bm_purge_student_results(@deletecount);
        if @deletecount > 0 then
            select 'Selected Results Purged' as sam_student_responses_purge_status;
            #set v_data_change = 1;
        else
            select 'No Results Purged' as sam_student_responses_purge_status;
        end if;
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_bm_purge_student_results()', 'c', 0, 0);



        ##################################
        ## Run etl_bm_load_pmi_pend_ola ## 
        ##################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_ola');
    
        if  @upload_id > 1 then 
            select 'New Scan Files - OLA' as uploader_scan_file_external, convert_tz(now(), 'UTC', 'US/Eastern') as begin_time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
            values (@etl_imp_id, @client_id, 'etl_bm_load_pmi_pend_ola()', 'b', 0, 0);
            call etl_bm_load_pmi_pend_ola();
            insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
            values (@etl_imp_id, @client_id, 'etl_bm_load_pmi_pend_ola()', 'c', 0, 0);
            select 'New Scan Files - OLA' as uploader_scan_file_external, convert_tz(now(), 'UTC', 'US/Eastern') as end_time;
        else 
            select 'No Scan Files - OLA' as uploader_scan_file_external;
        end if;        

        #######################################
        ## Run etl_imp_sam_lexmark_form          ##
        #######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_sam_lexmark_form');
    
        if  @upload_id > 1 then 
            select 'New SAM Lexmark Form File' as upload_sam_lexmark_form, convert_tz(now(), 'UTC', 'US/Eastern') as begin_time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
            values (@etl_imp_id, @client_id, 'etl_imp_sam_lexmark_form()', 'b', 0, 0);
            call etl_imp_sam_lexmark_form();
            insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
            values (@etl_imp_id, @client_id, 'etl_imp_sam_lexmark_form()', 'c', 0, 0);
            select 'New SAM Lexmark Form File' as upload_sam_lexmark_form, convert_tz(now(), 'UTC', 'US/Eastern') as end_time;
        else 
            select 'No SAM Lexmark Form File' as upload_sam_lexmark_form;
        end if;    

        ######################################
        ## Run etl_bm_load_lexmark_tests    ##
        ######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_lexmark_test_ak');
    
        if  @upload_id > 1 then 
            select 'New Scan Files - Lexmark Test/AK' as uploader_scan_file_lexmark_tests_ak, convert_tz(now(), 'UTC', 'US/Eastern') as begin_time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
            values (@etl_imp_id, @client_id, 'etl_bm_load_lexmark_tests()', 'b', 0, 0);
            call etl_bm_load_lexmark_tests();
            insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
            values (@etl_imp_id, @client_id, 'etl_bm_load_lexmark_tests()', 'c', 0, 0);
            select 'New Scan Files - Lexmark Test/AK' as uploader_scan_file_lexmark_tests_ak, convert_tz(now(), 'UTC', 'US/Eastern') as end_time;
        else 
            select 'No Scan Files - Lexmark Test/AK' as uploader_scan_file_lexmark_tests_ak;
        end if;    

        ######################################
        ## Run etl_bm_load_lexmark_results  ##
        ######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_lexmark');
    
        if  @upload_id > 1 then 
            select 'New Scan Files - Lexmark Results' as uploader_scan_file_lexmark, convert_tz(now(), 'UTC', 'US/Eastern') as begin_time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
            values (@etl_imp_id, @client_id, 'etl_bm_load_lexmark_results()', 'b', 0, 0);
            call etl_bm_load_lexmark_results();
            insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
            values (@etl_imp_id, @client_id, 'etl_bm_load_lexmark_results()', 'c', 0, 0);
            select 'New Scan Files - Lexmark Results' as uploader_scan_file_lexmark, convert_tz(now(), 'UTC', 'US/Eastern') as end_time;
        else 
            select 'No Scan Files - Lexmark Results' as uploader_scan_file_lexmark;
        end if;    

        ######################################
        ## Run etl_bm_load_turning_tests    ##
        ######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_turning_test_ak');
    
        if  @upload_id > 1 then 
            select 'New Scan Files - Turning Test/AK' as uploader_scan_file_turning_tests_ak, convert_tz(now(), 'UTC', 'US/Eastern') as begin_time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
            values (@etl_imp_id, @client_id, 'etl_bm_load_turning_tests()', 'b', 0, 0);
            call etl_bm_load_turning_tests();
            insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
            values (@etl_imp_id, @client_id, 'etl_bm_load_turning_tests()', 'c', 0, 0);
            select 'New Scan Files - Turning Test/AK' as uploader_scan_file_turning_tests_ak, convert_tz(now(), 'UTC', 'US/Eastern') as end_time;
        else 
            select 'No Scan Files - Turning Test/AK' as uploader_scan_file_turning_tests_ak;
        end if;    

        ######################################
        ## Run etl_bm_load_turning_results  ##
        ######################################
        call etl_imp_get_queued_id_by_table_name(@upload_id, 'pmi_ods_scan_results_turning');
    
        if  @upload_id > 1 then 
            select 'New Scan Files - Turning Results' as uploader_scan_file_turning, convert_tz(now(), 'UTC', 'US/Eastern') as begin_time;
            insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
            values (@etl_imp_id, @client_id, 'etl_bm_load_turning_results()', 'b', 0, 0);
            call etl_bm_load_turning_results();
            insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
            values (@etl_imp_id, @client_id, 'etl_bm_load_turning_results()', 'c', 0, 0);
            select 'New Scan Files - Turning Results' as uploader_scan_file_turning, convert_tz(now(), 'UTC', 'US/Eastern') as end_time;
        else 
            select 'No Scan Files - Turning Results' as uploader_scan_file_turning;
        end if;    

        ##################################
        ## Score test due to key update ##
        ##################################
    
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        select @etl_imp_id, @client_id, 'etl_bm_rescore_tests_for_key_update()', 'b', 0, 0;
        
        call etl_bm_rescore_tests_for_key_update(@rescored);
        if @rescored > 0 then
            select 'Tests Rescored' as SAM_Test_Rescore_Status;
            #set v_data_change = 1;
        else
            select 'No Tests Rescored' as SAM_Test_Rescore_Status;
        end if;
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        select @etl_imp_id, @client_id, 'etl_bm_rescore_tests_for_key_update()', 'c', 0, 0;


        #######################
        ## Report Aggregates ##
        #######################

        select 'Now running Reporting Aggregates' as `process`, convert_tz(now(), 'UTC', 'US/Eastern') as begin_time;

        # etl_rpt_bm_scores
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_bm_scores()', 'b', 0, 0);
        call etl_rpt_bm_scores();
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_bm_scores()', 'c', 0, 0);

        # etl_rpt_bm_scores_school
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_bm_scores_school()', 'b', 0, 0);
        call etl_rpt_bm_scores_school();
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_bm_scores_school()', 'c', 0, 0);

        # etl_rpt_bm_scores_school_grade
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_bm_scores_school_grade()', 'b', 0, 0);
        call etl_rpt_bm_scores_school_grade();
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_bm_scores_school_grade()', 'c', 0, 0);

        # etl_rpt_profile_leading_subject_stu
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_profile_leading_subject_stu()', 'b', 0, 0);
        call etl_rpt_profile_leading_subject_stu();
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_profile_leading_subject_stu()', 'c', 0, 0);

        # etl_rpt_profile_leading_strand_stu
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_profile_leading_strand_stu()', 'b', 0, 0);
        call etl_rpt_profile_leading_strand_stu();
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_profile_leading_strand_stu()', 'c', 0, 0);

        # etl_rpt_question_scores_district
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_question_scores_district()', 'b', 0, 0);
        call etl_rpt_question_scores_district();
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_question_scores_district()', 'c', 0, 0);

        # etl_rpt_profile_bm_stu
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_profile_bm_stu()', 'b', 0, 0);
        call etl_rpt_profile_bm_stu();
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_profile_bm_stu()', 'c', 0, 0);

        # etl_rpt_test_scores
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_test_scores()', 'b', 0, 0);
        call etl_rpt_test_scores();
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_test_scores()', 'c', 0, 0);

        # etl_rpt_bm_scores_district
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_bm_scores_district()', 'b', 0, 0);
        call etl_rpt_bm_scores_district();
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_bm_scores_district()', 'c', 0, 0);

        # etl_rpt_bm_scores_district_grade
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_bm_scores_district_grade()', 'b', 0, 0);
        call etl_rpt_bm_scores_district_grade();
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_bm_scores_district_grade()', 'c', 0, 0);

        # etl_rpt_bm_scores_class
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_bm_scores_class()', 'b', 0, 0);
        call etl_rpt_bm_scores_class();
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_bm_scores_class()', 'c', 0, 0);

        # etl_rpt_bm_scores_strand_class
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_bm_scores_strand_class()', 'b', 0, 0);
        call etl_rpt_bm_scores_strand_class();
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_bm_scores_strand_class()', 'c', 0, 0);

        # etl_rpt_test_curriculum_scores
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_test_curriculum_scores()', 'b', 0, 0);
        call etl_rpt_test_curriculum_scores();
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_rpt_test_curriculum_scores()', 'c', 0, 0);

        SELECT 'Reporting Aggregates - Completed' as `process`, convert_tz(now(), 'UTC', 'US/Eastern') as end_time;

        select convert_tz(now(), 'UTC', 'US/Eastern') as end_time;
        select timediff(now(), @begin_time) as elapsed_time;
        insert tmp.etl_imp_log (etl_imp_id, client_id, `action`, time_code, etl_rpt_flag, etl_bm_build_flag)
        values (@etl_imp_id, @client_id, 'etl_sbs_imp', 'c', 0, 0);

    else
        select 'School Year Check - Failed' as uploader_school_year_check;
    end if; # school year check


end proc;
//
