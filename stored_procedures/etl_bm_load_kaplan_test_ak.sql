/*
$Rev: 8553 $ 
$Author: randall.stanley $ 
$Date: 2010-05-10 11:21:53 -0400 (Mon, 10 May 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_kaplan_test_ak.sql $
$Id: etl_bm_load_kaplan_test_ak.sql 8553 2010-05-10 15:21:53Z randall.stanley $ 
 */


DROP PROCEDURE IF EXISTS etl_bm_load_kaplan_test_ak //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_kaplan_test_ak()
CONTAINS SQL
COMMENT '$Rev: 8553 $ $Date: 2010-05-10 11:21:53 -0400 (Mon, 10 May 2010) $'
SQL SECURITY INVOKER

BEGIN 


   call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = database()
    and     t.table_name = 'v_pmi_ods_sr_ak_kaplan';

    if @table_exists > 0 then
    
        drop table if exists tmp_test_list;
        create table tmp_test_list (test_id int(11) not null);
        drop table if exists tmp_ak_insert;
        drop table if exists tmp_al_list;

        select  external_answer_source_id
        into    @external_answer_source_id
        from    sam_external_answer_source
        where   external_answer_source_code = 'other'
        ;

        select  answer_set_id
        into    @answer_set_id
        from    sam_answer_set
        where   answer_set_code = '1234'
        ;

        select  answer_subset_id
        into    @answer_subset_id
        from    sam_answer_subset
        where   answer_set_id = @answer_set_id
        and     subset_ordinal = 1
        ;

        ######################################
        ## Load sam_test and sam_test_event ##
        ######################################
        
        INSERT sam_test (test_id, import_xref_code, moniker, answer_source_code, generation_method_code, external_answer_source_id, mastery_level, threshold_level, external_grading_flag, answer_set_id, client_id, owner_id, last_user_id, purge_flag, create_timestamp) 
        SELECT DISTINCT   pmi_f_get_next_sequence_app_db('sam_test', 1), 
                          at.test_id,
                          at.test_name,
                          'e', 
                          'e', 
                          @external_answer_source_id, 
                          70, 
                          50, 
                          0, 
                          @answer_set_id, 
                          @client_id,
                          @client_id,
                          1234, 
                          0, 
                          now() 
        FROM  v_pmi_ods_sr_ak_kaplan AS at  
        WHERE NOT EXISTS (SELECT * 
                            FROM sam_test t 
                            WHERE  at.test_id = t.import_xref_code)       
              AND at.test_id IS NOT NULL
        ON DUPLICATE KEY UPDATE last_user_id = 1234
        ;
        
        
        INSERT tmp_test_list (test_id) 
        SELECT t.test_id 
        FROM v_pmi_ods_sr_ak_kaplan AS at 
          JOIN  sam_test t ON 
            at.test_id = t.import_xref_code 
        GROUP BY t.test_id;

# we need address the sam_test_section

        INSERT sam_test_event (test_id, test_event_id, online_scoring_flag, start_date, end_date, admin_period_id, purge_flag, client_id, last_user_id, create_timestamp)
        SELECT  tmp1.test_id, pmi_f_get_next_sequence_app_db('sam_test_event', 1), 0, now(), null, 1000001, 0, @client_id, 1234, now()
        FROM    tmp_test_list AS tmp1
        LEFT JOIN   sam_test_event AS te
                ON      te.test_id = tmp1.test_id
        WHERE   te.test_event_id is null
        ;

            
        ##################################
        ## Build the answer key
        ##################################
        
        CREATE TABLE tmp_ak_insert
        SELECT DISTINCT t.test_id, pmi_f_get_next_sequence_app_db('sam_answer_key', 1) as test_question_id
           ,at.question_number as import_xref_code, 1 as section_num, at.question_number as section_question_num
           ,1000001 as question_type_id,  1000002 as rubric_id, CONCAT('1-',at.question_number) as question_label, at.question_number as flatfile_question_num, at.answer
        FROM   (SELECT distinct test_id, question_number, answer
             FROM v_pmi_ods_sr_ak_kaplan
             WHERE answer IS NOT NULL) at
        JOIN    sam_test t   
          ON      at.test_id = t.import_xref_code
        WHERE NOT EXISTS (SELECT * FROM sam_answer_key ak WHERE ak.test_id = t.test_id);
        

        INSERT sam_answer_key (test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, rubric_id, answer_set_id, answer_subset_id, question_label, flatfile_question_num, answer, last_user_id, create_timestamp)    
        SELECT  test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, rubric_id, @answer_set_id, @answer_subset_id, question_label, flatfile_question_num, answer, 1234, now()
        FROM    tmp_ak_insert
        ;


   
        #################
        ## Update Log  : 
        #################
            
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_sr_ak_kaplan', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;


    end if;

END;
//
