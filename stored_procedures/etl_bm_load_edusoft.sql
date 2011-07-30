/*
$Rev: 8472 $ 
$Author: randall.stanley $ 
$Date: 2010-04-29 16:01:54 -0400 (Thu, 29 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_edusoft.sql $
$Id: etl_bm_load_edusoft.sql 8472 2010-04-29 20:01:54Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_bm_load_edusoft //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_edusoft()
CONTAINS SQL
COMMENT '$Rev: 8472 $ $Date: 2010-04-29 16:01:54 -0400 (Thu, 29 Apr 2010) $'
SQL SECURITY INVOKER

BEGIN 


    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_ods
    and     t.table_name = 'pmi_ods_scan_results_edusoft';

    if @table_exists > 0 then
    
        drop table if exists tmp_test_list;
        create table tmp_test_list (test_id int(11) not null);
        drop table if exists tmp_ak_insert;

        select  external_answer_source_id
        into    @external_answer_source_id
        from    sam_external_answer_source
        where   external_answer_source_code = 'edusoft'
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
    
        SET @sql_sam_test := '';
        SET @sql_sam_test := CONCAT(@sql_sam_test, ' INSERT sam_test (test_id, import_xref_code, moniker, answer_source_code, generation_method_code, external_answer_source_id, mastery_level, threshold_level, external_grading_flag, answer_set_id, owner_id, last_user_id, purge_flag, create_timestamp)');
        SET @sql_sam_test := CONCAT(@sql_sam_test, ' SELECT DISTINCT pmi_f_get_next_sequence_app_db(\'sam_test\', 1), at.test_id ,at.test_name, \'e\', \'e\', @external_answer_source_id, 70, 50, 1, @answer_set_id, @client_id, 1234, 0, now()');
        SET @sql_sam_test := CONCAT(@sql_sam_test, ' FROM ', @db_name_ods, '.pmi_ods_scan_results_edusoft at ');
        SET @sql_sam_test := CONCAT(@sql_sam_test, ' WHERE NOT EXISTS (SELECT * FROM sam_test t WHERE  at.test_id = t.import_xref_code) ');
        SET @sql_sam_test := CONCAT(@sql_sam_test, '      AND at.test_id IS NOT NULL');   
        
        prepare sql_sam_test from @sql_sam_test;
        execute sql_sam_test;
        deallocate prepare sql_sam_test;
            
        SET @sql_sam_test_list := CONCAT( ' INSERT tmp_test_list (test_id)'
                                    ,' SELECT t.test_id'
                                    ,' FROM   ', @db_name_ods, '.pmi_ods_scan_results_edusoft at'
                                    ,' JOIN   sam_test t ON at.test_id = t.import_xref_code'
                                    ,' GROUP BY t.test_id');

        prepare sql_sam_test_list from @sql_sam_test_list;
        execute sql_sam_test_list;
        deallocate prepare sql_sam_test_list;
        

        INSERT sam_test_event (test_id, test_event_id, online_scoring_flag, start_date, end_date, last_user_id, admin_period_id, purge_flag, create_timestamp)
        SELECT  tmp1.test_id, pmi_f_get_next_sequence_app_db('sam_test_event', 1), 0, now(), null, 1234, 1000001, 0, now()
        FROM    tmp_test_list as tmp1
        LEFT JOIN   sam_test_event as te
                ON      te.test_id = tmp1.test_id
        WHERE   te.test_event_id is null
        ;

#        SET @sql_sam_test_event := '';
#        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' INSERT sam_test_event (test_id, test_event_id, import_xref_code, online_scoring_flag, admin_type_code, last_user_id, admin_period_id, purge_flag, create_timestamp)');
#        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' SELECT DISTINCT t.test_id, pmi_f_get_next_sequence_app_db(\'sam_test_event\', 1),at.test_id,0,\'s\', 1234, 1000001, 0, now()');
#        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' FROM ', @db_name_ods, '.pmi_ods_scan_results_edusoft at');
#        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' JOIN sam_test t   ON  at.test_id = t.import_xref_code');
#        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' WHERE NOT EXISTS (SELECT * FROM sam_test_event te WHERE te.test_id = t.test_id)');
#        
#        prepare sql_sam_test_event from @sql_sam_test_event;
#        execute sql_sam_test_event;
#        deallocate prepare sql_sam_test_event;
    
        ##################################
        ## Fix student_id zero padding
        ##################################
    
        SET @sql_text := CONCAT('UPDATE ', @db_name_ods, '.pmi_ods_scan_results_edusoft SET  student_id = lpad(student_id, 9, \'0\')');
    
        prepare sql_text from @sql_text;
        execute sql_text;
        deallocate prepare sql_text;
    
        ##################################
        ## Build the scan_results_pivot
        ##################################
        
        SET @sql_text_pivot := CONCAT('call ', @db_name_ods, '.etl_scan_results_edusoft_pivot()');
    
        prepare sql_text_pivot from @sql_text_pivot;
        execute sql_text_pivot;
        deallocate prepare sql_text_pivot;  
    
        SET @sql_sam_answer_key := '';
        SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' CREATE TABLE tmp_ak_insert ');
        SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' SELECT DISTINCT t.test_id, pmi_f_get_next_sequence_app_db(\'sam_answer_key\', 1) as test_question_id ');
        SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, '    ,at.question_eid as import_xref_code, 1 as section_num, at.question_eid as section_question_num ');
        SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, '    ,1000001 as question_type_id, 1000002 as rubric_id, CONCAT(\'1-\',at.question_eid) as question_label, at.question_eid as flatfile_question_num ');
        SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' FROM   (SELECT distinct test_id, question_eid ');
        SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, '      FROM ', @db_name_ods, '.scan_results_pivot ');
        SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, '      WHERE student_answer IS NOT NULL) at   ');
        SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' JOIN    sam_test t   ON at.test_id = t.test_id');
        SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' WHERE NOT EXISTS (SELECT * FROM sam_answer_key ak WHERE ak.test_id = t.test_id)');
    
        prepare sql_sam_answer_key from @sql_sam_answer_key;
        execute sql_sam_answer_key;
        deallocate prepare sql_sam_answer_key;


        INSERT sam_answer_key (test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, rubric_id, answer_set_id, answer_subset_id, question_label, flatfile_question_num, last_user_id, create_timestamp)    
        SELECT  test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, rubric_id, @answer_set_id, @answer_subset_id, question_label, flatfile_question_num, 1234, now()
        FROM    tmp_ak_insert
        ;

        #############################################
        # sam_test_student - student_code
        #############################################
    
        SET @sql_text_sam_test_student := '';
        SET @sql_text_sam_test_student := CONCAT('INSERT INTO sam_test_student (test_id, test_event_id, student_id, last_user_id, create_timestamp)');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, ' SELECT  te.test_id, te.test_event_id, s.student_id, 1234, current_timestamp ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, ' FROM   ', @db_name_ods, '.pmi_ods_scan_results_edusoft AS sr ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, ' JOIN   sam_test as t ON t.import_xref_code =  sr.test_id ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, ' JOIN   sam_test_event AS te ON te.test_id = t.test_id AND te.purge_flag = 0 ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, ' JOIN   c_student AS s ON s.student_code = sr.student_id ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, ' ON DUPLICATE KEY UPDATE last_user_id = 1234 ');
        
        prepare sql_text_sam_test_student from @sql_text_sam_test_student;
        execute sql_text_sam_test_student;
        deallocate prepare sql_text_sam_test_student;
            
        #############################################
        # sam_student_response - student_code
        #############################################
    
        SET @sql_text_sam_student_response := '';
        SET @sql_text_sam_student_response := CONCAT('INSERT INTO sam_student_response (test_id, test_event_id, student_id, test_question_id, student_answer, rubric_score, last_user_id, create_timestamp) ' );
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' SELECT  t.test_id, te.test_event_id, s.student_id, ak.test_question_id, case when sm.scoring_method_code = \'c\' then null else srp.student_answer end ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, '         ,case when sm.scoring_method_code in (\'s\',\'g\') and srp.student_answer = ak.answer then 1 when sm.scoring_method_code in (\'s\',\'g\') then 0 else srp.student_answer end, 1234, current_timestamp ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' FROM    ', @db_name_ods, '.scan_results_pivot AS srp ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' JOIN    sam_test as t ON t.test_id = srp.test_id ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' JOIN    c_student AS s ON s.student_code = srp.Student_ID ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' JOIN    sam_test_event AS te ON te.test_id = t.test_id  and te.purge_flag = 0 ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' JOIN    sam_answer_key AS ak ON ak.test_id = t.test_id AND ak.flatfile_question_num = srp.Question_EID ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' JOIN    sam_question_type AS qt ON ak.question_type_id = qt.question_type_id ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' JOIN    sam_scoring_method AS sm ON qt.scoring_method_id = sm.scoring_method_id ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' LEFT JOIN    sam_student_response AS er     ON er.test_id = t.test_id    AND  er.test_event_id = te.test_event_id    AND  er.student_id = s.student_id    AND  er.test_question_id = ak.test_question_id ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' WHERE   er.student_id is null or  er.gui_edit_flag <> 1 ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' ON DUPLICATE KEY UPDATE last_user_id = 1234, student_answer = values(student_answer), rubric_score = values(rubric_score) ');
    
        prepare sql_text_sam_student_response from @sql_text_sam_student_response;
        execute sql_text_sam_student_response;
        deallocate prepare sql_text_sam_student_response;
    
            
            
        #############################################
        # sam_student_response-scoring
        #############################################
        
        update  sam_student_response as er
        join    tmp_test_list as tl
                on      er.test_id = tl.test_id
        join    sam_answer_key as ak
                on      er.test_id = ak.test_id
                and     er.test_question_id = ak.test_question_id
        join    sam_rubric_list as rl
                on      ak.rubric_id = rl.rubric_id
                and     er.rubric_score = rl.rubric_score
        set     er.rubric_value = rl.rubric_value
        ;
        
        update  sam_test as t
        join    tmp_test_list as tl
                on      t.test_id = tl.test_id
        set     t.force_rescore_flag = 0
        ;
    
        #################
        ## Cleanup
        #################
        
        SET @sql_drop_pivot := CONCAT('DROP TABLE IF EXISTS ', @db_name_ods, '.scan_results_pivot');
        prepare sql_drop_pivot from @sql_drop_pivot;
        execute sql_drop_pivot;
        deallocate prepare sql_drop_pivot;
    
        DROP TABLE IF EXISTS tmp_score_sr;
        DROP TABLE IF EXISTS tmp_score_cr;
    
        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_scan_results_edusoft', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;

    end if;

END;
//
