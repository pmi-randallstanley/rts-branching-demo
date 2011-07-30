/*
$Rev: 8553 $ 
$Author: randall.stanley $ 
$Date: 2010-05-10 11:21:53 -0400 (Mon, 10 May 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_non_pmi.sql $
$Id: etl_bm_load_non_pmi.sql 8553 2010-05-10 15:21:53Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_bm_load_non_pmi //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_non_pmi()
CONTAINS SQL
COMMENT '$Rev: 8553 $ $Date: 2010-05-10 11:21:53 -0400 (Mon, 10 May 2010) $'
SQL SECURITY INVOKER

BEGIN 

   DECLARE v_useMonikerForImportXrefCode char(1) default 'y';
    
    
    SET @useMonikerForImportXrefCode := pmi_f_get_etl_setting('useMonikerForImportXrefCode');
    
    IF @useMonikerForImportXrefCode = 'y' THEN 
        SET v_useMonikerForImportXrefCode = 'n';
    END IF;
    
    IF v_useMonikerForImportXrefCode = 'n' 
        THEN 
            UPDATE sam_test SET
            import_xref_code = moniker
            where import_xref_code IS NULL
            AND     purge_flag = 0;
        ELSE
            UPDATE sam_test SET
            import_xref_code = test_id
            WHERE import_xref_code IS NULL
            AND     purge_flag = 0;
    END IF;


    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_ods
    and     t.table_name = 'pmi_ods_scan_results_external';

    if @table_exists > 0 then

        ####################################
        ## Load sam_test and sam_test_event ##
        ####################################
    
        SET @sql_sam_test := '';
        SET @sql_sam_test := CONCAT(@sql_sam_test, ' INSERT sam_test (test_id, import_xref_code, moniker, generation_method_code, mastery_level, threshold_level, external_grading_flag, client_id, owner_id, last_user_id, purge_flag)');
        SET @sql_sam_test := CONCAT(@sql_sam_test, ' SELECT DISTINCT pmi_f_get_next_sequence_app_db(\'sam_test\', 1), at.test_id ,COALESCE(at.test_name, at.test_id), \'e\', 70, 50, 0, ', @client_id, ', ', @client_id, ', 1234, 0');
        SET @sql_sam_test := CONCAT(@sql_sam_test, ' FROM ', @db_name_ods, '.pmi_ods_scan_results_external at ');
        SET @sql_sam_test := CONCAT(@sql_sam_test, ' WHERE NOT EXISTS (SELECT * FROM sam_test t WHERE COALESCE(at.test_name, at.test_id) = t.import_xref_code) AND COALESCE(at.test_name, at.test_id) IS NOT NULL');
        
        prepare sql_sam_test from @sql_sam_test;
                
            execute sql_sam_test;
                
            deallocate prepare sql_sam_test;
            
        
        SET @sql_sam_test_event := '';
        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' INSERT sam_test_event (test_id, test_event_id, import_xref_code, online_scoring_flag, admin_type_code, admin_period_id, purge_flag, client_id, last_user_id)');
        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' SELECT DISTINCT t.test_id, pmi_f_get_next_sequence_app_db(\'sam_test_event\', 1),at.test_id,0,\'s\',1000001, 0, ', @client_id, ', 1234 ');
        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' FROM ', @db_name_ods, '.pmi_ods_scan_results_external at');
        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' JOIN sam_test t   ON COALESCE(at.test_name, at.test_id) = t.import_xref_code');
        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' WHERE NOT EXISTS (SELECT * FROM sam_test_event te WHERE te.test_id = t.test_id)');
        
        prepare sql_sam_test_event from @sql_sam_test_event;
                
            execute sql_sam_test_event;
                
            deallocate prepare sql_sam_test_event;
    
        #########################
        ## Build the scan_results_pivot
        ##########################
        
       SET @sql_text_pivot := CONCAT('call ', @db_name_ods, '.etl_scan_results_non_pmi_pivot()');
    
        prepare sql_text_pivot from @sql_text_pivot;
                    
                execute sql_text_pivot;
                    
                deallocate prepare sql_text_pivot;  
    
        SET @sql_sam_answer_key := '';
        SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' INSERT sam_answer_key (test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, rubric_id, question_label, flatfile_question_num, last_user_id)');
        SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' SELECT DISTINCT t.test_id, pmi_f_get_next_sequence_app_db(\'sam_answer_key\', 1), at.test_id, 1, at.question_eid, 1000001, 1000002, CONCAT(\'1-\',at.question_eid), at.question_eid, 1234');
        SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' FROM (SELECT distinct test_id, question_eid FROM ', @db_name_ods, '.scan_results_pivot WHERE student_answer IS NOT NULL) at   JOIN    sam_test t   ON at.test_id = t.import_xref_code');
        SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' WHERE NOT EXISTS (SELECT * FROM sam_answer_key ak WHERE ak.test_id = t.test_id)');
    
        prepare sql_sam_answer_key from @sql_sam_answer_key;
                
            execute sql_sam_answer_key;
                
            deallocate prepare sql_sam_answer_key;
    
    
    
        #############################################
        # sam_test_student - student_code
        #############################################
    
        SET @sql_text_sam_test_student := '';
        SET @sql_text_sam_test_student := CONCAT('INSERT INTO sam_test_student (test_id, test_event_id, student_id, last_user_id, create_timestamp)');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'SELECT  te.test_id, te.test_event_id, s.student_id, 1234, current_timestamp ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'FROM ', @db_name_ods, '.pmi_ods_scan_results_external AS sr ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN sam_test as t ON t.import_xref_code = coalesce(sr.test_name, sr.test_id) ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN sam_test_event AS te ON te.test_id = t.Test_id and te.purge_flag = 0 ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN c_student AS s ON s.student_code = sr.Student_ID ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'ON DUPLICATE KEY UPDATE last_user_id = 1234 ');
        
        prepare sql_text_sam_test_student from @sql_text_sam_test_student;
                
            execute sql_text_sam_test_student;
                
            deallocate prepare sql_text_sam_test_student;
            
        #############################################
        # sam_student_response - student_code
        #############################################
    
        SET @sql_text_sam_student_response := '';
        SET @sql_text_sam_student_response := CONCAT('INSERT INTO sam_student_response (test_id, test_event_id, student_id, test_question_id, student_answer, last_user_id, create_timestamp) ' );
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'SELECT  t.test_id, te.test_event_id, s.student_id, ak.test_question_id, lower(srp.student_answer), 1234, current_timestamp ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'FROM    ', @db_name_ods, '.scan_results_pivot AS srp ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN        sam_test as t ON t.import_xref_code = srp.test_id ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    c_student AS s ON s.student_code = srp.Student_ID ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    sam_test_event AS te ON te.test_id = t.test_id  and te.purge_flag = 0 ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    sam_answer_key AS ak ON ak.test_id = t.test_id AND ak.flatfile_question_num = srp.Question_EID ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'LEFT JOIN    sam_student_response AS er     ON er.test_id = t.test_id    AND  er.test_event_id = te.test_event_id    AND  er.student_id = s.student_id    AND  er.test_question_id = ak.test_question_id ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'WHERE   er.student_id is null or  er.gui_edit_flag <> 1 ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'ON DUPLICATE KEY UPDATE last_user_id = 1234, student_answer = srp.student_answer  ');
    
        prepare sql_text_sam_student_response from @sql_text_sam_student_response;
                
            execute sql_text_sam_student_response;
                
            deallocate prepare sql_text_sam_student_response;
    
            
        #############################################
        # sam_test_student - student_state_code
        #############################################
    
        SET @sql_text_sam_test_student := '';
        SET @sql_text_sam_test_student := CONCAT('INSERT INTO sam_test_student (test_id, test_event_id, student_id, last_user_id, create_timestamp)');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'SELECT  te.test_id, te.test_event_id, s.student_id, 1234, current_timestamp ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'FROM ', @db_name_ods, '.pmi_ods_scan_results_external AS sr ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN sam_test as t ON t.import_xref_code = coalesce(sr.test_name, sr.test_id) ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN sam_test_event AS te ON te.test_id = t.Test_id and te.purge_flag = 0 ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN c_student AS s ON s.student_state_code = sr.Student_ID ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'ON DUPLICATE KEY UPDATE last_user_id = 1234 ');
        
        prepare sql_text_sam_test_student from @sql_text_sam_test_student;
                
            execute sql_text_sam_test_student;
                
            deallocate prepare sql_text_sam_test_student;
            
        #############################################
        # sam_student_response - student_state_code
        #############################################
    
        SET @sql_text_sam_student_response := '';
        SET @sql_text_sam_student_response := CONCAT('INSERT INTO sam_student_response (test_id, test_event_id, student_id, test_question_id, student_answer, last_user_id, create_timestamp) ' );
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'SELECT  t.test_id, te.test_event_id, s.student_id, ak.test_question_id, lower(srp.student_answer), 1234, current_timestamp ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'FROM    ', @db_name_ods, '.scan_results_pivot AS srp ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN        sam_test as t ON t.import_xref_code = srp.test_id ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    c_student AS s ON s.student_state_code = srp.Student_ID ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    sam_test_event AS te ON te.test_id = t.test_id  and te.purge_flag = 0 ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    sam_answer_key AS ak ON ak.test_id = t.test_id AND ak.flatfile_question_num = srp.Question_EID ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'LEFT JOIN    sam_student_response AS er     ON er.test_id = t.test_id    AND  er.test_event_id = te.test_event_id    AND  er.student_id = s.student_id    AND  er.test_question_id = ak.test_question_id ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'WHERE   er.student_id is null or  er.gui_edit_flag <> 1 ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'ON DUPLICATE KEY UPDATE last_user_id = 1234, student_answer = srp.student_answer  ');
    
        prepare sql_text_sam_student_response from @sql_text_sam_student_response;
                
            execute sql_text_sam_student_response;
                
            deallocate prepare sql_text_sam_student_response;
            
        #############################################
        # sam_student_response-scoring
        #############################################
        
        DROP TABLE IF EXISTS tmp_score_sr;
        DROP TABLE IF EXISTS tmp_score_cr;
        
        
        CREATE TABLE tmp_score_sr
            SELECT  ak.test_id
                ,ak.test_question_id
                ,ak.answer
                ,r.rubric_total
            FROM    sam_answer_key as ak
            JOIN    sam_rubric as r
                    ON  r.rubric_id = ak.rubric_id
            JOIN    sam_test t
                    ON  t.test_id = ak.test_id
                    AND t.external_grading_flag = 0        
            WHERE   ak.question_type_id = 1000001;
        
        
        
        CREATE TABLE tmp_score_cr
            SELECT  ak.test_id
                ,ak.test_question_id
                ,cast(cast(rl.rubric_score as signed) as char(3)) as rubric_score
                ,rl.rubric_value
            FROM    sam_answer_key as ak
            JOIN    sam_rubric as r
                    ON  r.rubric_id = ak.rubric_id
            JOIN    sam_rubric_list as rl
                    ON  rl.rubric_id = r.rubric_id
            JOIN    sam_test t
                    ON  t.test_id = ak.test_id
                    AND t.external_grading_flag = 0
            WHERE   ak.question_type_id > 1000002;
        
        
        UPDATE  sam_student_response as er
            JOIN    tmp_score_sr as tmp
                ON      tmp.test_id = er.test_id
                AND     tmp.test_question_id = er.test_question_id
        SET     rubric_value =
                case when er.student_answer = tmp.answer then tmp.rubric_total else 0 end
        WHERE   er.gui_edit_flag <> 1;
        
        
        UPDATE  sam_student_response as er
            JOIN    tmp_score_cr as tmp
                ON      tmp.test_id = er.test_id
                AND     tmp.test_question_id = er.test_question_id
                AND     tmp.rubric_score = coalesce(er.student_answer, er.rubric_score)
        SET     er.rubric_value = tmp.rubric_value,
                er.rubric_score = tmp.rubric_score
        WHERE   er.gui_edit_flag <> 1;
        
        
        UPDATE sam_student_response er
            JOIN    sam_answer_key ak
                ON     er.test_id = ak.test_id
                AND    er.test_question_id = ak.test_question_id
        SET     er.rubric_value = 0
        WHERE student_answer is null
        AND   ak.question_type_id = 1000001;
        
        UPDATE sam_student_response SET
        rubric_value = 0
        WHERE rubric_value IS NULL;
        
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
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_scan_results_external', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
                                                    
            execute sql_scan_log;
                    
            deallocate prepare sql_scan_log;   

    end if;
    
END;
//
