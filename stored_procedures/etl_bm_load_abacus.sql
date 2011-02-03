/*
$Rev: 8553 $ 
$Author: randall.stanley $ 
$Date: 2010-05-10 11:21:53 -0400 (Mon, 10 May 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_abacus.sql $
$Id: etl_bm_load_abacus.sql 8553 2010-05-10 15:21:53Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_bm_load_abacus //

CREATE definer=`dbadmin`@`localhost` procedure etl_bm_load_abacus()
CONTAINS SQL
COMMENT '$Rev: 8553 $ $Date: 2010-05-10 11:21:53 -0400 (Mon, 10 May 2010) $'
SQL SECURITY INVOKER

BEGIN 

call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

UPDATE sam_test SET
external_grading_flag = 1
WHERE external_grading_flag = 0;

##################################
## pmi_ods_scan_results cleanup ##
##################################

select concat(database(), '_ods') into @db;


####################################
## Load sam_test and sam_test_event ##
####################################

    SET @sql_sam_test := '';
    SET @sql_sam_test := CONCAT(@sql_sam_test, ' INSERT sam_test (test_id, import_xref_code, moniker, generation_method_code, mastery_level, threshold_level, external_grading_flag, client_id, owner_id, last_user_id, purge_flag)');
    SET @sql_sam_test := CONCAT(@sql_sam_test, ' SELECT DISTINCT pmi_f_get_next_sequence_app_db(\'sam_test\', 1), at.test_id, at.test_name  ,\'e\' ,70 ,50 ,1, ', @client_id, ', ', @client_id, ', 1234, 0');
    SET @sql_sam_test := CONCAT(@sql_sam_test, ' FROM ', @db, '.pmi_ods_scan_results_abacus at ');
    SET @sql_sam_test := CONCAT(@sql_sam_test, ' WHERE NOT EXISTS (SELECT * FROM sam_test t WHERE at.test_id = t.import_xref_code)');
    
    prepare sql_sam_test from @sql_sam_test;
            
        execute sql_sam_test;
            
        deallocate prepare sql_sam_test;
        
    
    SET @sql_sam_test_event := '';
    SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' INSERT sam_test_event (test_id, test_event_id, import_xref_code, online_scoring_flag, start_date, end_date, admin_type_code, admin_period_id, purge_flag, client_id, last_user_id)');
    SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' SELECT  t.test_id, pmi_f_get_next_sequence_app_db(\'sam_test_event\', 1), min(at.test_id) ,0, min(at.start_date), max(at.end_date), \'s\',1000001, 0, ', @client_id, ', 1234');
    SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' FROM ', @db, '.pmi_ods_scan_results_abacus at');
    SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' JOIN sam_test t   ON at.test_id = t.import_xref_code');
    SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' WHERE NOT EXISTS (SELECT * FROM sam_test_event te WHERE te.test_id = t.test_id)');
    SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' GROUP BY t.test_id');
    
    prepare sql_sam_test_event from @sql_sam_test_event;
            
        execute sql_sam_test_event;
            
        deallocate prepare sql_sam_test_event;
    

    SET @sql_sam_answer_key := '';
    SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' INSERT sam_answer_key (test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, rubric_id, answer, question_label, last_user_id)');
    SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' SELECT DISTINCT t.test_id, pmi_f_get_next_sequence_app_db(\'sam_answer_key\', 1), at.test_id, 1, at.question_id, 1000003, 1000002, 1, CONCAT(\'1-\',at.question_id), 1234');
    SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' FROM ', @db, '.pmi_ods_scan_results_abacus at    JOIN    sam_test t   ON at.test_id = t.import_xref_code');
    SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' WHERE NOT EXISTS (SELECT * FROM sam_answer_key ak WHERE ak.test_id = t.test_id AND ak.section_question_num = at.question_id)');

    prepare sql_sam_answer_key from @sql_sam_answer_key;
            
        execute sql_sam_answer_key;
            
        deallocate prepare sql_sam_answer_key;

#############################################
# sam_test_student
#############################################

    SET @sql_text_sam_test_student := CONCAT('INSERT INTO sam_test_student (test_id, test_event_id, student_id, last_user_id, create_timestamp) ');
    SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'SELECT  te.test_id, te.test_event_id, s.student_id, 1234, current_timestamp ');
    SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'FROM ', @db, '.pmi_ods_scan_results_abacus AS sr ');
    SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN sam_test as t ON t.import_xref_code = sr.test_id ');
    SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN sam_test_event AS te ON te.test_id = t.test_id ');
    SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN c_student AS s ON s.student_code = sr.Student_ID ');
    SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'ON DUPLICATE KEY UPDATE last_user_id = 1234 ');
    
    prepare sql_text_sam_test_student from @sql_text_sam_test_student;
            
        execute sql_text_sam_test_student;
            
        deallocate prepare sql_text_sam_test_student;

#############################################
# sam_student_response
#############################################

    

    SET @sql_text_sam_student_response := CONCAT('INSERT INTO sam_student_response (test_id, test_event_id, student_id, test_question_id, student_answer, last_user_id, create_timestamp) ' );
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'SELECT  t.test_id, te.test_event_id, s.student_id, ak.test_question_id, srp.student_answer, 1234, current_timestamp ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'FROM    ', @db, '.pmi_ods_scan_results_abacus AS srp ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN        sam_test as t ON t.import_xref_code = srp.test_id ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    c_student AS s ON s.student_code = srp.Student_ID ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    sam_test_event AS te ON te.test_id = t.test_id ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    sam_answer_key AS ak ON ak.test_id = t.test_id AND ak.section_question_num = srp.Question_id ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'LEFT JOIN    sam_student_response AS er     ON er.test_id = t.test_id    AND  er.test_event_id = te.test_event_id    AND  er.student_id = s.student_id    AND  er.test_question_id = ak.test_question_id ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'WHERE   er.student_id is null or  er.gui_edit_flag <> 1 ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'ON DUPLICATE KEY UPDATE last_user_id = 1234, student_answer = srp.student_answer ');

    prepare sql_text_sam_student_response from @sql_text_sam_student_response;
            
        execute sql_text_sam_student_response;
            
        deallocate prepare sql_text_sam_student_response;

#############################################
# Pop student with no responses
#############################################

INSERT INTO sam_student_response (test_id, test_event_id, student_id, test_question_id, student_answer, last_user_id, create_timestamp)
SELECT dt.test_id, dt.test_event_id, dt.student_id, ak.test_question_id, 0, 1234, current_timestamp
FROM sam_test_student dt
    JOIN    sam_answer_key ak
        ON      dt.test_id = ak.test_id AND dt.student_id AND dt.student_id = 2304687
WHERE NOT EXISTS (SELECT *
                    FROM sam_student_response er
                    WHERE   er.test_id = dt.test_id
                    AND     er.student_id = dt.student_id
                    AND     er.test_question_id = ak.test_question_id);



#############################################
# sam_student_response-scoring
#############################################

DROP TABLE IF EXISTS tmp_score_cr;

CREATE TABLE tmp_score_cr
    SELECT  ak.test_id
        ,ak.test_question_id
        ,cast(cast(rl.rubric_score as signed) as char(3)) as rubric_score
        ,rl.rubric_value
    FROM    sam_answer_key as ak
    JOIN    sam_rubric as r
        ON  r.rubric_id = ak.rubric_id
    JOIN    sam_rubric_list as rl
        ON  rl.rubric_id = r.rubric_id;

create index ind_tmp_score_cr on tmp_score_cr(test_id, test_question_id, rubric_score);

UPDATE  sam_student_response as er
    JOIN    tmp_score_cr as tmp
        ON      tmp.test_id = er.test_id
        AND     tmp.test_question_id = er.test_question_id
        AND     tmp.rubric_score = coalesce(er.student_answer, '0')
SET     er.rubric_value = tmp.rubric_value,
        er.rubric_score = tmp.rubric_score
    WHERE   er.gui_edit_flag <> 1;


update sam_student_response set
rubric_value = 0
where student_answer is null;

DROP TABLE IF EXISTS tmp_score_cr;

#################
## Update Log
############

        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_scan_results_abacus', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
            prepare sql_scan_log from @sql_scan_log;
                            
                    execute sql_scan_log;
                            
                    deallocate prepare sql_scan_log;   

END;
//
