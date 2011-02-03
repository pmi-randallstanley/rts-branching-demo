/*
$Rev: 8068 $ 
$Author: randall.stanley $ 
$Date: 2010-01-04 14:18:42 -0500 (Mon, 04 Jan 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_pmi.sql $
$Id: etl_bm_load_pmi.sql 8068 2010-01-04 19:18:42Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_bm_load_pmi //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_pmi()
CONTAINS SQL
COMMENT '$Rev: 8068 $ $Date: 2010-01-04 14:18:42 -0500 (Mon, 04 Jan 2010) $'
SQL SECURITY INVOKER

BEGIN 

    drop table if exists `tmp_test_list`;

    create table `tmp_test_list` (
        `test_id` int(11) not null,
        primary key (`test_id`)
    );

##################################
## pmi_ods_scan_results cleanup ##
##################################

    SET @db := '';
    SET @db := concat(database(), '_ods');

#########################
## Build the scan_results_pivot
##########################
    
    SET @sql_text_pivot := '';
    SET @sql_text_pivot := CONCAT('call ', @db, '.etl_scan_results_pmi_pivot()');

    prepare sql_text_pivot from @sql_text_pivot;
            
        execute sql_text_pivot;
            
        deallocate prepare sql_text_pivot;
        

    # capture unique test list delivered
    insert  tmp_test_list (test_id)
    select  test_id
    from    v_pmi_ods_scan_results_internal
    where   test_id is not null
    group by test_id
    ;


#############################################
# sam_test_student
#############################################

    SET @sql_text_sam_test_student := '';
    SET @sql_text_sam_test_student := CONCAT('INSERT INTO sam_test_student (test_id, test_event_id, student_id, last_user_id, create_timestamp) ');
    SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'SELECT  te.test_id, te.test_event_id, s.student_id, 1234, current_timestamp ');
    SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'FROM ', @db, '.pmi_ods_scan_results_internal AS sr ');
    SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN sam_test as t ON t.test_id = sr.test_id ');
    SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN sam_test_event AS te ON te.test_id = t.test_id and te.test_event_id = sr.test_event_id ');
    SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN c_student AS s ON s.student_code = sr.Student_ID ');
    SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'ON DUPLICATE KEY UPDATE last_user_id = 1234 ');
    
    prepare sql_text_sam_test_student from @sql_text_sam_test_student;
            
        execute sql_text_sam_test_student;
            
        deallocate prepare sql_text_sam_test_student;
        

#############################################
# sam_student_response
#############################################

    SET @sql_text_sam_student_response := '';
    SET @sql_text_sam_student_response := CONCAT('INSERT INTO sam_student_response (test_id, test_event_id, student_id, test_question_id, student_answer, last_user_id, create_timestamp) ' );
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'SELECT  t.test_id, te.test_event_id, s.student_id, ak.test_question_id, srp.student_answer, 1234, current_timestamp ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'FROM    ', @db, '.scan_results_pivot AS srp ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN        sam_test as t ON t.test_id = srp.test_id ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    c_student AS s ON s.student_code = srp.student_ID ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    sam_test_event AS te ON te.test_id = t.test_id AND     te.test_event_id = srp.test_event_id ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    sam_answer_key AS ak ON ak.test_id = t.test_id AND     ak.flatfile_question_num = srp.question_eid ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    sam_test_student AS ts ON ts.test_id = t.test_id AND     ts.test_event_id = te.test_event_id AND    ts.student_id = s.student_id ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'LEFT JOIN    sam_student_response AS er     ON er.test_id = t.test_id    AND  er.test_event_id = te.test_event_id    AND  er.student_id = s.student_id    AND  er.test_question_id = ak.test_question_id ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'WHERE   er.student_id is null or  er.gui_edit_flag <> 1 ');
    SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'ON DUPLICATE KEY UPDATE last_user_id = 1234, student_answer = srp.student_answer ');

    prepare sql_text_sam_student_response from @sql_text_sam_student_response;
            
        execute sql_text_sam_student_response;
            
        deallocate prepare sql_text_sam_student_response;

#############################################
# sam_student_response-scoring
#############################################
    
    UPDATE sam_student_response er
        JOIN sam_answer_key ak
            ON er.test_id = ak.test_id
            AND er.test_question_id = ak.test_question_id
        JOIN sam_question_type qt
            ON ak.question_type_id = qt.question_type_id
            AND qt.question_type_code = 'gr'
    SET er.student_answer = student_answer * 1
    WHERE   er.gui_edit_flag <> 1;
    
    
    DROP TABLE IF EXISTS tmp_score_sr;
    DROP TABLE IF EXISTS tmp_score_cr;
    
    # REWRITE new pivot version to account for Gridded Response as a scoring method of 's' for SR.
    CREATE TABLE tmp_score_sr
        SELECT  ak.test_id
            ,ak.test_question_id
            ,ak.answer
            ,r.rubric_total
        FROM    sam_answer_key as ak
        JOIN    tmp_test_list as tmp1
                ON      ak.test_id = tmp1.test_id
        JOIN    sam_rubric as r
                ON  r.rubric_id = ak.rubric_id
        WHERE   ak.question_type_id = 1000001
        UNION 
        SELECT  ak.test_id
            ,ak.test_question_id
            ,(ak.answer * 1)
            ,r.rubric_total
        FROM    sam_answer_key as ak
        JOIN    tmp_test_list as tmp1
                ON      ak.test_id = tmp1.test_id
        JOIN    sam_rubric as r
            ON  r.rubric_id = ak.rubric_id
        WHERE   ak.question_type_id = 1000009;
    
    
    
    CREATE TABLE tmp_score_cr
        SELECT  ak.test_id
            ,ak.test_question_id
            ,cast(cast(rl.rubric_score as signed) as char(3)) as rubric_score
            ,rl.rubric_value
        FROM    sam_answer_key as ak
        JOIN    tmp_test_list as tmp1
                ON      ak.test_id = tmp1.test_id
        JOIN    sam_rubric_list as rl
            ON  ak.rubric_id = rl.rubric_id
        JOIN    sam_question_type as qt
                on      ak.question_type_id = qt.question_type_id
        JOIN    sam_scoring_method as sm
                on      qt.scoring_method_id = sm.scoring_method_id
                and     sm.scoring_method_code = 'c'
        ;
    
    UPDATE  sam_student_response as er
        JOIN    tmp_score_sr as tmp
            ON      tmp.test_id = er.test_id
            AND     tmp.test_question_id = er.test_question_id
    SET     rubric_value = case when er.student_answer = tmp.answer then tmp.rubric_total else 0 END,
            rubric_score = case when er.student_answer = tmp.answer then 1 else 0 end
    WHERE   er.gui_edit_flag <> 1;
    
    
    UPDATE  sam_student_response as er
        JOIN    tmp_score_cr as tmp
            ON      tmp.test_id = er.test_id
            AND     tmp.test_question_id = er.test_question_id
            AND     tmp.rubric_score = coalesce(er.student_answer, '0')
    SET     er.rubric_value = tmp.rubric_value,
            er.rubric_score = tmp.rubric_score
    WHERE   er.gui_edit_flag <> 1;
    
    
    UPDATE sam_student_response as sr
    JOIN    tmp_test_list as tmp1
            ON      sr.test_id = tmp1.test_id
    SET sr.rubric_value = 0
    WHERE sr.student_answer is null;
    
    UPDATE sam_student_response as sr
    JOIN    tmp_test_list as tmp1
            ON      sr.test_id = tmp1.test_id
    SET sr.rubric_value = 0
    WHERE sr.rubric_value IS NULL;
    
#################
## Cleanup
#################
    
    SET @sql_drop_pivot := CONCAT('DROP TABLE IF EXISTS ', @db, '.scan_results_pivot');
    prepare sql_drop_pivot from @sql_drop_pivot;
        execute sql_drop_pivot;
        deallocate prepare sql_drop_pivot;

    DROP TABLE IF EXISTS tmp_score_sr;
    DROP TABLE IF EXISTS tmp_score_cr;
    drop table if exists `tmp_test_list`;

#################
## Update Log
#################
    
    SET @sql_scan_log := '';
    SET @sql_scan_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_scan_results_internal', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');

    prepare sql_scan_log from @sql_scan_log;
                
        execute sql_scan_log;
                
        deallocate prepare sql_scan_log;   
                                                    
END;
//
