/*
$Rev: 8542 $ 
$Author: randall.stanley $ 
$Date: 2010-05-07 14:52:21 -0400 (Fri, 07 May 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_edusoft_results.sql $
$Id: etl_bm_load_edusoft_results.sql 8542 2010-05-07 18:52:21Z randall.stanley $ 
 */



DROP PROCEDURE IF EXISTS etl_bm_load_edusoft_results //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_edusoft_results()
CONTAINS SQL
COMMENT '$Rev: 8542 $ $Date: 2010-05-07 14:52:21 -0400 (Fri, 07 May 2010) $'
SQL SECURITY INVOKER

BEGIN 


    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_scan_results_pivot_edusoft';

    if @table_exists > 0 then
    
        drop table if exists tmp_test_list_results;
        create table tmp_test_list_results (test_id int(11) not null);

        set @v_bm_pad_edusoft_stu_id := pmi_f_get_etl_setting('bmEdusoftPadStuId');
    
        ##################################
        ## Fix student_id zero padding
        ##################################
    
        if @v_bm_pad_edusoft_stu_id = 'y' then

            SET @sql_text := CONCAT('UPDATE ', @db_name_ods, '.pmi_ods_scan_results_pivot_edusoft SET  student_id = lpad(student_id, 9, \'0\')');
    
            prepare sql_text from @sql_text;
            execute sql_text;
            deallocate prepare sql_text;

        end if;
        
        #############################################
        # sam_test_student - student_code
        #############################################
    
        INSERT INTO sam_test_student (test_id, test_event_id, student_id, last_user_id, create_timestamp)
        SELECT  DISTINCT te.test_id, te.test_event_id, s.student_id, 1234, current_timestamp
        FROM   v_pmi_ods_scan_results_pivot_edusoft AS sr
          JOIN   sam_test as t 
            ON t.import_xref_code =  sr.test_id
          JOIN   sam_test_event AS te 
            ON te.test_id = t.test_id AND te.purge_flag = 0
          JOIN   c_student AS s 
            ON s.student_code = sr.student_id
        ON DUPLICATE KEY UPDATE last_user_id = 1234;
            
        #############################################
        # sam_student_response - student_code
        #############################################

        INSERT INTO sam_student_response (test_id, test_event_id, student_id, test_question_id, student_answer, rubric_score, last_user_id, create_timestamp)
        SELECT  t.test_id
            ,te.test_event_id
            ,s.student_id
            ,ak.test_question_id
            ,srp.student_answer
            ,srp.points_earned
            ,1234
            ,current_timestamp
        FROM    v_pmi_ods_scan_results_pivot_edusoft AS srp
          JOIN    sam_test as t 
            ON t.import_xref_code = srp.test_id
          JOIN    c_student AS s 
            ON s.student_code = srp.Student_ID
          JOIN    sam_test_event AS te 
            ON te.test_id = t.test_id  
            and te.purge_flag = 0
          JOIN    sam_answer_key AS ak 
            ON ak.test_id = t.test_id 
            AND ak.flatfile_question_num = srp.Question
          JOIN    sam_question_type AS qt 
            ON ak.question_type_id = qt.question_type_id
          JOIN    sam_scoring_method AS sm 
            ON qt.scoring_method_id = sm.scoring_method_id
          LEFT JOIN    sam_student_response AS er     
            ON er.test_id = t.test_id    
            AND  er.test_event_id = te.test_event_id    
            AND  er.student_id = s.student_id    
            AND  er.test_question_id = ak.test_question_id
        WHERE   er.student_id is null or  er.gui_edit_flag <> 1 
        ON DUPLICATE KEY UPDATE last_user_id = 1234, student_answer = values(student_answer), rubric_score = values(rubric_score);
    
        #############################################
        # sam_student_response-scoring
        #############################################
        
        INSERT tmp_test_list_results (test_id) 
        SELECT t.test_id
        FROM    (SELECT test_id
             FROM v_pmi_ods_scan_results_pivot_edusoft AS at
           GROUP BY at.test_id) AS dt
        JOIN sam_test AS t
          ON t.import_xref_code = dt.test_id;
        
        update  sam_student_response as er
        join    tmp_test_list_results as tl
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
        join    tmp_test_list_results as tl
                on      t.test_id = tl.test_id
        set     t.force_rescore_flag = 0
        ;

        #################
        ## Cleanup
        #################
        
        SET @sql_truncate_pivot := CONCAT('TRUNCATE TABLE ', @db_name_ods, '.pmi_ods_scan_results_pivot_edusoft');
        prepare sql_truncate_pivot from @sql_truncate_pivot;
        execute sql_truncate_pivot;
        deallocate prepare sql_truncate_pivot;
    
        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_scan_results_pivot_edusoft', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;

    end if;

END;

//
