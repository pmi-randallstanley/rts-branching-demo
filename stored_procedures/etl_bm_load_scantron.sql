/*
$Rev: 9335 $ 
$Author: randall.stanley $ 
$Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_scantron.sql $
$Id: etl_bm_load_scantron.sql 9335 2010-10-03 18:10:23Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_bm_load_scantron //

CREATE definer=`dbadmin`@`localhost` procedure etl_bm_load_scantron()
CONTAINS SQL
COMMENT '$Rev: 9335 $ $Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $'
SQL SECURITY INVOKER

BEGIN 
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_scan_results_pivot_scantron';

    if @view_exists > 0 then
    
        drop table if exists tmp_test_list;
        drop table if exists tmp_ak_insert;
        drop table if exists tmp_test_event;

        create table tmp_test_list (test_id int(11) not null);

        CREATE TABLE `tmp_test_event` (
          `test_id` int(10) NOT NULL,
          `test_event_id` int(10)  NULL,
          PRIMARY KEY  (`test_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;


        select  external_answer_source_id
        into    @external_answer_source_id
        from    sam_external_answer_source
        where   external_answer_source_code = 'scantron'
        ;

        select  answer_set_id
        into    @answer_set_id
        from    sam_answer_set
        where   answer_set_code = 'abcd'
        ;

        select  answer_subset_id
        into    @answer_subset_id
        from    sam_answer_subset
        where   answer_set_id = @answer_set_id
        and     subset_ordinal = 1
        ;
    
        ####################################
        ## Load sam_test and sam_test_event ##
        ####################################
        /*
        SET @sql_sam_test := CONCAT( ' INSERT sam_test (test_id, import_xref_code, moniker, answer_source_code, generation_method_code, external_answer_source_id, mastery_level, threshold_level, external_grading_flag, answer_set_id, owner_id, last_user_id, purge_flag, create_timestamp)'
                                    ,' SELECT DISTINCT pmi_f_get_next_sequence_app_db(\'sam_test\', 1), at.test_name, at.test_name, \'e\', \'e\', @external_answer_source_id, 70, 50, 0, @answer_set_id, @client_id, 1234, 0, now()'
                                    ,' FROM   ', @db_name_ods, '.pmi_ods_scan_results_pivot_scantron at'
                                    ,' WHERE at.test_name IS NOT NULL AND'
                                    ,' NOT EXISTS (SELECT * FROM sam_test t WHERE at.test_name = t.import_xref_code)');
    
        prepare sql_sam_test from @sql_sam_test;
        execute sql_sam_test;
        deallocate prepare sql_sam_test;
        */    
        SET @sql_sam_test_list := CONCAT( ' INSERT tmp_test_list (test_id)'
                                    ,' SELECT t.test_id'
                                    ,' FROM   ', @db_name_ods, '.pmi_ods_scan_results_pivot_scantron at'
                                    ,' JOIN   sam_test t ON at.test_name = t.import_xref_code'
                                    ,' GROUP BY t.test_id');

        prepare sql_sam_test_list from @sql_sam_test_list;
        execute sql_sam_test_list;
        deallocate prepare sql_sam_test_list;

        # Need new test event created based on following rules
        # 1. If no events exist, create one 
        # 2. If events exist and they are for OLA admin, create new event 
        # 3. If events exist and they are non OLA, use that event 
        # 4. If there are more than 1 non OLA event, use first one selected

        # This statement finds the minimum event id for a given test
        # that is not desigated as an OLA event. If no event exists
        # for this criteria, a NULL will be returned and a new id
        # will be subsequently generated below for the associated Test.

        insert tmp_test_event (
            test_id
            ,test_event_id
        )
        
        select  tl.test_id
            ,min(test_event_id)
            
        from    tmp_test_list as tl
        left join   sam_test_event as te
                on      tl.test_id = te.test_id
                and     te.purge_flag = 0
                and     te.ola_flag = 0
        group by tl.test_id
        ;
        
        insert  tmp_test_event (test_id
            , test_event_id
        )
        select  test_id
            ,pmi_f_get_next_sequence_app_db('sam_test_event', 1)
        from    tmp_test_event as t
        where   test_event_id is null
        on duplicate key update test_event_id = values(test_event_id)
        ;

        INSERT sam_test_event (test_id, test_event_id, online_scoring_flag, start_date, end_date, admin_period_id, purge_flag, client_id, last_user_id, create_timestamp)
        SELECT  tmp1.test_id, tmp1.test_event_id, 0, now(), null, 1000001, 0, @client_id, 1234, now()
        FROM    tmp_test_event as tmp1
        LEFT JOIN   sam_test_event as te
                ON      te.test_id = tmp1.test_id
                AND     te.test_event_id = tmp1.test_event_id
        WHERE   te.test_event_id is null
        ;

        SET @sql_sam_answer_key := CONCAT(' CREATE TABLE tmp_ak_insert '
                                    ,' SELECT DISTINCT t.test_id, pmi_f_get_next_sequence_app_db(\'sam_answer_key\', 1) as test_question_id, at.question as import_xref_code, 1 as section_num  '
                                    ,'        ,at.question as section_question_num, CASE WHEN at.student_response = \'N/A\' THEN 1000003 ELSE 1000001 END as question_type_id, 1000002 as rubric_id '
                                    ,'        ,CASE WHEN at.student_response = \'N/A\' THEN null ELSE \'1\' END as answer, CONCAT(at.question, \' (\',at.question_pp,\')\') as question_label,  at.question as flatfile_question_num'
                                    ,' FROM   ', @db_name_ods, '.pmi_ods_scan_results_pivot_scantron at'
                                    ,' JOIN   sam_test t   ON at.test_name = t.import_xref_code'
                                    ,' WHERE NOT EXISTS (SELECT * FROM sam_answer_key ak WHERE ak.test_id = t.test_id AND ak.section_question_num = at.question)');

        prepare sql_sam_answer_key from @sql_sam_answer_key;
        execute sql_sam_answer_key;
        deallocate prepare sql_sam_answer_key;
    
        INSERT sam_answer_key (test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, rubric_id, answer, answer_set_id, answer_subset_id, question_label, last_user_id, create_timestamp)
        SELECT  test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, rubric_id, answer, @answer_set_id, @answer_subset_id, question_label, 1234, now()
        FROM    tmp_ak_insert
        ;
        
        #############################################
        # sam_test_student
        #############################################
        SET @sql_text_sam_test_student := CONCAT('INSERT INTO sam_test_student (test_id, test_event_id, student_id, last_user_id, create_timestamp) '
                                    ,' SELECT  te.test_id, te.test_event_id, s.student_id, 1234, current_timestamp '
                                    ,' FROM   ', @db_name_ods, '.pmi_ods_scan_results_pivot_scantron AS at ' 
                                    ,' JOIN   sam_test t            ON at.test_name = t.import_xref_code'
                                    ,' JOIN   tmp_test_event AS te  ON te.test_id = t.test_id '
                                    ,' JOIN   c_student AS s       ON s.student_code = at.student_id'
                                    ,' GROUP BY te.test_id, te.test_event_id, s.student_id '
                                    ,' ON DUPLICATE KEY UPDATE last_user_id = 1234 ');

        prepare sql_text_sam_test_student from @sql_text_sam_test_student;
        execute sql_text_sam_test_student;
        deallocate prepare sql_text_sam_test_student;
    
    
        #############################################
        # sam_student_response
        #############################################
    
        SET @sql_text_sam_student_response := CONCAT('INSERT INTO sam_student_response (test_id, test_event_id, student_id, test_question_id, student_answer, rubric_score, last_user_id, create_timestamp) ' 
                                    ,' SELECT  t.test_id, te.test_event_id, s.student_id, ak.test_question_id, case when sm.scoring_method_code = \'c\' then null else at.student_response end, '
                                    ,'         case when sm.scoring_method_code in (\'s\',\'g\') and at.student_response = ak.answer then 1 when sm.scoring_method_code in (\'s\',\'g\') then 0 else at.question_pe end, '
                                    ,'         1234, current_timestamp '
                                    ,' FROM   ', @db_name_ods, '.pmi_ods_scan_results_pivot_scantron AS at '
                                    ,' JOIN   sam_test AS t        ON at.test_name = t.import_xref_code'
                                    ,' JOIN   c_student AS s      ON s.student_code = at.student_id'
                                    ,' JOIN   tmp_test_event AS te ON te.test_id = t.test_id '
                                    ,' JOIN   sam_answer_key AS ak ON ak.test_id = t.test_id AND ak.section_question_num = at.question '
                                    ,' JOIN   sam_question_type AS qt ON ak.question_type_id = qt.question_type_id '
                                    ,' JOIN   sam_scoring_method AS sm ON qt.scoring_method_id = sm.scoring_method_id '
                                    ,' LEFT JOIN    sam_student_response AS er     ON er.test_id = t.test_id    AND  er.test_event_id = te.test_event_id    AND  er.student_id = s.student_id    AND  er.test_question_id = ak.test_question_id '
                                    ,' WHERE   er.student_id is null or  er.gui_edit_flag <> 1 '
                                    ,' ON DUPLICATE KEY UPDATE last_user_id = 1234, student_answer = values(student_answer), rubric_score = values(rubric_score) ');
    
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

        SET @sql_drop_pivot := CONCAT('TRUNCATE TABLE ', @db_name_ods, '.pmi_ods_scan_results_pivot_scantron');
       
        prepare sql_drop_pivot from @sql_drop_pivot;
        execute sql_drop_pivot;
        deallocate prepare sql_drop_pivot;
    
        #################
        ## Update Log
        #################
    
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_scan_results_pivot_scantron', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
            
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;   

        drop table if exists tmp_test_list;
        drop table if exists tmp_ak_insert;

    end if;
END;
//
