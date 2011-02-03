
/*
$Rev: 8553 $ 
$Author: randall.stanley $ 
$Date: 2010-05-10 11:21:53 -0400 (Mon, 10 May 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_edmin.sql $
$Id: etl_bm_load_edmin.sql 8553 2010-05-10 15:21:53Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_bm_load_edmin //

CREATE definer=`dbadmin`@`localhost` procedure etl_bm_load_edmin()
CONTAINS SQL
COMMENT '$Rev: 8553 $ $Date: 2010-05-10 11:21:53 -0400 (Mon, 10 May 2010) $'
SQL SECURITY INVOKER

BEGIN 

 call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_scan_results_pivot_edmin';

    if @view_exists > 0 then

        drop table if exists tmp_ak_insert;
        drop table if exists tmp_test_list;
        drop table if exists tmp_edmin_question_list;
        create table tmp_test_list (test_id int(11) not null);

        select  min(case when question_type_code = 'nr' then question_type_id end)
                ,min(case when question_type_code = 'chr' then question_type_id end)
        into    @normal_response_id
                ,@challenge_response_id
                
        from    sam_question_type
        where   question_type_code in ('chr','nr')
        ;
        
        select  external_answer_source_id
        into    @external_answer_source_id
        from    sam_external_answer_source
        where   external_answer_source_code = 'edmin'
        ;
            
        ####################################
        ## Load sam_test and sam_test_event ##
        ####################################
    
        SET @sql_sam_test := CONCAT( ' INSERT sam_test (test_id, import_xref_code, moniker, answer_source_code, generation_method_code, external_answer_source_id, mastery_level, threshold_level, external_grading_flag, client_id, owner_id, purge_flag, last_user_id, create_timestamp)'
                                    ,' SELECT DISTINCT pmi_f_get_next_sequence_app_db(\'sam_test\', 1), at.test_name, at.test_name, \'e\', \'e\', @external_answer_source_id, 70, 50, 1, ', @client_id, ', ', @client_id, ', 0, 1234, now()'
                                    ,' FROM   ', @db_name_ods, '.pmi_ods_scan_results_pivot_edmin at'
                                    ,' WHERE NOT EXISTS (SELECT * FROM sam_test t WHERE at.test_name = t.import_xref_code)');

        prepare sql_sam_test from @sql_sam_test;
        execute sql_sam_test;
        deallocate prepare sql_sam_test;

        SET @sql_sam_test_list := CONCAT( ' INSERT tmp_test_list (test_id)'
                                    ,' SELECT t.test_id'
                                    ,' FROM   ', @db_name_ods, '.pmi_ods_scan_results_pivot_edmin at'
                                    ,' JOIN   sam_test t ON at.test_name = t.import_xref_code'
                                    ,' GROUP BY t.test_id');

        prepare sql_sam_test_list from @sql_sam_test_list;
        execute sql_sam_test_list;
        deallocate prepare sql_sam_test_list;
                
#                SET @sql_sam_test_event := CONCAT(' INSERT sam_test_event (test_id, test_event_id, import_xref_code, online_scoring_flag, start_date, end_date, admin_type_code, last_user_id, admin_period_id, purge_flag, create_timestamp)'
#                                            ,' SELECT DISTINCT t.test_id, pmi_f_get_next_sequence_app_db(\'sam_test_event\', 1), at.test_name, 0, now(),null,\'s\',1234, 1000001, 0, now()'
#                                            ,' FROM   ', @db_name_ods, '.pmi_ods_scan_results_pivot_edmin at'
#                                            ,' JOIN   sam_test t   ON at.test_name = t.import_xref_code'
#                                            ,' WHERE NOT EXISTS (SELECT * FROM sam_test_event te WHERE te.test_id = t.test_id)');
#                prepare sql_sam_test_event from @sql_sam_test_event;
#                        
#                    execute sql_sam_test_event;
#                        
#                    deallocate prepare sql_sam_test_event;

        INSERT sam_test_event (test_id, test_event_id, online_scoring_flag, start_date, end_date, admin_period_id, purge_flag, client_id, last_user_id, create_timestamp)
        SELECT  tmp1.test_id, pmi_f_get_next_sequence_app_db('sam_test_event', 1), 0, now(), null, 1000001, 0, @client_id, 1234, now()
        FROM    tmp_test_list as tmp1
        LEFT JOIN   sam_test_event as te
                ON      te.test_id = tmp1.test_id
        WHERE   te.test_event_id is null
        ;
                
        SET @sql_edmin_question_list := CONCAT(' CREATE TABLE tmp_edmin_question_list '
                                    ,' SELECT t.test_id, pmi_f_get_next_sequence_app_db(\'sam_answer_key\', 1) as test_question_id, '
                                    ,'                  MIN(at.score_column_name_prefix) as import_xref_code, 1 as section_num, at.question_sequence as section_question_num, '
                                    ,'                  MIN(MID(score_column_name_prefix, 2, 5)) as flatfile_question_num, @normal_response_id as question_type_id, MIN(COALESCE(r.rubric_id, 1000002)) as rubric_id, '
                                    ,'                  MIN(at.benchmark_abbr) as question_label '
                                    ,' FROM   ', @db_name_ods, '.pmi_ods_scan_results_pivot_edmin at'
                                    ,' JOIN   sam_test t   ON at.test_name = t.import_xref_code'
                                    ,' LEFT JOIN sam_rubric r  ON  r.rubric_total = at.benchmark_score_range_high and r.moniker = CONCAT( ROUND(at.benchmark_score_range_high), \' POINT (\',ROUND(at.benchmark_score_range_high), \')\') '
                                    ,' LEFT JOIN sam_answer_key ak ON ak.test_id = t.test_id AND ak.section_question_num = at.question_sequence '
                                    ,' WHERE ak.test_question_id is null '
                                    ,' GROUP BY t.test_id, at.question_sequence ');

        prepare sql_edmin_question_list from @sql_edmin_question_list;
        execute sql_edmin_question_list;
        deallocate prepare sql_edmin_question_list;

#                SET @sql_sam_answer_key := CONCAT(' INSERT sam_answer_key (test_id, test_question_id, import_xref_code, section_num, section_question_num, flatfile_question_num, question_type_id, rubric_id, question_label, last_user_id, create_timestamp) '
#                                            ,' SELECT   test_id, test_question_id, import_xref_code, section_num, section_question_num, flatfile_question_num '
#                                            ,'          ,question_type_id, rubric_id, question_label '
#                                            ,' SELECT DISTINCT  t.test_id, pmi_f_get_next_sequence_app_db(\'sam_answer_key\', 1) as test_question_id, '
#                                            ,'                  at.score_column_name_prefix as import_xref_code, 1 as section_num, at.question_sequence as section_question_num, '
#                                            ,'                  MID(score_column_name_prefix, 2, 5) as flatfile_question_num, 1000001 as question_type_id, COALESCE(r.rubric_id, 1000002) as rubric_id, '
#                                            ,'                  at.benchmark_abbr as question_label '
#                                            ,' FROM   ', @db_name_ods, '.pmi_ods_scan_results_pivot_edmin at'
#                                            ,' JOIN   sam_test t   ON at.test_name = t.import_xref_code'
#                                            ,' LEFT JOIN sam_rubric r  ON  r.rubric_total = at.benchmark_score_range_high and r.moniker = CONCAT( ROUND(at.benchmark_score_range_high), \' POINT (\',ROUND(at.benchmark_score_range_high), \')\') '
#                                            ,' WHERE NOT EXISTS (SELECT * FROM sam_answer_key ak WHERE ak.test_id = t.test_id AND ak.section_question_num = at.question_sequence)');
#                prepare sql_sam_answer_key from @sql_sam_answer_key;
#                        
#                    execute sql_sam_answer_key;
#                        
#                    deallocate prepare sql_sam_answer_key;

        INSERT sam_answer_key (
            test_id
            , test_question_id
            , import_xref_code
            , section_num
            , section_question_num
            , flatfile_question_num
            , question_type_id
            , rubric_id
            , question_label
            , last_user_id
            , create_timestamp
            ) 
        SELECT  eql.test_id
            , eql.test_question_id
            , eql.import_xref_code
            , eql.section_num
            , eql.section_question_num
            , eql.flatfile_question_num
            , case when instr(eql.question_label, '*') > 0 then @challenge_response_id else eql.question_type_id end
            , case when instr(eql.question_label, '*') > 0 then chrr.rubric_id else eql.rubric_id end
            , eql.question_label
            , 1234
            , now()
        FROM    tmp_edmin_question_list as eql
        LEFT JOIN sam_rubric as chrr
                ON      chrr.moniker = '1 Point (.001)'
        ;
                
#                UPDATE sam_answer_key ak
#                    JOIN    sam_test t
#                        ON      t.test_id = ak.test_id
#                        AND     t.external_grading_flag = 1
#                JOIN    tmp_test_list as tmp1
#                        ON      t.test_id = tmp1.test_id
#                SET  ak.question_type_id = 1000008, ak.rubric_id = 1000169
#                WHERE ak.question_label LIKE '%*%';
        
        
        #############################################
        # sam_test_student
        #############################################
        
             
        SET @sql_text_sam_test_student := CONCAT('INSERT INTO sam_test_student (test_id, test_event_id, student_id, last_user_id, create_timestamp) '
                                    ,' SELECT  te.test_id, te.test_event_id, s.student_id, 1234, current_timestamp '
                                    ,' FROM   ', @db_name_ods, '.pmi_ods_scan_results_pivot_edmin AS at ' 
                                    ,' JOIN   sam_test t            ON at.test_name = t.import_xref_code'
                                    ,' JOIN   sam_test_event AS te  ON te.test_id = t.test_id '
                                    ,' JOIN   c_student AS s       ON s.student_code = at.person_code'
                                    ,' GROUP BY te.test_id, te.test_event_id, s.student_id '
                                    ,' ON DUPLICATE KEY UPDATE last_user_id = 1234 ');

        prepare sql_text_sam_test_student from @sql_text_sam_test_student;
        execute sql_text_sam_test_student;
        deallocate prepare sql_text_sam_test_student;

        #############################################
        # sam_student_response
        #############################################
                
        
        SET @sql_text_sam_student_response := CONCAT('INSERT INTO sam_student_response (test_id, test_event_id, student_id, test_question_id, rubric_score, last_user_id, create_timestamp) ' 
                                    ,' SELECT  t.test_id, te.test_event_id, s.student_id, ak.test_question_id, ROUND(at.student_score, 0), 1234, current_timestamp '
                                    ,' FROM   ', @db_name_ods, '.pmi_ods_scan_results_pivot_edmin AS at '
                                    ,' JOIN   sam_test AS t        ON at.test_name = t.import_xref_code'
                                    ,' JOIN   c_student AS s      ON s.student_code = at.person_code'
                                    ,' JOIN   sam_test_event AS te ON te.test_id = t.test_id '
                                    ,' JOIN   sam_answer_key AS ak ON ak.test_id = t.test_id AND ak.section_question_num = at.question_sequence '
                                    ,' LEFT JOIN    sam_student_response AS er     ON er.test_id = t.test_id    AND  er.test_event_id = te.test_event_id    AND  er.student_id = s.student_id    AND  er.test_question_id = ak.test_question_id '
                                    ,' WHERE   er.student_id is null or  er.gui_edit_flag <> 1 '
                                    ,' ON DUPLICATE KEY UPDATE last_user_id = 1234, rubric_score = values(rubric_score) ');

        prepare sql_text_sam_student_response from @sql_text_sam_student_response;
        execute sql_text_sam_student_response;
        deallocate prepare sql_text_sam_student_response;
        
        
        #############################################
        # sam_student_response-scoring
        #############################################
    
        UPDATE sam_student_response er
        JOIN    tmp_test_list as tmp1
            ON      er.test_id = tmp1.test_id
        JOIN    sam_answer_key ak
            ON      er.test_id = ak.test_id
            AND     er.test_question_id = ak.test_question_id
        JOIN    sam_rubric_list rl
            ON      rl.rubric_id = ak.rubric_id
            AND     er.rubric_score = rl.rubric_score    
        SET er.rubric_value = rl.rubric_value
        ;

        update  sam_test as t
        join    tmp_test_list as tl
                on      t.test_id = tl.test_id
        set     t.force_rescore_flag = 0
        ;
        
        #################
        ## Cleanup
        #################
        
        SET @sql_drop_pivot := CONCAT('TRUNCATE TABLE ', @db_name_ods, '.pmi_ods_scan_results_pivot_edmin');
       
        prepare sql_drop_pivot from @sql_drop_pivot;
        execute sql_drop_pivot;
        deallocate prepare sql_drop_pivot;
        
                 
        #################
        ## Update Log
        #################
    
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_scan_results_pivot_edmin', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
            
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;   

        drop table if exists tmp_ak_insert;
        drop table if exists tmp_test_list;
        drop table if exists tmp_edmin_question_list;

    end if;

END;
//
