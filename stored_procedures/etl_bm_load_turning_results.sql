/*
$Rev: 9335 $ 
$Author: randall.stanley $ 
$Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_turning_results.sql $
$Id: etl_bm_load_turning_results.sql 9335 2010-10-03 18:10:23Z randall.stanley $ 
*/

DROP PROCEDURE IF EXISTS etl_bm_load_turning_results //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_turning_results()
contains sql
sql security invoker
comment '$Rev: 9335 $ $Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $'

BEGIN 


    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_ods
    and     t.table_name = 'pmi_ods_scan_results_turning';

    if @table_exists > 0 then
    
        drop table if exists `tmp_test_list`;
        create table `tmp_test_list` (
          `test_id` int(11) not null,
          `client_id` int(11) not null,
          primary key  (`test_id`)
        ) engine=innodb default charset=latin1
        ;

        INSERT tmp_test_list (test_id, client_id)
        SELECT DISTINCT t.test_id, t.client_id
        FROM   v_pmi_ods_scan_results_turning at
        JOIN   sam_test t 
               ON at.pm_test_id = t.test_id
        UNION
        SELECT DISTINCT t2.test_id, t2.client_id
        FROM   v_pmi_ods_scan_results_turning at
        JOIN   sam_test as t2 
               ON at.test_name = t2.import_xref_code
               AND at.user_id = t2.owner_id;

        INSERT sam_test_event (test_id, test_event_id, online_scoring_flag, start_date, end_date, admin_period_id, purge_flag, client_id, last_user_id, create_timestamp)
        SELECT  tmp1.test_id, pmi_f_get_next_sequence_app_db('sam_test_event', 1), 0, now(), null, 1000001, 0, tmp1.client_id, 1234, now()
        FROM    tmp_test_list as tmp1
        LEFT JOIN   sam_test_event as te
                ON      te.test_id = tmp1.test_id
                AND     te.purge_flag = 0
        WHERE   te.test_event_id is null
        ;

        
        
        #############################################
        # sam_test_student - student_code
        #############################################
    
        INSERT INTO sam_test_student (test_id, test_event_id, student_id, last_user_id, create_timestamp)
        SELECT  distinct te.test_id, te.test_event_id, s.student_id, 1234, current_timestamp
        FROM   v_pmi_ods_scan_results_turning AS sr
        JOIN   sam_test as t 
               ON t.test_id =  sr.pm_test_id
        JOIN   sam_test_event AS te 
               ON te.test_id = t.test_id 
               AND te.purge_flag = 0
        JOIN   c_student AS s 
               ON s.student_code = sr.student_code
        UNION ALL
        SELECT  distinct te.test_id, te.test_event_id, s.student_id, 1234, current_timestamp
        FROM   v_pmi_ods_scan_results_turning AS sr
        JOIN   sam_test as t 
               ON t.import_xref_code =  sr.test_name 
               and t.owner_id = sr.user_id
               AND sr.pm_test_id is null
        JOIN   sam_test_event AS te 
               ON te.test_id = t.test_id 
               AND te.purge_flag = 0
        JOIN   c_student AS s 
               ON s.student_code = sr.student_code
        ON DUPLICATE KEY UPDATE last_user_id = 1234;
        
        #############################################
        # sam_student_response - district test
        #############################################
    
        INSERT INTO sam_student_response (test_id, test_event_id, student_id, test_question_id, student_answer, rubric_score, last_user_id, create_timestamp)
        SELECT  t.test_id
                ,te.test_event_id
                ,s.student_id
                ,ak.test_question_id
                ,case 
                  when sm.scoring_method_code = 'c' then null 
                  else sr.response_value end
                ,case 
                  when sm.scoring_method_code in ('s','g') and sr.response_value = ak.answer then 1 
                  when sm.scoring_method_code in ('s','g') then 0 
                  else sr.points_earned end
                 ,1234
                 ,current_timestamp
        FROM    v_pmi_ods_scan_results_turning AS sr
        JOIN    sam_test as t 
                ON t.test_id = sr.pm_test_id
        JOIN    c_student AS s 
                ON s.student_code = sr.student_code
        JOIN    sam_test_event AS te  
                ON te.test_id = t.test_id  and te.purge_flag = 0
        JOIN    sam_answer_key AS ak 
                ON ak.test_id = t.test_id 
                AND ak.flatfile_question_num = sr.question_number
        JOIN    sam_question_type AS qt 
                ON ak.question_type_id = qt.question_type_id
        JOIN    sam_scoring_method AS sm 
                ON qt.scoring_method_id = sm.scoring_method_id
        LEFT JOIN  sam_student_response AS er     
                ON er.test_id = t.test_id    
                AND  er.test_event_id = te.test_event_id    
                AND  er.student_id = s.student_id    
                AND  er.test_question_id = ak.test_question_id
        WHERE   er.student_id is null or  er.gui_edit_flag <> 1
        ON DUPLICATE KEY UPDATE last_user_id = 1234, student_answer = values(student_answer), rubric_score = values(rubric_score);

        #############################################
        # sam_student_response - User test
        #############################################
    
        INSERT INTO sam_student_response (test_id, test_event_id, student_id, test_question_id, student_answer, rubric_score, last_user_id, create_timestamp)
        SELECT  t.test_id
                ,te.test_event_id
                ,s.student_id
                ,ak.test_question_id
                ,case 
                  when sm.scoring_method_code = 'c' then null 
                  else sr.response_value end as student_answer
                ,case 
                  when sm.scoring_method_code in ('s','g') and sr.response_value = ak.answer then 1 
                  when sm.scoring_method_code in ('s','g') then 0 
                  else sr.points_earned end as rubric_score
                 ,1234
                 ,current_timestamp
        FROM    v_pmi_ods_scan_results_turning AS sr
        JOIN    sam_test as t 
                ON t.import_xref_code = sr.test_name
                and t.owner_id = sr.user_id
        JOIN    c_student AS s 
                ON s.student_code = sr.student_code
        JOIN    sam_test_event AS te  
                ON te.test_id = t.test_id  and te.purge_flag = 0
        JOIN    sam_answer_key AS ak 
                ON ak.test_id = t.test_id 
                AND ak.flatfile_question_num = sr.question_number
        JOIN    sam_question_type AS qt 
                ON ak.question_type_id = qt.question_type_id
        JOIN    sam_scoring_method AS sm 
                ON qt.scoring_method_id = sm.scoring_method_id
        LEFT JOIN  sam_student_response AS er     
                ON er.test_id = t.test_id    
                AND  er.test_event_id = te.test_event_id    
                AND  er.student_id = s.student_id    
                AND  er.test_question_id = ak.test_question_id
        WHERE   er.student_id is null or  er.gui_edit_flag <> 1
        ON DUPLICATE KEY UPDATE last_user_id = 1234, student_answer = values(student_answer), rubric_score = values(rubric_score);
        
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
        
        SET @sql_truncate_pivot := CONCAT('TRUNCATE TABLE ', @db_name_ods, '.pmi_ods_scan_results_turning');
        prepare sql_truncate_pivot from @sql_truncate_pivot;
        execute sql_truncate_pivot;
        deallocate prepare sql_truncate_pivot;
   
        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_scan_results_turning', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;

    end if;
    
END;
//
