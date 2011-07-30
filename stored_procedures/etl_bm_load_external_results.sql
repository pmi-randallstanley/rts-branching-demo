/*
$Rev: 8535 $ 
$Author: randall.stanley $ 
$Date: 2010-05-07 10:11:52 -0400 (Fri, 07 May 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_external_results.sql $
$Id: etl_bm_load_external_results.sql 8535 2010-05-07 14:11:52Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_bm_load_external_results//

create definer=`dbadmin`@`localhost` procedure etl_bm_load_external_results()
CONTAINS SQL
COMMENT '$Rev: 8535 $ $Date: 2010-05-07 10:11:52 -0400 (Fri, 07 May 2010) $'
SQL SECURITY INVOKER

BEGIN 

    DECLARE v_useMonikerForImportXrefCode char(1) default 'y';    
    DECLARE v_use_stu_state_code char(1) default 'n';    
    DECLARE v_add_sam_test char(1) default 'n';    
    
    
    SET @useMonikerForImportXrefCode := pmi_f_get_etl_setting('useMonikerForImportXrefCode');
    SET @bmDoesNotSendCRScanResults := pmi_f_get_etl_setting('bmDoesNotSendCRScanResults');
    SET @bmUseStuStateCodeScanResults := pmi_f_get_etl_setting('bmUseStuStateCodeScanResults');
    SET @allowAddSAMTest := pmi_f_get_etl_setting('allowAddSAMTest');
    
    if @allowaddsamtest is not null then
        set v_add_sam_test = @allowaddsamtest;
    end if;

    IF @useMonikerForImportXrefCode = 'y' THEN 
        SET v_useMonikerForImportXrefCode = 'n';
    END IF;
    
    if v_add_sam_test = 'y' then 
        IF v_useMonikerForImportXrefCode = 'n' 
            THEN 
                UPDATE sam_test 
                SET import_xref_code = moniker
                where import_xref_code IS NULL
                AND     purge_flag = 0;
            ELSE
                UPDATE sam_test 
                SET import_xref_code = test_id
                WHERE import_xref_code IS NULL
                AND     purge_flag = 0;
        END IF;
    end if;
    
    if @bmUseStuStateCodeScanResults is not null then
        set v_use_stu_state_code = @bmUseStuStateCodeScanResults;
    end if;


    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_ods
    and     t.table_name = 'pmi_ods_scan_results_pivot_external';

    if @table_exists > 0 then

        drop table if exists tmp_test_list;
        drop table if exists tmp_test_student;
        drop table if exists tmp_ak_insert;
        drop table if exists tmp_test_event;
        
        create table tmp_test_list (test_id int(11) not null);

        CREATE TABLE `tmp_test_student` (
          `test_id` int(10) NOT NULL,
          `test_event_id` int(10) NOT NULL,
          `student_id` int(10) NOT NULL,
          PRIMARY KEY  (`test_id`,`test_event_id`,`student_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;


        CREATE TABLE `tmp_test_event` (
          `test_id` int(10) NOT NULL,
          `test_event_id` int(10)  NULL,
          PRIMARY KEY  (`test_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        ####################################
        ## Load sam_test and sam_test_event ##
        ####################################

        if v_add_sam_test = 'y' then

            select  external_answer_source_id
            into    @external_answer_source_id
            from    sam_external_answer_source
            where   external_answer_source_code = 'other'
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

            SET @sql_sam_test := '';
            SET @sql_sam_test := CONCAT(@sql_sam_test, ' INSERT sam_test (test_id, import_xref_code, moniker, answer_source_code, generation_method_code, external_answer_source_id ');
            SET @sql_sam_test := CONCAT(@sql_sam_test, '       , mastery_level, threshold_level, external_grading_flag, client_id, owner_id, last_user_id, answer_set_id, purge_flag)');
            SET @sql_sam_test := CONCAT(@sql_sam_test, ' SELECT DISTINCT pmi_f_get_next_sequence_app_db(\'sam_test\', 1), at.import_xref_code ,at.import_xref_code, \'e\', \'e\', @external_answer_source_id ');
            SET @sql_sam_test := CONCAT(@sql_sam_test, '       ,70, 50, 0, @client_id, @client_id, 1234, @answer_set_id, 0');
            SET @sql_sam_test := CONCAT(@sql_sam_test, ' FROM ', @db_name_ods, '.pmi_ods_scan_results_pivot_external at ');
            SET @sql_sam_test := CONCAT(@sql_sam_test, ' WHERE NOT EXISTS (SELECT * FROM sam_test t WHERE at.import_xref_code = t.import_xref_code) AND at.import_xref_code IS NOT NULL');
            
            prepare sql_sam_test from @sql_sam_test;
            execute sql_sam_test;
            deallocate prepare sql_sam_test;

            #########################
            ## sam_answer_key
            ##########################
        
            SET @sql_sam_answer_key := '';
            SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' CREATE TABLE tmp_ak_insert ');
            SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' SELECT DISTINCT t.test_id, pmi_f_get_next_sequence_app_db(\'sam_answer_key\', 1) as test_question_id, at.flatfile_question_num as import_xref_code,  ');
            SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, '   1 as section_num,  at.flatfile_question_num as section_question_num, 1000001 as question_type_id, 1000002 as rubric_id,  ');
            SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, '   CONCAT(\'1-\',at.flatfile_question_num) as question_label, at.flatfile_question_num ');
            SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' FROM (SELECT distinct import_xref_code, flatfile_question_num FROM ', @db_name_ods, '.pmi_ods_scan_results_pivot_external WHERE student_answer IS NOT NULL) at   JOIN    sam_test t   ON at.import_xref_code = t.import_xref_code');
            SET @sql_sam_answer_key := CONCAT(@sql_sam_answer_key, ' WHERE NOT EXISTS (SELECT * FROM sam_answer_key ak WHERE ak.test_id = t.test_id)');
        
            prepare sql_sam_answer_key from @sql_sam_answer_key;
            execute sql_sam_answer_key;
            deallocate prepare sql_sam_answer_key;
    
            INSERT  sam_answer_key (test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, rubric_id, answer_set_id, answer_subset_id, question_label, flatfile_question_num, last_user_id)
            SELECT  test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, rubric_id, @answer_set_id, @answer_subset_id, question_label, flatfile_question_num, 1234
            FROM    tmp_ak_insert;
            
            call sam_set_answer_key_flatfile_num();

        end if;

        SET @sql_sam_test_list := CONCAT( ' INSERT tmp_test_list (test_id)'
                                    ,' SELECT t.test_id'
                                    ,' FROM   ', @db_name_ods, '.pmi_ods_scan_results_pivot_external at'
                                    ,' JOIN   sam_test t ON at.import_xref_code = t.import_xref_code'
                                    ,' GROUP BY t.test_id');

        prepare sql_sam_test_list from @sql_sam_test_list;
        execute sql_sam_test_list;
        deallocate prepare sql_sam_test_list;
        
#        SET @sql_sam_test_event := '';
#        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' INSERT sam_test_event (test_id, test_event_id, import_xref_code, online_scoring_flag, admin_type_code, last_user_id, admin_period_id, purge_flag)');
#        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' SELECT DISTINCT t.test_id, pmi_f_get_next_sequence_app_db(\'sam_test_event\', 1),at.import_xref_code,0,\'s\',1234, 1000001, 0');
#        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' FROM ', @db_name_ods, '.pmi_ods_scan_results_pivot_external at');
#        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' JOIN sam_test t   ON at.import_xref_code = t.import_xref_code');
#        SET @sql_sam_test_event := CONCAT(@sql_sam_test_event, ' WHERE NOT EXISTS (SELECT * FROM sam_test_event te WHERE te.test_id = t.test_id)');
#        
#        prepare sql_sam_test_event from @sql_sam_test_event;
#        execute sql_sam_test_event;
#        deallocate prepare sql_sam_test_event;

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
    
        #############################################
        # sam_test_student 
        #############################################
    
        SET @sql_text_sam_test_student := '';
        SET @sql_text_sam_test_student := CONCAT('INSERT INTO sam_test_student (test_id, test_event_id, student_id, last_user_id, create_timestamp)');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'SELECT  te.test_id, te.test_event_id, s.student_id, 1234, current_timestamp ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'FROM ', @db_name_ods, '.pmi_ods_scan_results_pivot_external AS sr ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN sam_test as t ON t.import_xref_code = sr.import_xref_code ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN tmp_test_event AS te ON te.test_id = t.Test_id ');

        if v_use_stu_state_code = 'y' then
            SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN c_student AS s ON s.student_state_code = sr.student_code ');
        else
            SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'JOIN c_student AS s ON s.student_code = sr.student_code ');        
        end if;

        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'GROUP BY te.test_id, te.test_event_id, s.student_id ');
        SET @sql_text_sam_test_student := CONCAT(@sql_text_sam_test_student, 'ON DUPLICATE KEY UPDATE last_user_id = 1234 ');
        
        prepare sql_text_sam_test_student from @sql_text_sam_test_student;
        execute sql_text_sam_test_student;
        deallocate prepare sql_text_sam_test_student;
            
        #############################################
        # sam_student_response 
        #############################################
    
        SET @sql_text_sam_student_response := '';
        SET @sql_text_sam_student_response := CONCAT('INSERT INTO sam_student_response (test_id, test_event_id, student_id, test_question_id, student_answer, rubric_score, last_user_id, create_timestamp) ' );
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' SELECT  t.test_id, te.test_event_id, s.student_id, ak.test_question_id' );
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ', case when sm.scoring_method_code = \'c\' then null else srp.student_answer end, ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' case when t.external_grading_flag = 1 and srp.student_answer REGEXP \'^[0-9]\' != 0 then cast(srp.student_answer as signed)  ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' when t.external_grading_flag = 1 and srp.student_answer REGEXP \'^[0-9]\' = 0 then 0  ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' when sm.scoring_method_code in (\'s\',\'g\') and srp.student_answer = ak.answer then 1 ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' when sm.scoring_method_code in (\'s\',\'g\') then 0 ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' when sm.scoring_method_code = \'c\' and srp.student_answer REGEXP \'^[0-9]\' = 0 then 0 ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' when sm.scoring_method_code = \'c\' and srp.student_answer REGEXP \'^[0-9]\' != 0 then cast(srp.student_answer as signed) ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' else null end,  ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' 1234, current_timestamp ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'FROM    ', @db_name_ods, '.pmi_ods_scan_results_pivot_external AS srp ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN        sam_test as t ON t.import_xref_code = srp.import_xref_code ');

        if v_use_stu_state_code = 'y' then
            SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    c_student AS s ON s.student_state_code = srp.student_code ');
        else
            SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    c_student AS s ON s.student_code = srp.student_code ');
        end if;

        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    tmp_test_event AS te ON te.test_id = t.test_id   ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    sam_answer_key AS ak ON ak.test_id = t.test_id AND ak.flatfile_question_num = srp.flatfile_question_num ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    sam_question_type AS qt ON ak.question_type_id = qt.question_type_id  '); 
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'JOIN    sam_scoring_method AS sm ON qt.scoring_method_id = sm.scoring_method_id '); 

        IF @bmDoesNotSendCRScanResults = 'y' THEN
            SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, ' AND sm.scoring_method_code != \'c\''); 
        END IF;
    
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'LEFT JOIN    sam_student_response AS er     ON er.test_id = t.test_id    AND  er.test_event_id = te.test_event_id    AND  er.student_id = s.student_id    AND  er.test_question_id = ak.test_question_id ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'WHERE   er.student_id is null or  er.gui_edit_flag <> 1 ');
        SET @sql_text_sam_student_response := CONCAT(@sql_text_sam_student_response, 'ON DUPLICATE KEY UPDATE last_user_id = 1234, student_answer = values(student_answer), rubric_score = values(rubric_score)  ');
    
        prepare sql_text_sam_student_response from @sql_text_sam_student_response;
        execute sql_text_sam_student_response;
        deallocate prepare sql_text_sam_student_response;

        #############################################
        # insert no-responses
        #############################################

        insert  tmp_test_student (test_id, test_event_id, student_id)
        select  ts.test_id, ts.test_event_id, ts.student_id
        from    sam_test_student as ts
        join    tmp_test_event as tl
                on      ts.test_id = tl.test_id
                and     ts.test_event_id = tl.test_event_id
        where exists (  select  *
                        from    sam_student_response as er
                        where   ts.test_id = er.test_id
                        and     ts.test_event_id = er.test_event_id
                        and     ts.student_id = er.student_id
                     )
        ;

        SET @sql_text_missing := 'insert sam_student_response (test_id, test_event_id, student_id, test_question_id, rubric_score, rubric_value, last_user_id, create_timestamp) ';
        SET @sql_text_missing := concat(@sql_text_missing, ' select  ak.test_id, ts.test_event_id, ts.student_id, ak.test_question_id, 0, 0, 1234, now() ');
        SET @sql_text_missing := concat(@sql_text_missing, ' from    sam_answer_key as ak ');
        SET @sql_text_missing := concat(@sql_text_missing, ' join    tmp_test_list as tmp1 ');
        SET @sql_text_missing := concat(@sql_text_missing, '         on      tmp1.test_id = ak.test_id ');

        IF @bmDoesNotSendCRScanResults = 'y' THEN
            SET @sql_text_missing := concat(@sql_text_missing, ' join    sam_question_type as qt ');
            SET @sql_text_missing := concat(@sql_text_missing, '         on      ak.question_type_id = qt.question_type_id ');
            SET @sql_text_missing := concat(@sql_text_missing, ' join    sam_scoring_method as sm ');
            SET @sql_text_missing := concat(@sql_text_missing, '         on      qt.scoring_method_id = sm.scoring_method_id ');
            SET @sql_text_missing := concat(@sql_text_missing, '         and     sm.scoring_method_code != \'c\' ');
        END IF;

        SET @sql_text_missing := concat(@sql_text_missing, ' join    tmp_test_student as ts ');
        SET @sql_text_missing := concat(@sql_text_missing, '         on      ts.test_id = ak.test_id ');
        SET @sql_text_missing := concat(@sql_text_missing, ' left join    sam_student_response as er ');
        SET @sql_text_missing := concat(@sql_text_missing, '         on      er.test_id = ak.test_id ');
        SET @sql_text_missing := concat(@sql_text_missing, '         and     er.test_event_id = ts.test_event_id ');
        SET @sql_text_missing := concat(@sql_text_missing, '         and     er.student_id = ts.student_id ');
        SET @sql_text_missing := concat(@sql_text_missing, '         and     er.test_question_id = ak.test_question_id ');
        SET @sql_text_missing := concat(@sql_text_missing, ' where   er.test_id is null ');

        prepare sql_text_missing from @sql_text_missing;
        execute sql_text_missing;
        deallocate prepare sql_text_missing;

        #############################################
        # sam_student_response-scoring
        #############################################

        update  sam_student_response as er
        join    tmp_test_list as tl
                on      er.test_id = tl.test_id
        join    sam_answer_key as ak
                on      er.test_id = ak.test_id
                and     er.test_question_id = ak.test_question_id
        left join    sam_rubric_list as rl
                on      ak.rubric_id = rl.rubric_id
                and     er.rubric_score = rl.rubric_score
        set     er.rubric_value = case when rl.rubric_item_id is null then 0 else rl.rubric_value end
        ;
        
        # reset force_rescore_flag for set just processed
        update  sam_test as t
        join    tmp_test_list as tl
                on      t.test_id = tl.test_id
        set     t.force_rescore_flag = 0
        ;
        #################
        ## Cleanup
        #################
        
        SET @sql_drop_pivot := CONCAT('TRUNCATE TABLE ', @db_name_ods, '.pmi_ods_scan_results_pivot_external');
        prepare sql_drop_pivot from @sql_drop_pivot;
            execute sql_drop_pivot;
            deallocate prepare sql_drop_pivot;


        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_scan_results_pivot_external', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;   

        drop table if exists tmp_test_list;
        drop table if exists tmp_test_student;
        drop table if exists tmp_ak_insert;
        drop table if exists tmp_test_event;

    end if;
    
END;
//
