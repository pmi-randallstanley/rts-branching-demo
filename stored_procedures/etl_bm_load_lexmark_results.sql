/*
$Rev: 9904 $ 
$Author: randall.stanley $ 
$Date: 2011-01-19 13:12:43 -0500 (Wed, 19 Jan 2011) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_lexmark_results.sql $
$Id: etl_bm_load_lexmark_results.sql 9904 2011-01-19 18:12:43Z randall.stanley $ 
*/

DROP PROCEDURE IF EXISTS etl_bm_load_lexmark_results //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_lexmark_results()
contains sql
sql security invoker
comment '$Rev: 9904 $ $Date: 2011-01-19 13:12:43 -0500 (Wed, 19 Jan 2011) $'

BEGIN 

   DECLARE v_test_id int;
   DECLARE v_grid_resp_type_exists int default '0';

   DECLARE v_no_more_rows BOOLEAN;
      
   DECLARE cur_flatfile_question_number CURSOR FOR
    select  test_id
    from    tmp_test_list;
   DECLARE CONTINUE HANDLER FOR NOT FOUND 
      SET v_no_more_rows = TRUE;

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_ods
    and     t.table_name = 'pmi_ods_scan_results_lexmark';

    if @table_exists > 0 then

        drop table if exists `tmp_test_list`;
        drop table if exists `tmp_stu_grid_response_calc`;
        drop table if exists `tmp_ak_grid`;
        drop table if exists `tmp_test_event`;

        create table `tmp_test_list` (
          `test_id` int(11) not null,
          `client_id` int(11) not null,
          primary key  (`test_id`)
        ) engine=innodb default charset=latin1
        ;

        CREATE TABLE `tmp_test_event` (
          `test_id` int NOT NULL,
          `test_event_id` int NULL,
          PRIMARY KEY  (`test_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_stu_grid_response_calc` (
          `test_id` int(11) NOT NULL,
          `test_event_id` int(11) NOT NULL,
          `student_id` int(11) NOT NULL,
          `test_question_id` int(11) NOT NULL,
          `student_answer_raw` varchar(255) default NULL,
          `student_answer_numeric_char` varchar(25) default NULL,
          `numerator_value` smallint(6) default NULL,
          `denominator_value` smallint(6) default NULL,
          `gui_edit_flag` tinyint(1) NOT NULL default '0',
          `last_user_id` int(11) NOT NULL,
          `create_timestamp` datetime NOT NULL default '1980-12-31 00:00:00',
          `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
          PRIMARY KEY  (`test_id`,`test_event_id`,`student_id`,`test_question_id`),
          KEY `ind_tmp_stu_grid_response_calc_stu` (`student_id`),
          KEY `ind_tmp_stu_grid_response_calc_ak` (`test_id`,`test_question_id`),
          KEY `ind_tmp_stu_grid_response_calc_stu_question` (`test_id`,`test_question_id`,`student_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_ak_grid` (
          `test_id` int(11) NOT NULL,
          `test_question_id` int(11) NOT NULL,
          `correct_answer` varchar(255) NOT NULL,
          `round_digits` tinyint(4) NOT NULL,
          `last_user_id` int(11) NOT NULL,
          `create_timestamp` datetime NOT NULL default '1980-12-31 00:00:00',
          `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
          PRIMARY KEY  (`test_id`,`test_question_id`),
          KEY `ind_tmp_ak_grid_round_digits` (`round_digits`,`test_id`,`test_question_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        INSERT tmp_test_list (test_id, client_id)
        SELECT DISTINCT t.test_id, t.client_id
        FROM   v_pmi_ods_scan_results_lexmark at
        JOIN   sam_test t 
               ON at.pm_test_id = t.test_id
        UNION ALL
        SELECT DISTINCT t.test_id, t.client_id
        FROM   v_pmi_ods_scan_results_lexmark at
        JOIN   sam_test t 
               ON at.test_name = t.import_xref_code
               AND at.user_id = t.owner_id;

        SELECT  count(*)
        INTO    v_grid_resp_type_exists
        FROM    tmp_test_list AS src
        JOIN    sam_test as t 
                ON t.test_id = src.test_id
        JOIN    sam_answer_key AS ak 
                ON ak.test_id = t.test_id 
        JOIN    sam_question_type AS qt 
                ON ak.question_type_id = qt.question_type_id
        JOIN    sam_scoring_method AS sm 
                ON qt.scoring_method_id = sm.scoring_method_id
                AND sm.scoring_method_code = 'g'
        ;


        OPEN cur_flatfile_question_number;
        loop_cur_flatfile_question_number: LOOP
            FETCH   cur_flatfile_question_number 
            INTO     v_test_id;
        
            IF v_no_more_rows THEN
                  CLOSE cur_flatfile_question_number;
                  LEAVE loop_cur_flatfile_question_number;
            END IF;

              call sam_set_answer_key_flatfile_num_lexmark(v_test_id);
              
        END LOOP loop_cur_flatfile_question_number;

        insert tmp_test_event (
               test_id
              ,test_event_id
        )
        select tl.test_id
              ,min(test_event_id)
        from    tmp_test_list as tl
        left join   sam_test_event as te
                on      tl.test_id = te.test_id
                and     te.purge_flag = 0
                and     te.ola_flag   = 0
        group by tl.test_id
        ;
        
        insert tmp_test_event (
               test_id
              ,test_event_id
        )
        select test_id
              ,pmi_f_get_next_sequence_app_db('sam_test_event', 1)
        from    tmp_test_event as t
        where   test_event_id is null
        on duplicate key update test_event_id = values(test_event_id)
        ;

        #############################################
        # sam_test_event
        #############################################

        INSERT sam_test_event (test_id, test_event_id, online_scoring_flag, start_date, end_date, admin_period_id, purge_flag, client_id, last_user_id, create_timestamp)
        SELECT  tmp1.test_id, tmp1.test_event_id, 0, now(), null, 1000001, 0, @client_id, 1234, now()
        FROM    tmp_test_event as tmp1
        LEFT JOIN   sam_test_event as te
                ON      te.test_id = tmp1.test_id
                AND     te.test_event_id = tmp1.test_event_id
        WHERE   te.test_event_id is null
        ;

        #############################################
        # sam_test_student - student_code
        #############################################
    
        INSERT INTO sam_test_student (test_id, test_event_id, student_id, last_user_id, create_timestamp)
        SELECT  distinct te.test_id, te.test_event_id, s.student_id, 1234, current_timestamp
        FROM   v_pmi_ods_scan_results_lexmark AS sr
        JOIN   sam_test as t 
               ON t.test_id =  sr.pm_test_id
        JOIN   tmp_test_event AS te
               ON te.test_id = t.test_id 
        JOIN   c_student AS s 
               ON s.student_code = sr.student_code
        UNION ALL
        SELECT  distinct te.test_id, te.test_event_id, s.student_id, 1234, current_timestamp
        FROM   v_pmi_ods_scan_results_lexmark AS sr
        JOIN   sam_test as t 
               ON t.import_xref_code =  sr.test_name 
               AND t.owner_id = sr.user_id
               AND sr.pm_test_id is null
        JOIN   tmp_test_event AS te 
               ON te.test_id = t.test_id 
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
                  when sm.scoring_method_code = 's' and sr.response_value = ak.answer then 1 
                  when sm.scoring_method_code = 's' then 0 
                  when sm.scoring_method_code = 'c' and sr.response_value REGEXP '[^0-9]+' = 1 then 0
                  when sm.scoring_method_code = 'c' then sr.response_value
                  else 0 end
                 ,1234
                 ,current_timestamp
        FROM    v_pmi_ods_scan_results_lexmark AS sr
        JOIN    sam_test as t 
                ON t.test_id = sr.pm_test_id
        JOIN    c_student AS s 
                ON s.student_code = sr.student_code
        JOIN    tmp_test_event AS te  
                ON te.test_id = t.test_id
        JOIN    sam_answer_key AS ak 
                ON ak.test_id = t.test_id 
                AND ak.flatfile_question_num = sr.question_number
        JOIN    sam_question_type AS qt 
                ON ak.question_type_id = qt.question_type_id
        JOIN    sam_scoring_method AS sm 
                ON qt.scoring_method_id = sm.scoring_method_id
                AND sm.scoring_method_code in ('s','c')
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
                  else sr.response_value end
                ,case 
                  when sm.scoring_method_code = 's' and sr.response_value = ak.answer then 1 
                  when sm.scoring_method_code = 's' then 0 
                  when sm.scoring_method_code = 'c' and sr.response_value REGEXP '[^0-9]+' = 1 then 0
                  when sm.scoring_method_code = 'c' then sr.response_value
                  else 0 end
                 ,1234
                 ,current_timestamp
        FROM    v_pmi_ods_scan_results_lexmark AS sr
        JOIN    sam_test as t 
                ON t.import_xref_code = sr.test_name
                AND t.owner_id = sr.user_id
        JOIN    c_student AS s 
                ON s.student_code = sr.student_code
        JOIN    tmp_test_event AS te  
                ON te.test_id = t.test_id
        JOIN    sam_answer_key AS ak 
                ON ak.test_id = t.test_id 
                AND ak.flatfile_question_num = sr.question_number
        JOIN    sam_question_type AS qt 
                ON ak.question_type_id = qt.question_type_id
        JOIN    sam_scoring_method AS sm 
                ON qt.scoring_method_id = sm.scoring_method_id
                AND sm.scoring_method_code in ('s','c')
        LEFT JOIN  sam_student_response AS er     
                ON er.test_id = t.test_id    
                AND  er.test_event_id = te.test_event_id    
                AND  er.student_id = s.student_id    
                AND  er.test_question_id = ak.test_question_id
        WHERE   er.student_id is null or  er.gui_edit_flag <> 1
        ON DUPLICATE KEY UPDATE last_user_id = 1234, student_answer = values(student_answer), rubric_score = values(rubric_score);

        if v_grid_resp_type_exists > 0 then

            insert tmp_ak_grid (
                test_id
                ,test_question_id
                ,correct_answer
                ,round_digits
                ,last_user_id
                ,create_timestamp
            )
            
            select  ak.test_id
                ,ak.test_question_id
                ,ak.answer
                ,case when instr(ak.answer, '.') != 0 then char_length(substring_index(ak.answer, '.', -1)) else 0 end
                ,1234
                ,now()
            from    tmp_test_list as src
            join    sam_test as t 
                    on t.test_id = src.test_id
            join    sam_answer_key as ak 
                    on ak.test_id = t.test_id 
            join    sam_question_type as qt 
                    on ak.question_type_id = qt.question_type_id
            join    sam_scoring_method as sm 
                    on qt.scoring_method_id = sm.scoring_method_id
                    and sm.scoring_method_code = 'g'
            ;

            # district test
            insert tmp_stu_grid_response_calc (
                test_id
                ,test_event_id
                ,student_id
                ,test_question_id
                ,student_answer_raw
                ,numerator_value
                ,denominator_value
                ,last_user_id
                ,create_timestamp
            )
            
            SELECT  t.test_id
                    ,te.test_event_id
                    ,s.student_id
                    ,ak.test_question_id
                    ,sr.response_value
                    ,case   # ignore responses with multiple division operators or decimal points
                            when substring_index(sr.response_value, '/', 2) != sr.response_value then null
                            when substring_index(sr.response_value, '.', 2) != sr.response_value then null
                            # ignore responses with leading division operator
                            when sr.response_value regexp '^/' then null
                            # ignore responses ending with division operator
                            when sr.response_value regexp '/$' then null
                            # ignore responses beginning with decimal followed by division operator
                            when sr.response_value regexp '^\\./' then null
                            # ignore responses containing both decimal point and division operator
                            when sr.response_value regexp '\\.+.*/+' then null
                            when sr.response_value regexp '/+.*\\.+' then null
                            # extract and split responses requiring calculation by division
                            when instr(sr.response_value, '/') != 0 and sr.response_value regexp '[^0-9./]+' = 0 then substring_index(sr.response_value, '/', 1) 
                            else null 
                    end as numerator_value
                    ,case   # ignore responses with multiple division operators or decimal points
                            when substring_index(sr.response_value, '/', 2) != sr.response_value then null
                            when substring_index(sr.response_value, '.', 2) != sr.response_value then null
                            # ignore responses with leading division operator
                            when sr.response_value regexp '^/' then null
                            # ignore responses ending with division operator
                            when sr.response_value regexp '/$' then null
                            # ignore responses beginning with decimal followed by division operator
                            when sr.response_value regexp '^\\./' then null
                            # ignore responses containing both decimal point and division operator
                            when sr.response_value regexp '\\.+.*/+' then null
                            when sr.response_value regexp '/+.*\\.+' then null
                            # extract and split responses requiring calculation by division
                            when instr(sr.response_value, '/') != 0 and sr.response_value regexp '[^0-9./]+' = 0 then substring_index(sr.response_value, '/', -1) 
                            else null 
                    end as denominator_value
                     ,1234
                     ,now()
            FROM    v_pmi_ods_scan_results_lexmark AS sr
            JOIN    sam_test as t 
                    ON t.test_id = sr.pm_test_id
            JOIN    c_student AS s 
                    ON s.student_code = sr.student_code
            JOIN    tmp_test_event AS te  
                    ON te.test_id = t.test_id
            JOIN    sam_answer_key AS ak 
                    ON ak.test_id = t.test_id 
                    AND ak.flatfile_question_num = sr.question_number
            JOIN    sam_question_type AS qt 
                    ON ak.question_type_id = qt.question_type_id
            JOIN    sam_scoring_method AS sm 
                    ON qt.scoring_method_id = sm.scoring_method_id
                    AND sm.scoring_method_code = 'g'
            LEFT JOIN  sam_student_response AS er     
                    ON er.test_id = t.test_id    
                    AND  er.test_event_id = te.test_event_id    
                    AND  er.student_id = s.student_id    
                    AND  er.test_question_id = ak.test_question_id
            WHERE   er.student_id is null or  er.gui_edit_flag <> 1
            ON DUPLICATE KEY UPDATE student_answer_raw = values(student_answer_raw)
                ,numerator_value = values(numerator_value)
                ,denominator_value = values(denominator_value)
                ,last_user_id = values(last_user_id)
            ;

            # user-defined test (classroom)
            insert tmp_stu_grid_response_calc (
                test_id
                ,test_event_id
                ,student_id
                ,test_question_id
                ,student_answer_raw
                ,numerator_value
                ,denominator_value
                ,last_user_id
                ,create_timestamp
            )
            
            SELECT  t.test_id
                    ,te.test_event_id
                    ,s.student_id
                    ,ak.test_question_id
                    ,sr.response_value
                    ,case   # ignore responses with multiple division operators or decimal points
                            when substring_index(sr.response_value, '/', 2) != sr.response_value then null
                            when substring_index(sr.response_value, '.', 2) != sr.response_value then null
                            # ignore responses with leading division operator
                            when sr.response_value regexp '^/' then null
                            # ignore responses ending with division operator
                            when sr.response_value regexp '/$' then null
                            # ignore responses beginning with decimal followed by division operator
                            when sr.response_value regexp '^\\./' then null
                            # ignore responses containing both decimal point and division operator
                            when sr.response_value regexp '\\.+.*/+' then null
                            when sr.response_value regexp '/+.*\\.+' then null
                            # extract and split responses requiring calculation by division
                            when instr(sr.response_value, '/') != 0 and sr.response_value regexp '[^0-9./]+' = 0 then substring_index(sr.response_value, '/', 1) 
                            else null 
                    end as numerator_value
                    ,case   # ignore responses with multiple division operators or decimal points
                            when substring_index(sr.response_value, '/', 2) != sr.response_value then null
                            when substring_index(sr.response_value, '.', 2) != sr.response_value then null
                            # ignore responses with leading division operator
                            when sr.response_value regexp '^/' then null
                            # ignore responses ending with division operator
                            when sr.response_value regexp '/$' then null
                            # ignore responses beginning with decimal followed by division operator
                            when sr.response_value regexp '^\\./' then null
                            # ignore responses containing both decimal point and division operator
                            when sr.response_value regexp '\\.+.*/+' then null
                            when sr.response_value regexp '/+.*\\.+' then null
                            # extract and split responses requiring calculation by division
                            when instr(sr.response_value, '/') != 0 and sr.response_value regexp '[^0-9./]+' = 0 then substring_index(sr.response_value, '/', -1) 
                            else null 
                    end as denominator_value
                     ,1234
                     ,now()
            FROM    v_pmi_ods_scan_results_lexmark AS sr
            JOIN    sam_test as t 
                    ON t.import_xref_code = sr.test_name
                    AND t.owner_id = sr.user_id
            JOIN    c_student AS s 
                    ON s.student_code = sr.student_code
            JOIN    tmp_test_event AS te  
                    ON te.test_id = t.test_id
            JOIN    sam_answer_key AS ak 
                    ON ak.test_id = t.test_id 
                    AND ak.flatfile_question_num = sr.question_number
            JOIN    sam_question_type AS qt 
                    ON ak.question_type_id = qt.question_type_id
            JOIN    sam_scoring_method AS sm 
                    ON qt.scoring_method_id = sm.scoring_method_id
                    AND sm.scoring_method_code = 'g'
            LEFT JOIN  sam_student_response AS er     
                    ON er.test_id = t.test_id    
                    AND  er.test_event_id = te.test_event_id    
                    AND  er.student_id = s.student_id    
                    AND  er.test_question_id = ak.test_question_id
            WHERE   sr.pm_test_id is null
            and     er.student_id is null or  er.gui_edit_flag <> 1
            ON DUPLICATE KEY UPDATE student_answer_raw = values(student_answer_raw)
                ,numerator_value = values(numerator_value)
                ,denominator_value = values(denominator_value)
                ,last_user_id = values(last_user_id)
            ;

            update  tmp_stu_grid_response_calc as upd
            join    tmp_ak_grid as akg
                    on      upd.test_id = akg.test_id
                    and     upd.test_question_id = akg.test_question_id
            set     upd.student_answer_numeric_char = 
                    case when upd.numerator_value is not null and upd.denominator_value != 0 then round(upd.numerator_value/upd.denominator_value, akg.round_digits) 
                            when upd.student_answer_raw regexp '[^0-9.]+' = 0 and upd.student_answer_raw != '.' and upd.student_answer_raw regexp '\\.+.*\\.+' = 0 then upd.student_answer_raw
                            else null
                    end
            ;

            insert into sam_student_response (
                test_id
                , test_event_id
                , student_id
                , test_question_id
                , student_answer
                , student_answer_raw_gr
                , rubric_score
                , last_user_id
                , create_timestamp
            )

            select  tmp1.test_id
                ,tmp1.test_event_id
                ,tmp1.student_id
                ,tmp1.test_question_id
                ,coalesce(tmp1.student_answer_numeric_char, tmp1.student_answer_raw)
                ,tmp1.student_answer_raw
                ,case when cast(tmp1.student_answer_numeric_char as decimal(18,9)) = cast(ak.correct_answer as decimal(18,9)) then 1 else 0 end as rubric_score
                ,1234
                ,now()
            
            from    tmp_stu_grid_response_calc as tmp1
            join    tmp_ak_grid as ak
                    on      tmp1.test_id = ak.test_id
                    and     tmp1.test_question_id = ak.test_question_id
            on duplicate key update student_answer = values(student_answer)
                ,student_answer_raw_gr = values(student_answer_raw_gr)
                ,rubric_score = values(rubric_score)
                ,last_user_id = values(last_user_id)
            ;


        end if;

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
        
        SET @sql_truncate_pivot := CONCAT('TRUNCATE TABLE ', @db_name_ods, '.pmi_ods_scan_results_lexmark');
        prepare sql_truncate_pivot from @sql_truncate_pivot;
        execute sql_truncate_pivot;
        deallocate prepare sql_truncate_pivot;

        drop table if exists `tmp_test_list`;
        drop table if exists `tmp_stu_grid_response_calc`;
        drop table if exists `tmp_ak_grid`;
        drop table if exists `tmp_test_event`;
   
        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_scan_results_lexmark', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;

    end if;
    
END;
//
