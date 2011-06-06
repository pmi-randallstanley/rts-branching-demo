drop procedure if exists etl_bm_load_pmi_scan_eng_results//

create definer=`dbadmin`@`localhost` procedure etl_bm_load_pmi_scan_eng_results()
contains sql
sql security invoker
comment '$Rev$ $Date$'


proc: begin 

    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_view_exists tinyint(1);
    declare v_grid_resp_type_exists int default '0';

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_scan_results_pmi_scan_eng';
    set v_ods_view = concat('v_', v_ods_table);

    select  count(*)
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;
    

    if v_view_exists > 0 then

        drop table if exists `tmp_test_list`;
#         drop table if exists `tmp_test_student`;
        drop table if exists `tmp_test_student_scan_page`;
        drop table if exists `tmp_stu_grid_response_calc`;
        drop table if exists `tmp_ak_grid`;

        create table `tmp_test_list` (
          `test_id` int(11) not null,
          `client_id` int(11) not null,
          primary key  (`test_id`)
        ) engine=innodb default charset=latin1
        ;

#         create table `tmp_test_student` (
#           `test_id` int(10) not null,
#           `test_event_id` int(10) not null,
#           `student_id` int(10) not null,
#           `sis_student_code` varchar(15) not null,
#           `omr_processor_scan_code` varchar(100) not null,
#           primary key  (`test_id`,`test_event_id`,`student_id`),
#           unique key `uq_tmp_test_student` (`test_id`,`test_event_id`,`sis_student_code`),
#           key `ind_tmp_test_student` (`sis_student_code`)
#         ) engine=innodb default charset=latin1
#         ;

        create table `tmp_test_student_scan_page` (
          `test_id` int(10) not null,
          `test_event_id` int(10) not null,
          `student_id` int(10) not null,
          `sis_student_code` varchar(15) not null,
          `scan_page_num` tinyint(4) not null,
          `omr_processor_scan_code` varchar(100) not null,
          primary key  (`test_id`,`test_event_id`,`student_id`,`scan_page_num`),
          unique key `uq_tmp_test_student_scan_page` (`test_id`,`test_event_id`,`sis_student_code`,`scan_page_num`),
          key `ind_tmp_test_student_scan_page_1` (`sis_student_code`),
          key `ind_tmp_test_student_scan_page_2` (`test_id`,`scan_page_num`)
        ) engine=innodb default charset=latin1
        ;

        create table `tmp_stu_grid_response_calc` (
          `test_id` int(11) not null,
          `test_event_id` int(11) not null,
          `student_id` int(11) not null,
          `test_question_id` int(11) not null,
          `student_answer_raw` varchar(255) default null,
          `student_answer_numeric_char` varchar(25) default null,
          `numerator_value` smallint(6) default null,
          `denominator_value` smallint(6) default null,
          `gui_edit_flag` tinyint(1) not null default '0',
          `last_user_id` int(11) not null,
          `create_timestamp` datetime not null default '1980-12-31 00:00:00',
          `last_edit_timestamp` timestamp not null default current_timestamp on update current_timestamp,
          primary key  (`test_id`,`test_event_id`,`student_id`,`test_question_id`),
          key `ind_tmp_stu_grid_response_calc_stu` (`student_id`),
          key `ind_tmp_stu_grid_response_calc_ak` (`test_id`,`test_question_id`),
          key `ind_tmp_stu_grid_response_calc_stu_question` (`test_id`,`test_question_id`,`student_id`)
        ) engine=innodb default charset=latin1
        ;

        create table `tmp_ak_grid` (
          `test_id` int(11) not null,
          `test_question_id` int(11) not null,
          `correct_answer` varchar(255) not null,
          `round_digits` tinyint(4) not null,
          `last_user_id` int(11) not null,
          `create_timestamp` datetime not null default '1980-12-31 00:00:00',
          `last_edit_timestamp` timestamp not null default current_timestamp on update current_timestamp,
          primary key  (`test_id`,`test_question_id`),
          key `ind_tmp_ak_grid_round_digits` (`round_digits`,`test_id`,`test_question_id`)
        ) engine=innodb default charset=latin1
        ;

        insert tmp_test_list (
            test_id
            ,client_id
        )
        
        select test_id
            ,client_id
        
        from    v_pmi_ods_scan_results_pmi_scan_eng
        group by test_id, client_id
        ;

        insert tmp_test_student_scan_page (
            test_id
            ,test_event_id
            ,student_id
            ,sis_student_code
            ,scan_page_num
            ,omr_processor_scan_code
        )
        
        select ods.test_id
            ,ods.test_event_id
            ,min(s.student_id)
            ,ods.sis_student_code
            ,coalesce(ods.scan_page_num, 1)
            ,max(omr_processor_scan_code)
        
        from    v_pmi_ods_scan_results_pmi_scan_eng as ods
        join    c_student as s
                on      ods.sis_student_code = s.student_code
        group by ods.test_id, ods.test_event_id, ods.sis_student_code, ods.scan_page_num
        ;

        select  count(*)
        into    v_grid_resp_type_exists
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


        insert sam_test_student (
            test_id
            ,test_event_id
            ,student_id
#             ,omr_processor_scan_code
            ,last_user_id
            ,create_timestamp
        )
        
        select  test_id
            ,test_event_id
            ,student_id
#             ,omr_processor_scan_code
            ,1234
            ,now()

        from    tmp_test_student_scan_page
        group by test_id, test_event_id, student_id
        on duplicate key update last_user_id = values(last_user_id)
        ;
        

        insert sam_test_student_scan_page_list (
            test_id
            ,test_event_id
            ,student_id
            ,scan_page_num
            ,omr_processor_scan_code
            ,last_user_id
            ,create_timestamp
        )

        select  test_id
            ,test_event_id
            ,student_id
            ,scan_page_num
            ,omr_processor_scan_code
            ,1234
            ,now()

        from    tmp_test_student_scan_page
        on duplicate key update omr_processor_scan_code = values(omr_processor_scan_code)
            ,last_user_id = values(last_user_id)
        ;

        insert into sam_student_response (
            test_id
            , test_event_id
            , student_id
            , test_question_id
            , student_answer
            , rubric_score
            , last_user_id
            , create_timestamp
        )

        select  tts.test_id
            ,tts.test_event_id
            ,tts.student_id
            ,ak.test_question_id
            ,case when sm.scoring_method_code = 'c' then null 
                else ods.student_response 
            end
            ,case when sm.scoring_method_code = 's' and ods.student_response = ak.answer then 1 
                when sm.scoring_method_code = 's' then 0 
                when sm.scoring_method_code = 'c' and ods.student_response regexp '[^0-9]+' = 1 then 0
                when sm.scoring_method_code = 'c' then ods.student_response
                else 0 
            end
            ,1234
            ,now()
             
        from    v_pmi_ods_scan_results_pmi_scan_eng as ods
        join    tmp_test_student_scan_page as tts
                on      ods.test_id = tts.test_id
                and     ods.test_event_id = tts.test_event_id
                and     ods.sis_student_code = tts.sis_student_code
                and     tts.scan_page_num = 1
        join    sam_answer_key as ak 
                on      ak.test_id = tts.test_id 
                and     ak.flatfile_question_num = ods.flatfile_question_number
        join    sam_question_type as qt 
                on      ak.question_type_id = qt.question_type_id
        join    sam_scoring_method as sm 
                on      qt.scoring_method_id = sm.scoring_method_id
                and     sm.scoring_method_code in ('s','c')
        left join  sam_student_response as sr     
                on      sr.test_id = tts.test_id    
                and     sr.test_event_id = tts.test_event_id    
                and     sr.student_id = tts.student_id    
                and     sr.test_question_id = ak.test_question_id
        where   sr.student_id is null or  sr.gui_edit_flag <> 1
        on duplicate key update last_user_id = 1234, student_answer = values(student_answer), rubric_score = values(rubric_score)
        ;

        #############################################
        # Process Gridded Response Question Type
        #############################################

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
            
            select  tts.test_id
                    ,tts.test_event_id
                    ,tts.student_id
                    ,ak.test_question_id
                    ,ods.student_response
                    ,case   # ignore responses with multiple division operators or decimal points
                            when substring_index(ods.student_response, '/', 2) != ods.student_response then null
                            when substring_index(ods.student_response, '.', 2) != ods.student_response then null
                            # ignore responses with leading division operator
                            when ods.student_response regexp '^/' then null
                            # ignore responses ending with division operator
                            when ods.student_response regexp '/$' then null
                            # ignore responses beginning with decimal followed by division operator
                            when ods.student_response regexp '^\\./' then null
                            # ignore responses containing both decimal point and division operator
                            when ods.student_response regexp '\\.+.*/+' then null
                            when ods.student_response regexp '/+.*\\.+' then null
                            # extract and split responses requiring calculation by division
                            when instr(ods.student_response, '/') != 0 and ods.student_response regexp '[^0-9./]+' = 0 then substring_index(ods.student_response, '/', 1) 
                            else null 
                    end as numerator_value
                    ,case   # ignore responses with multiple division operators or decimal points
                            when substring_index(ods.student_response, '/', 2) != ods.student_response then null
                            when substring_index(ods.student_response, '.', 2) != ods.student_response then null
                            # ignore responses with leading division operator
                            when ods.student_response regexp '^/' then null
                            # ignore responses ending with division operator
                            when ods.student_response regexp '/$' then null
                            # ignore responses beginning with decimal followed by division operator
                            when ods.student_response regexp '^\\./' then null
                            # ignore responses containing both decimal point and division operator
                            when ods.student_response regexp '\\.+.*/+' then null
                            when ods.student_response regexp '/+.*\\.+' then null
                            # extract and split responses requiring calculation by division
                            when instr(ods.student_response, '/') != 0 and ods.student_response regexp '[^0-9./]+' = 0 then substring_index(ods.student_response, '/', -1) 
                            else null 
                    end as denominator_value
                     ,1234
                     ,now()
            from    v_pmi_ods_scan_results_pmi_scan_eng as ods
            join    tmp_test_student_scan_page as tts
                    on      ods.test_id = tts.test_id
                    and     ods.test_event_id = tts.test_event_id
                    and     ods.sis_student_code = tts.sis_student_code
                    and     tts.scan_page_num = 1
            join    sam_answer_key as ak 
                    on ak.test_id = tts.test_id 
                    and ak.flatfile_question_num = ods.flatfile_question_number
            join    sam_question_type as qt 
                    on ak.question_type_id = qt.question_type_id
            join    sam_scoring_method as sm 
                    on qt.scoring_method_id = sm.scoring_method_id
                    and sm.scoring_method_code = 'g'
            left join  sam_student_response as sr     
                    on sr.test_id = tts.test_id    
                    and  sr.test_event_id = tts.test_event_id    
                    and  sr.student_id = tts.student_id    
                    and  sr.test_question_id = ak.test_question_id
            where   sr.student_id is null or  sr.gui_edit_flag <> 1
            on duplicate key update student_answer_raw = values(student_answer_raw)
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
        # insert no-responses
        #############################################

        insert sam_student_response (
                test_id
                ,test_event_id
                ,student_id
                ,test_question_id 
                ,rubric_score
                ,rubric_value
                ,last_user_id
                ,create_timestamp
        )  
        select  ts.test_id
                ,ts.test_event_id
                ,ts.student_id
                ,ak.test_question_id
                ,0
                ,0
                ,1234
                ,now()  
        from    sam_answer_key as ak  
        join    tmp_test_list as tmp1          
                on      tmp1.test_id = ak.test_id  
        join    sam_question_type as qt          
                on      ak.question_type_id = qt.question_type_id  
        join    sam_scoring_method as sm          
                on      qt.scoring_method_id = sm.scoring_method_id          
        join    tmp_test_student_scan_page as ts          
                on      ts.test_id = ak.test_id
                and     ts.scan_page_num = 1
        left join   sam_student_response as sr          
                on      sr.test_id = ts.test_id          
                and     sr.test_event_id = ts.test_event_id          
                and     sr.student_id = ts.student_id          
                and     sr.test_question_id = ak.test_question_id  
        where sr.test_question_id is null
        ;

        #############################################
        # sam_student_response-scoring
        #############################################

        update  sam_student_response as upd
        join    tmp_test_list as tl
                on      upd.test_id = tl.test_id
        join    sam_answer_key as ak
                on      upd.test_id = ak.test_id
                and     upd.test_question_id = ak.test_question_id
        join    sam_rubric_list as rl
                on      ak.rubric_id = rl.rubric_id
                and     upd.rubric_score = rl.rubric_score
        set     upd.rubric_value = rl.rubric_value
        ;

        #################
        ## Update Log
        #################
        set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', v_ods_table, '\', \'P\', \'ETL Load Successful\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;

        drop table if exists `tmp_test_list`;
        drop table if exists `tmp_test_student`;
        drop table if exists `tmp_test_student_scan_page`;
        drop table if exists `tmp_stu_grid_response_calc`;
        drop table if exists `tmp_ak_grid`;

    end if;

end proc;
//
