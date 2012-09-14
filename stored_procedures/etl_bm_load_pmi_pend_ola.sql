

drop procedure if exists etl_bm_load_pmi_pend_ola//

create definer=`dbadmin`@`localhost` procedure etl_bm_load_pmi_pend_ola()
contains sql
sql security invoker
comment '$Rev: 7981 $ $Date: 2009-12-09 09:47:32 -0500 (Wed, 09 Dec 2009) $'


proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set @bmDoesNotSendCRScanResults := pmi_f_get_etl_setting('bmDoesNotSendCRScanResults');

    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name
    and     t.table_name = 'v_pmi_ods_scan_results_ola';


    if @view_exists > 0 then

        drop table if exists `tmp_test_list`;
        drop table if exists `tmp_test_student`;

        create table `tmp_test_list` (
            `test_id` int(11) not null,
            primary key (`test_id`)
        );

        CREATE TABLE `tmp_test_student` (
          `test_id` int(10) NOT NULL,
          `test_event_id` int(10) NOT NULL,
          `student_id` int(10) NOT NULL,
          PRIMARY KEY  (`test_id`,`test_event_id`,`student_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        # create remote (federated) tables to Pend data
#        call etl_bm_load_pmi_pend_ola_tables();

        
        ## Make sure incoming data exists in sam_test_student
        insert into sam_test_student (test_id, test_event_id, student_id, purge_responses_flag, last_user_id, create_timestamp)
        select ods.pmi_test_id, ods.pmi_test_event_id, ods.pmi_student_id,0,1234,now()
        from v_pmi_ods_scan_results_ola ods
        left join sam_test_student sts
            on    ods.pmi_test_id = sts.test_id
            and   ods.pmi_test_event_id = sts.test_event_id
            and   ods.pmi_student_id = sts.student_id
        where sts.student_id is null
        group by ods.pmi_test_id, ods.pmi_test_event_id, ods.pmi_student_id
        ;
        
        
        insert  tmp_test_list (test_id)
        select  pmi_test_id
        from    v_pmi_ods_scan_results_ola
        group by pmi_test_id
        ;
        
        # Load results
        insert  sam_student_response (
            test_id
            ,test_event_id
            ,student_id
            ,test_question_id
            ,student_answer
            ,rubric_score
            ,last_user_id
            ,create_timestamp
        )  
        
        select  ods.pmi_test_id
            ,ods.pmi_test_event_id
            ,ods.pmi_student_id
            ,ods.pmi_test_question_id
            ,case when sm.scoring_method_code = 'c' then null else ods.student_response end
            ,case when sm.scoring_method_code in ('s','g') and ods.student_response = ak.answer then 1  
                    when sm.scoring_method_code in ('s','g') then 0  
                    when sm.scoring_method_code = 'c' and ods.student_response REGEXP '^[0-9]' = 0 then 0  
                    when sm.scoring_method_code = 'c' and ods.student_response REGEXP '^[0-9]' != 0 then cast(ods.student_response as signed)  
                    else null
             end
            ,1234
            ,now()
        from    v_pmi_ods_scan_results_ola as ods
        join    sam_answer_key as ak
                on      ak.test_id = ods.pmi_test_id
                and     ak.test_question_id = ods.pmi_test_question_id
        join    sam_question_type as qt
                on      ak.question_type_id = qt.question_type_id
        join    sam_scoring_method as sm
                on      qt.scoring_method_id = sm.scoring_method_id
        left join   sam_student_response as sr
                on      sr.test_id = ods.pmi_test_id
                and     sr.test_event_id = ods.pmi_test_event_id
                and     sr.student_id = ods.pmi_student_id
                and     sr.test_question_id = ods.pmi_test_question_id 
        where   sr.student_id is null or  sr.gui_edit_flag <> 1 
        on duplicate key update last_user_id = values(last_user_id)
            ,student_answer = values(student_answer)
            ,rubric_score = values(rubric_score)
        ;

        #############################################
        # insert no-responses
        #############################################

        insert  tmp_test_student (test_id, test_event_id, student_id)
        select  ts.test_id, ts.test_event_id, ts.student_id
        from    sam_test_student as ts
        join    tmp_test_list as tl
                on      ts.test_id = tl.test_id
        where exists (  select  *
                        from    sam_student_response as er
                        where   ts.test_id = er.test_id
                        and     ts.test_event_id = er.test_event_id
                        and     ts.student_id = er.student_id
                     )
        ;

        if @bmDoesNotSendCRScanResults = 'y' then

            insert sam_student_response (
                test_id
                , test_event_id
                , student_id
                , test_question_id
                , rubric_score
                , rubric_value
                , last_user_id
                , create_timestamp
            )  
            select  ak.test_id
                , ts.test_event_id
                , ts.student_id
                , ak.test_question_id
                , 0
                , 0
                , 1234
                , now()  
            from    sam_answer_key as ak
            join    tmp_test_list as tmp1
                    on      tmp1.test_id = ak.test_id
            join    sam_question_type as qt
                    on      ak.question_type_id = qt.question_type_id  
            join    sam_scoring_method as sm
                    on      qt.scoring_method_id = sm.scoring_method_id
                    and     sm.scoring_method_code != 'c'
            join    tmp_test_student as ts
                    on      ts.test_id = ak.test_id  
            left join    sam_student_response as sr
                    on      sr.test_id = ak.test_id
                    and     sr.test_event_id = ts.test_event_id
                    and     sr.student_id = ts.student_id
                    and     sr.test_question_id = ak.test_question_id
            where   sr.test_id is null 
            ;

        else
            insert sam_student_response (
                test_id
                , test_event_id
                , student_id
                , test_question_id
                , rubric_score
                , rubric_value
                , last_user_id
                , create_timestamp
            )  
            select  ak.test_id
                , ts.test_event_id
                , ts.student_id
                , ak.test_question_id
                , 0
                , 0
                , 1234
                , now()  
            from    sam_answer_key as ak
            join    tmp_test_list as tmp1
                    on      tmp1.test_id = ak.test_id
            join    tmp_test_student as ts
                    on      ts.test_id = ak.test_id  
            left join    sam_student_response as sr
                    on      sr.test_id = ak.test_id
                    and     sr.test_event_id = ts.test_event_id
                    and     sr.student_id = ts.student_id
                    and     sr.test_question_id = ak.test_question_id
            where   sr.test_id is null 
            ;

        end if;
        
        # Scoring
        update  sam_student_response as sr
        join    tmp_test_list as tl
                on      sr.test_id = tl.test_id
        join    sam_answer_key as ak
                on      sr.test_id = ak.test_id
                and     sr.test_question_id = ak.test_question_id
        left join    sam_rubric_list as rl
                on      ak.rubric_id = rl.rubric_id
                and     sr.rubric_score = rl.rubric_score
        set     sr.rubric_value = case when rl.rubric_item_id is null then 0 else rl.rubric_value end
        ;


    
        drop table if exists `tmp_test_list`;
        drop table if exists `tmp_test_student`;

        -- Update imp_upload_log
        set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_scan_results_ola\', \'P\', \'ETL Load Successful\')');
        
        prepare sql_string from @sql_string;
        execute sql_string;
        deallocate prepare sql_string;     

    end if;
    
end proc;
//
