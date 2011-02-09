/*
$Rev$ 
$Author$
$Date$
$HeadURL$
$Id$
*/

delimiter //
DROP PROCEDURE IF EXISTS etl_bm_load_intel_tests //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_intel_tests()
contains sql
sql security invoker
comment '$Rev$ $Date$'

BEGIN

    declare v_course_type_id            int;

    declare v_default_answer_set_id            int(11);
    declare v_default_answer_subset_id         int(11);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name IN ('pmi_ods_intel_assess_ak'
                            ,'pmi_ods_intel_assess_test')
    ;

    drop table if exists `tmp_test`;

    if @table_exists >= 2 then

        select  course_type_id
        into    v_course_type_id
        from    c_course_type
        where   moniker = 'Other'
        ;

        create table `tmp_test` (
          `test_id` int(11) NOT NULL,
          `external_test_id` varchar(50) DEFAULT NULL,
          `test_name` varchar(100) DEFAULT NULL,
          primary key (`test_id`),
          key (`external_test_id`)
        ) engine=innodb default charset=latin1;

        # sam_test
        insert into tmp_test (
            test_id
           ,external_test_id
           ,test_name
        )
        select pmi_f_get_next_sequence_app_db('sam_test', 1)
              ,ot.external_test_id
              ,ot.test_name
        from pmi_ods_intel_assess_test ot
        join pmi_ods_intel_assess_ak oak
          on oak.external_test_id = ot.external_test_id
        left join sam_test t
          on ot.external_test_id = t.import_xref_code
        where t.test_id is null
        group by 2
        ;

        insert into sam_test (
            test_id
           ,import_xref_code
           ,moniker
           ,answer_source_code
           ,generation_method_code
           ,search_tag
           ,external_answer_source_id
           ,mastery_level
           ,threshold_level
           ,course_type_id
           ,answer_set_id
           ,client_id
           ,owner_id
           ,doc_upload_timestamp
           ,last_user_id
           ,create_timestamp
           ,last_edit_timestamp
        )
        select tt.test_id
              ,tt.test_id
              ,tt.test_name
              ,'e'
              ,'i'
              ,'intel assess'
              ,1000001
              ,70
              ,50
              ,1000000        # other
              ,sas.answer_set_id
              ,@client_id
              ,@client_id
              ,now()
              ,1234
              ,now()
              ,now()
        from tmp_test tt
        join sam_answer_set sas
          on sas.answer_set_code = 'abcd'
        ;

        SET @ndx = 0;
        SET @last_test_id = 9999999999;

        # sam_test_layout
        insert into sam_test_layout (
            element_id
           ,test_id
           ,passage_id
           ,question_id
           ,client_id
           ,last_user_id
           ,create_timestamp
        )
        select IF(@last_test_id = test_id, @ndx := @ndx + 1, @ndx := 1) as element_id
              ,@last_test_id := test_id                                 as test_id
              ,passage_id
              ,IF(is_passage = 0, NULL, question_id)
              ,client_id
              ,last_user_id
              ,create_timestamp
        from (select tt.test_id
                    ,q.passage_id
                    ,q.question_id
                    ,@client_id    as client_id
                    ,1234          as last_user_id
                    ,NOW()         as create_timestamp
                    ,1             as is_passage
              from tmp_test tt
              join pmi_ods_intel_assess_ak oak
                on tt.external_test_id = oak.external_test_id
              join ca_ib.question q
                on q.import_xref_code = oak.external_question_id
              union all
              select tt.test_id
                    ,q.passage_id
                    ,q.question_id
                    ,@client_id    as client_id
                    ,1234          as last_user_id
                    ,NOW()         as create_timestamp
                    ,0             as is_passage
              from tmp_test tt
              join pmi_ods_intel_assess_ak oak
                on tt.external_test_id = oak.external_test_id
              join ca_ib.question q
                on q.import_xref_code = oak.external_question_id
              where q.passage_id IS NOT NULL) tmp
         order by test_id, question_id, is_passage
         ;

         insert into sam_test_section (
             test_id
            ,section_num
            ,question_count
            ,section_label
            ,gui_edit_section_label_flag
            ,last_user_id
            ,create_timestamp
         )
         select tt.test_id
               ,1
               ,count(*) as question_count
               ,1
               ,0
               ,1234
               ,NOW()
         from tmp_test tt
         join pmi_ods_intel_assess_ak oak
           on tt.external_test_id = oak.external_test_id
         group by tt.test_id
         ; 


        # test_event
#        insert into sam_test_event (
#            test_id
#           ,test_event_id
#           ,start_date
#           ,end_date
#           ,admin_period_id
#           ,client_id
#           ,last_user_id
#           ,create_timestamp
#        )
#        select t.test_id
#              ,pmi_f_get_next_sequence_app_db('sam_test_event', 1)
#              ,now()
#              ,NULL
#              ,1000001
#              ,@client_id
#              ,1234
#              ,now()
#        from tmp_test t
#        left join sam_test_event te
#          on te.test_id = t.test_id
#         and te.ola_flag   = 0
#         and te.purge_flag = 0
#        where te.test_event_id is null
#        ;

#        SET @ndx = 0;

#        # sam_answer_key
#        insert sam_answer_key (
#            test_id
#           ,test_question_id
#           ,section_num
#           ,section_question_num
#           ,question_type_id
#           ,rubric_id
#           ,answer_ordinal
#           ,answer
#           ,answer_set_id
#           ,answer_subset_id
#           ,ib_question_id
#           ,last_user_id
#           ,create_timestamp
#        )
#        select tt.test_id
#              ,pmi_f_get_next_sequence_app_db('sam_answer_key', 1)
#              ,1
#              ,(@ndx := @ndx + 1)
#              ,sqt.question_type_id
#              ,1000002 as rubric_id
#              ,oak.section_question_num
#              ,oak.correct_answer
#              ,1000001
#              ,1000001
#              ,q.question_id
#              ,1234
#              ,NOW()
#        from pmi_ods_intel_assess_ak oak
#        join tmp_test tt
#          on tt.external_test_id = oak.external_test_id
#        join ca_ib.question q
#          on q.import_xref_code = oak.external_question_id
#        join sam_question_type sqt   
#          on sqt.question_type_code = 'sr'
#        ;

#        insert sam_alignment_list (
#            test_id
#           ,test_question_id
#           ,curriculum_id
#           ,last_user_id
#           ,create_timestamp
#        )
#        select ak.test_id
#              ,ak.test_question_id
#              ,qal.curriculum_id
#              ,1234
#              ,NOW()
#        from sam_answer_key ak
#        join ca_ib.question q
#          on q.question_id = ak.ib_question_id
#        join ca_ib.question_alignment_list qal
#          on q.question_id = qal.question_id
#        on duplicate key update last_user_id = 1234
#        ;

        #################
        ## Update Log
        #################
            
#        SET @sql_scan_log := '';
#        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_lexmark_test_ak', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
#        prepare sql_scan_log from @sql_scan_log;
#        execute sql_scan_log;
#        deallocate prepare sql_scan_log;
        
    end if;

    drop table if exists `tmp_test`;

END;
//
