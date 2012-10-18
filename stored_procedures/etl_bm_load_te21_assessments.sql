DROP PROCEDURE IF EXISTS etl_bm_load_te21_assessments //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_te21_assessments()
contains sql
sql security invoker
comment '$ Load proc for TE21 assessment $'

BEGIN 

    declare v_default_course_type_id           int(10);
    declare v_default_answer_set_id            int(11);
    declare v_default_answer_subset_id         int(11);
    declare v_default_rubric_id                int(11);
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_scan_results_pivot_te21';

    if @table_exists > 0 then

        drop table if exists `tmp_test_list`;
        drop table if exists `tmp_answer_key`;
        drop table if exists `tmp_id_assign_test`;
        drop table if exists `tmp_import_test_list`;
        drop table if exists `tmp_test_event_list`;
        drop table if exists `tmp_import_test_xref`;
        
        create table `tmp_import_test_list` (
          `import_xref_code` varchar(255) not null,
          `moniker` varchar(255) not null,
          `min_grade_level_id` int(10) default null,
          `max_grade_level_id` int(10) default null,
          `question_count` smallint(6) default null,
          `answer_set_id` int(11) default null,
          `answer_subset_id` int(11) default null,
          `owner_id` int(11) not null,
          primary key  (`import_xref_code`, `owner_id`)
        ) engine=innodb default charset=latin1
        ;
        
        create table `tmp_import_test_xref` (
          `te_21_test_id` varchar(100) not null,
          `import_xref_code` varchar(255) not null,
          primary key  (`te_21_test_id`)
        ) engine=innodb default charset=latin1
        ;
 
        create table `tmp_test_list` (
          `test_id` int(11) not null,
          `client_id` int(11) not null,
          primary key  (`test_id`)
        ) engine=innodb default charset=latin1
        ;
        
        create table `tmp_test_event_list` (
          `test_id` int(11) not null,
          `test_event_id` int(11) not null,
          primary key  (`test_id`)
        ) engine=innodb default charset=latin1
        ;
        
        create table `tmp_answer_key` (
          `import_xref_code` varchar(255) not null,
          `flatfile_question_num` smallint(6) not null,
          `answer` tinytext,
          `standard` varchar(50) default NULL,
          primary key (`import_xref_code`,`flatfile_question_num`)
        ) engine=innodb default charset=latin1
        ;
        

        create table `tmp_id_assign_test` (
          `new_id` int(11) not null,
          `base_code` varchar(255) not null,
          `userid` int(11) default 1234 not null,
          primary key  (`new_id`),
          unique key `uq_tmp_id_assign_test` (`base_code`, `userid`)
        ) engine=innodb default charset=latin1
        ;

        ######################################
        ## Setup
        ######################################
        
        # Get default answer set id and answer subset id if incoming data does not match what we have defined.
        select  min(course_type_id)
        into    v_default_course_type_id
        from    c_course_type
        where   moniker = 'Other'
        ;
        
        if v_default_course_type_id is null then
          set v_default_course_type_id := 0; #0 is Not Assigned and should be in all databases.
        end if;
        
        # Get default answer set id and answer subset id if incoming data does not match what we have defined.
        select  min(sas.answer_set_id), min(sub.answer_subset_id)
        into    v_default_answer_set_id
                ,v_default_answer_subset_id
        from    sam_answer_set sas
        join    sam_answer_subset  sub
                on    sas.answer_set_id = sub.answer_set_id
                and   sub.subset_ordinal = 1
        where   answer_set_code = 'abcd'
        ;
        
        select min(rubric_id)
        into   v_default_rubric_id
        from sam_rubric r
        where r.rubric_total = 1
          and exists (select 'x' from sam_rubric_list x where r.rubric_id = x.rubric_id and x.rubric_score = 0)
          and exists (select 'x' from sam_rubric_list x where r.rubric_id = x.rubric_id and x.rubric_score = 1)
        ;
         
          
        SELECT  MIN(CASE WHEN question_type_code = 'SR' THEN question_type_id END)
               ,MIN(CASE WHEN question_type_code = 'BCR' THEN question_type_id END)
               ,MIN(CASE WHEN question_type_code = 'GR' THEN question_type_id END)
        INTO     @sr_question_type_id
                ,@bcr_question_type_id
                ,@gr_question_type_id
        FROM  sam_question_type
        WHERE question_type_code in ('SR', 'BCR', 'GR');

        select  external_answer_source_id
        into    @external_answer_source_id
        from    sam_external_answer_source
        where   external_answer_source_code = 'other'
        ;
        
        # Create xref of incoming test id to our import_xref_code
        insert into tmp_import_test_xref (te_21_test_id, import_xref_code)
        select ods.te_21_test_id
              ,case 
                  when ods.te_21_test_id like '%x2' then concat(substring(ods.te_21_test_id,1,length(ods.te_21_test_id) - 6),'x2')
                  else substring(ods.te_21_test_id,1,length(ods.te_21_test_id) - 4)
              end as import_xref_code
        from v_pmi_ods_scan_results_pivot_te21 ods
        where ods.te_21_test_id is not null
        group by 1,2
        ;
        
        # Get list of tests coming in the ods table
        insert into tmp_import_test_list(import_xref_code,moniker, min_grade_level_id, max_grade_level_id, question_count, answer_set_id, answer_subset_id, owner_id)
        select case 
                  when ods.te_21_test_id like '%x2' then concat(substring(ods.te_21_test_id,1,length(ods.te_21_test_id) - 6),'x2')
                  else substring(ods.te_21_test_id,1,length(ods.te_21_test_id) - 4)
              end as import_xref_code
              , case
                  when ods.te_21_test_id like '%x2' then replace(concat(ods.test_name,'-',ods.test_admin, ' x2'),'&','and') 
                  else replace(concat(ods.test_name,'-',ods.test_admin),'&','and')
                end as test_name
              , min(gl.grade_level_id) as min_grade_level_id
              , max(gl.grade_level_id) as max_grade_level_id
              , count(distinct ods.item_num) as question_count
              , v_default_answer_set_id
              , v_default_answer_subset_id
              , @client_id
        from v_pmi_ods_scan_results_pivot_te21 ods
        left join c_grade_level gl
                  on    ods.grade = gl.grade_sequence
        where ods.te_21_test_id is not null
        group by 1,2
        ;
        
       
        ######################################
        ## Load sam_test 
        ######################################
        
     
        ### obtain a new test id only for test that are not already in the target table.
        INSERT tmp_id_assign_test (new_id, base_code, userid)
        SELECT DISTINCT pmi_f_get_next_sequence_app_db('sam_test', 1)
                        , tmp.import_xref_code
                        , tmp.owner_id
        FROM    tmp_import_test_list AS tmp
        LEFT JOIN   sam_test as tar
                ON      tmp.import_xref_code = tar.import_xref_code
                AND     tmp.owner_id = tar.owner_id
        WHERE   tar.import_xref_code IS NULL
        GROUP BY tmp.import_xref_code, tmp.owner_id
        ;   

        INSERT sam_test (test_id, import_xref_code, moniker, answer_source_code, generation_method_code
                        , external_answer_source_id, mastery_level, threshold_level, external_grading_flag
                        , course_type_id, answer_set_id, lexmark_form_id, client_id, owner_id, last_user_id, purge_flag, create_timestamp) 
        SELECT DISTINCT   tmpid.new_id, 
                          at.import_xref_code,
                          at.moniker,
                          'e', 
                          'e', 
                          @external_answer_source_id, 
                          70, 
                          50, 
                          0, 
                          v_default_course_type_id,
                          at.answer_set_id, 
                          null,
                          @client_id,
                          at.owner_id,
                          1234,
                          0, 
                          now() 
        FROM  tmp_import_test_list AS at  
        JOIN  tmp_id_assign_test AS tmpid
              ON    at.import_xref_code = tmpid.base_code
              AND   at.owner_id = tmpid.userid
        ;

        ##################################
        ## Build test events if necessary
        ##################################
        INSERT tmp_test_list (test_id, client_id)
        SELECT t.test_id, t.client_id
        FROM   tmp_import_test_list as ak
        JOIN   sam_test as t 
               ON ak.import_xref_code = t.import_xref_code 
               and ak.owner_id = t.owner_id
        group  by t.test_id, t.client_id
        ;

        INSERT sam_test_event (test_id, test_event_id, online_scoring_flag, start_date, end_date, admin_period_id, purge_flag, client_id, last_user_id, create_timestamp)
        SELECT  tmp1.test_id, pmi_f_get_next_sequence_app_db('sam_test_event', 1), 0, now(), null, 1000001, 0, tmp1.client_id, 1234, now()
        FROM    tmp_test_list as tmp1
        LEFT JOIN   sam_test_event as te
                ON      te.test_id = tmp1.test_id
                AND     te.purge_flag = 0
        WHERE   te.test_event_id is null
        ;

        ##################################
        ## Build sam_test_section
        ##################################
        
        insert  sam_test_section (
            test_id
            ,section_num
            ,question_count
            ,section_label
            ,gui_edit_section_label_flag
            ,last_user_id
            ,create_timestamp
        )
        
        select  t.test_id
                ,1 as section_num
                ,tmp.question_count
                ,'1'
                ,0 as gui_edit_section_label_flag
                ,1234
                ,now()
        from    tmp_import_test_list tmp
        join    sam_test t
                on    tmp.import_xref_code = t.import_xref_code
                and   tmp.owner_id = t.owner_id
        left join sam_test_section sts
                on    t.test_id = sts.test_id
                and   sts.section_num = 1
        where   sts.test_id is null
        ;

        ##################################
        ## Build sam_test_grade_level
        ##################################
        
        insert  sam_test_grade_level (
            test_id
            ,grade_level_id
            ,last_user_id
            ,create_timestamp
        )
        
        select  t.test_id
                ,gl.grade_level_id
                ,1234
                ,now()
        from    tmp_import_test_list tmp
        join    sam_test t
                on    tmp.import_xref_code = t.import_xref_code
                and   tmp.owner_id = t.owner_id
        join    c_grade_level gl
                on    gl.grade_level_id between tmp.min_grade_level_id and tmp.max_grade_level_id
        on duplicate key update last_user_id = 1234
        ;

 

        ##################################
        ## Build the Answer Keys
        ##################################
        
        insert into tmp_answer_key (import_xref_code, flatfile_question_num, answer, standard)
        select case 
                  when ods.te_21_test_id like '%x2' then concat(substring(ods.te_21_test_id,1,length(ods.te_21_test_id) - 6),'x2')
                  else substring(ods.te_21_test_id,1,length(ods.te_21_test_id) - 4)
              end as import_xref_code
              , cast(ods.item_num as signed) as flatfile_question_num
              , ods.key_answer
              , ods.standard
        from v_pmi_ods_scan_results_pivot_te21 ods
        left join c_grade_level gl
                  on    ods.grade = gl.grade_sequence
        group by 1,2
        order by 1,2; 
      
        INSERT INTO sam_answer_key
            (test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, answer_type_id, rubric_id, answer_ordinal, answer, answer_set_id, answer_subset_id, ib_question_id, flatfile_question_num, question_label, gui_edit_question_label_flag, last_user_id, create_timestamp) 
        select  t.test_id
                , pmi_f_get_next_sequence_app_db('sam_answer_key', 1) as test_question_id
                , tmp.flatfile_question_num as import_xref_code
                , 1 as section_num
                , tmp.flatfile_question_num as section_question_num
                , case when tmp.answer regexp '[^0-9.]+' = 0 then @gr_question_type_id else @sr_question_type_id end as question_type_id
                , null as answer_type_id
                , v_default_rubric_id as rubric_id
                , asi.item_ordinal
                , tmp.answer
                , t.answer_set_id
                , v_default_answer_subset_id
                , null as ib_question_id
                , tmp.flatfile_question_num
                , CONCAT('1-',tmp.flatfile_question_num) as question_label
                , 0 as gui_edit_question_label_flag
                , 1234 as last_user_id
                , now() as create_timestamp
        from    tmp_answer_key tmp
        join    sam_test t
                on  tmp.import_xref_code = t.import_xref_code
                and t.owner_id = @client_id
        left join    sam_answer_subset_item asi
                on  asi.answer_set_id = t.answer_set_id
                and asi.answer_subset_id = v_default_answer_subset_id
                and asi.answer_subset_item_code = tmp.answer
        on duplicate key update last_user_id = 1234
                                , answer = values(answer)
        ;
 
 
        ##################################
        ## Load Sam Test Student
        ##################################
        
        insert into `tmp_test_event_list` (`test_id`,`test_event_id`)
        select t.test_id, min(te.test_event_id)
        from   tmp_import_test_list as ak
        join   sam_test as t 
               ON ak.import_xref_code = t.import_xref_code 
               and ak.owner_id = t.owner_id
        join   sam_test_event te
               on   t.test_id = te.test_id
               and  te.purge_flag = 0
        group  by t.test_id
        ;
        
        insert into sam_test_student (test_id, test_event_id, student_id, purge_responses_flag, last_user_id, create_timestamp)
        select  t.test_id, tel.test_event_id, s.student_id, 0, 1234, now()
        from    v_pmi_ods_scan_results_pivot_te21 ods
        join    tmp_import_test_xref xref
                on    ods.te_21_test_id = xref.te_21_test_id
        join    sam_test t
                on    t.import_xref_code = xref.import_xref_code
                and   t.owner_id = @client_id
        join    tmp_test_event_list tel
                on    t.test_id = tel.test_id
        join    c_student s
                on    ods.student_id = s.student_code
        group by t.test_id, tel.test_event_id, s.student_id
        on duplicate key update last_user_id = 1234, purge_responses_flag = 0;
        

        
        INSERT INTO sam_student_response (test_id, test_event_id, student_id, test_question_id, student_answer, rubric_score, last_user_id, create_timestamp)
        SELECT  t.test_id
                ,tel.test_event_id
                ,s.student_id
                ,ak.test_question_id
                ,case 
                  when sm.scoring_method_code = 'c' then null 
                  else ods.student_answer end
                ,case 
                  when sm.scoring_method_code in ('s','g') and ods.student_answer = ak.answer then 1 
                  when sm.scoring_method_code in ('s','g') then 0 
                  #when sm.scoring_method_code = 'c' and ods.student_answer REGEXP '[^0-9]+' = 1 then 0
                  #when sm.scoring_method_code = 'c' then ods.student_answer
                  else 0 end
                 ,1234
                 ,current_timestamp
        from    v_pmi_ods_scan_results_pivot_te21 ods
        join    tmp_import_test_xref xref
                on    ods.te_21_test_id = xref.te_21_test_id
        join    sam_test t
                on    t.import_xref_code = xref.import_xref_code
                and   t.owner_id = @client_id
        join    tmp_test_event_list tel
                on    t.test_id = tel.test_id
        join    c_student s
                on    ods.student_id = s.student_code
        join    sam_answer_key AS ak 
                on    ak.test_id = t.test_id 
                and   ak.flatfile_question_num = ods.item_num
        join    sam_question_type AS qt 
                on    ak.question_type_id = qt.question_type_id
        join    sam_scoring_method AS sm 
                on    qt.scoring_method_id = sm.scoring_method_id
                and   sm.scoring_method_code in ('s','c','g')
        left join  sam_student_response AS er     
                on er.test_id = t.test_id    
                and  er.test_event_id = tel.test_event_id    
                and  er.student_id = s.student_id    
                and  er.test_question_id = ak.test_question_id
        where   er.student_id is null or  er.gui_edit_flag <> 1
        on duplicate key update last_user_id = 1234, student_answer = values(student_answer), rubric_score = values(rubric_score);
 
 
 
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
        ## Update Log
        #################
            
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_scan_results_pivot_te21', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;
        
        #################
        ## Cleanup
        #################
        drop table if exists `tmp_test_list`;
        drop table if exists `tmp_answer_key`;
        drop table if exists `tmp_id_assign_test`;
        drop table if exists `tmp_import_test_list`;
        drop table if exists `tmp_test_event_list`;
        drop table if exists `tmp_import_test_xref`;
        
    end if;
      
END;
//
