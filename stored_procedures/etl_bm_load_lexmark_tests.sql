/*
$Rev: 9764 $ 
$Author: randall.stanley $ 
$Date: 2010-12-08 09:50:45 -0500 (Wed, 08 Dec 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_lexmark_tests.sql $
$Id: etl_bm_load_lexmark_tests.sql 9764 2010-12-08 14:50:45Z randall.stanley $ 
*/

DROP PROCEDURE IF EXISTS etl_bm_load_lexmark_tests //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_lexmark_tests()
contains sql
sql security invoker
comment '$Rev: 9764 $ $Date: 2010-12-08 09:50:45 -0500 (Wed, 08 Dec 2010) $'

BEGIN 

    declare v_default_course_type_id           int(10);
    declare v_default_answer_set_id            int(11);
    declare v_default_answer_subset_id         int(11);
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);
    
    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_lexmark_test_ak';

    if @table_exists > 0 then

        drop table if exists `tmp_test_list`;
        create table `tmp_test_list` (
          `test_id` int(11) not null,
          `client_id` int(11) not null,
          primary key  (`test_id`)
        ) engine=innodb default charset=latin1
        ;

        drop table if exists tmp_ak_insert;
        drop table if exists tmp_test_answer_set;
        create table tmp_test_answer_set (import_xref_code char(255), userid int(11), answer_set_id int(11), answer_subset_id int(11));

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
        
        
          
        SELECT  MIN(CASE WHEN question_type_code = 'SR' THEN question_type_id END)
               ,MIN(CASE WHEN question_type_code = 'BCR' THEN question_type_id END)
        INTO     @sr_question_type_id
                ,@bcr_question_type_id
        FROM  sam_question_type
        WHERE question_type_code in ('SR', 'BCR');

        select  external_answer_source_id
        into    @external_answer_source_id
        from    sam_external_answer_source
        where   external_answer_source_code = 'lexmark'
        ;

        Insert  tmp_test_answer_set
        select at.test_name, at.user_id, coalesce(x.answer_set_id,v_default_answer_set_id) , coalesce(y.answer_subset_id,v_default_answer_set_id)
        from    v_pmi_ods_lexmark_test_ak at
        left join sam_answer_set x
                on    at.bubble_codes = x.answer_set_code
                and   at.pm_test_id is null
        left join sam_answer_subset y
                on    x.answer_set_id = y.answer_set_id
                and   y.subset_ordinal = 1
        group by at.test_name, at.user_id, x.answer_set_id, y.answer_subset_id
        ;
                
        Insert  tmp_test_answer_set
        select distinct at.test_name, at.user_id, x.answer_set_id, y.answer_subset_id
        from    sam_answer_set x
        join    sam_answer_subset y
                on    x.answer_set_id = y.answer_set_id
                and   y.subset_ordinal = 1
        join    v_pmi_ods_lexmark_test_ak at
                on    at.bubble_codes = x.answer_set_code
                and   at.pm_test_id is null;

        ######################################
        ## Load sam_test and sam_test_event ##
        ######################################
        DROP TABLE IF EXISTS tmp_id_assign_test;
        CREATE TABLE `tmp_id_assign_test` (
          `new_id` int(11) not null,
          `base_code` varchar(255) not null,
          `userid` int(11) default 1234 not null,
          PRIMARY KEY  (`new_id`),
          UNIQUE KEY `uq_tmp_id_assign_test` (`base_code`, `userid`)
        );
          
      
        ### obtain a new test id only for test that are not already in the target table.
        INSERT tmp_id_assign_test (new_id, base_code, userid)
        SELECT DISTINCT pmi_f_get_next_sequence_app_db('sam_test', 1), ods.test_name, ods.user_id
        FROM    v_pmi_ods_lexmark_test_ak AS ods
        LEFT JOIN   sam_test as tar
                ON      ods.test_name = tar.import_xref_code
                AND     ods.user_id = tar.owner_id
        WHERE   tar.import_xref_code IS NULL
        AND     ods.pm_test_id is null
        GROUP BY ods.test_name, ods.user_id
          ;   

        INSERT sam_test (test_id, import_xref_code, moniker, answer_source_code, generation_method_code, external_answer_source_id, mastery_level, threshold_level, external_grading_flag, course_type_id, answer_set_id, lexmark_form_id, client_id, owner_id, last_user_id, purge_flag, create_timestamp) 
        SELECT DISTINCT   tmpid.new_id, 
                          at.test_name,
                          at.test_name,
                          'e', 
                          'e', 
                          @external_answer_source_id, 
                          70, 
                          50, 
                          0, 
                          coalesce(ct.course_type_id,v_default_course_type_id), #at.subject_id,
                          tmp.answer_set_id, 
                          lf.lexmark_form_id,
                          at.pm_district_id,
                          at.user_id,
                          at.user_id, 
                          0, 
                          now() 
        FROM  v_pmi_ods_lexmark_test_ak AS at  
        JOIN  tmp_test_answer_set tmp
              ON  tmp.import_xref_code = at.test_name
              AND tmp.userid = at.user_id
        JOIN tmp_id_assign_test AS tmpid
                    ON at.test_name = tmpid.base_code
                    AND at.user_id = tmpid.userid
        JOIN sam_lexmark_form AS lf
                    ON lf.form_code = at.form_name
                    AND lf.client_id = at.pm_district_id
        LEFT JOIN c_course_type ct
                    ON  at.subject_id = ct.course_type_id
              ;
        
        INSERT tmp_test_list (test_id, client_id)
        SELECT DISTINCT t.test_id, t.client_id
        FROM   v_pmi_ods_lexmark_test_ak as ak
        JOIN   sam_test as t 
               ON ak.test_name = t.import_xref_code 
               and ak.user_id = t.owner_id
               and ak.pm_test_id is null;

        INSERT sam_test_event (test_id, test_event_id, online_scoring_flag, start_date, end_date, admin_period_id, purge_flag, client_id, last_user_id, create_timestamp)
        SELECT  tmp1.test_id, pmi_f_get_next_sequence_app_db('sam_test_event', 1), 0, now(), null, 1000001, 0, tmp1.client_id, 1234, now()
        FROM    tmp_test_list as tmp1
        LEFT JOIN   sam_test_event as te
                ON      te.test_id = tmp1.test_id
                AND     te.purge_flag = 0
        WHERE   te.test_event_id is null
        ;

        ##################################
        ## Build the Answer Keys
        ##################################
        
        CREATE TABLE tmp_ak_insert
        SELECT DISTINCT t.test_id
           ,pmi_f_get_next_sequence_app_db('sam_answer_key', 1) as test_question_id
           ,at.question_code as import_xref_code
           ,1 as section_num
           ,at.flatfile_question_number as section_question_num
           ,CASE WHEN at.question_type IN ('CR','OR') THEN @bcr_question_type_id ELSE @sr_question_type_id END as question_type_id
           ,coalesce(r.rubric_id, 1000002) as rubric_id
           ,CONCAT('1-',at.question_code) as question_label
           ,at.flatfile_question_number as flatfile_question_num
           ,at.answer AS answer
           ,tas.answer_set_id
           ,tas.answer_subset_id
        FROM   (SELECT distinct test_name, user_id, question_number as flatfile_question_number, points_possible as points_possible, correct_answer as answer, question_number as question_code, question_type
             FROM v_pmi_ods_lexmark_test_ak) AS  at
        JOIN    sam_test AS t   
          ON      at.test_name = t.import_xref_code
          AND     at.user_id = t.owner_id
        JOIn    tmp_test_answer_set tas
          on      tas.import_xref_code = t.import_xref_code
          and     tas.userid = at.user_id
        left join    sam_rubric AS r
          on      r.moniker = concat('lexmark_', at.points_possible,' Point (', at.points_possible,')')
        WHERE NOT EXISTS (SELECT * FROM sam_answer_key ak WHERE ak.test_id = t.test_id);
        

        INSERT sam_answer_key (test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, rubric_id, answer_set_id, answer_subset_id, question_label, flatfile_question_num, answer, last_user_id, create_timestamp)    
        SELECT  test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, rubric_id, answer_set_id, answer_subset_id, question_label, flatfile_question_num, answer, 1234, now()
        FROM    tmp_ak_insert
        ON DUPLICATE KEY UPDATE last_user_id = 1234
        ;

        insert  sam_test_section (
            test_id
            ,section_num
            ,question_count
            ,section_label
            ,gui_edit_section_label_flag
            ,last_user_id
            ,create_timestamp
        )

        select  dt.test_id
            ,dt.section_num
            ,dt.question_count
            ,cast(dt.section_num as char(10))
            ,0
            ,1234
            ,now()

        from    (
                    select  ak.test_id, ak.section_num, count(ak.test_question_id) as question_count
                    
                    from    tmp_test_list as tl
                    join    sam_answer_key as ak
                            on      ak.test_id = tl.test_id
                    group by ak.test_id, ak.section_num
                ) as dt
        left join   sam_test_section as tar
                on      dt.test_id = tar.test_id
                and     dt.section_num = tar.section_num
                and     tar.gui_edit_section_label_flag = 1
        where   tar.section_num is null
        on duplicate key update question_count = values(question_count)
            ,section_label = values(section_label)
            ,last_user_id = values(last_user_id)
        ;

        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_lexmark_test_ak', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;
        
    end if;
      
END;
//
