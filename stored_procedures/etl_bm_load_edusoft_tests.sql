/*
$Rev: 8543 $ 
$Author: randall.stanley $ 
$Date: 2010-05-10 08:20:10 -0400 (Mon, 10 May 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_edusoft_tests.sql $
$Id: etl_bm_load_edusoft_tests.sql 8543 2010-05-10 12:20:10Z randall.stanley $ 
 */



DROP PROCEDURE IF EXISTS etl_bm_load_edusoft_tests //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_edusoft_tests()
CONTAINS SQL
COMMENT '$Rev: 8543 $ $Date: 2010-05-10 08:20:10 -0400 (Mon, 10 May 2010) $'
SQL SECURITY INVOKER

BEGIN 


    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_edusoft_test_ak';

    if @table_exists > 0 then


    
        drop table if exists tmp_test_list;
        create table tmp_test_list (test_id int(11) not null);
        drop table if exists tmp_ak_insert;
               
        SELECT  MIN(CASE WHEN question_type_code = 'SR' THEN question_type_id END)
               ,MIN(CASE WHEN question_type_code = 'BCR' THEN question_type_id END)
        INTO     @sr_question_type_id
                ,@bcr_question_type_id
        FROM  sam_question_type
        WHERE question_type_code in ('SR', 'BCR');
        

        select  external_answer_source_id
        into    @external_answer_source_id
        from    sam_external_answer_source
        where   external_answer_source_code = 'edusoft'
        ;

        select  answer_set_id
        into    @answer_set_id
        from    sam_answer_set
        where   answer_set_code = '1234'
        ;

        select  answer_subset_id
        into    @answer_subset_id
        from    sam_answer_subset
        where   answer_set_id = @answer_set_id
        and     subset_ordinal = 1
        ;
        
        select  course_type_id
        into    @other_course_type_id
        from    c_course_type
        where   moniker = 'Other'
        ;
        

        ######################################
        ## Load sam_test and sam_test_event ##
        ######################################
       DROP TABLE IF EXISTS tmp_id_assign_test;
          CREATE TABLE tmp_id_assign_test (
              new_id int(11) not null,
              base_code varchar(20) not null,
              PRIMARY KEY  (`new_id`),
              UNIQUE KEY `uq_tmp_id_assign_test` (`base_code`)
          );
          
      
          ### obtain a new test id only for test that are not already in the target table.
          INSERT tmp_id_assign_test (new_id, base_code)
          SELECT  pmi_f_get_next_sequence_app_db('sam_test', 1), ods.test_id
          FROM    v_pmi_ods_edusoft_test_ak AS ods
          LEFT JOIN   sam_test as tar
                  ON      ods.test_id = tar.import_xref_code
          WHERE   tar.import_xref_code IS NULL
          GROUP BY ods.test_id 
          ;   
        
        INSERT sam_test (
            test_id
            , import_xref_code
            , moniker
            , answer_source_code
            , generation_method_code
            , external_answer_source_id
            , mastery_level
            , threshold_level
            , external_grading_flag
            , course_type_id
            , answer_set_id
            , client_id
            , owner_id
            , last_user_id
            , purge_flag
            , create_timestamp
        ) 
        SELECT DISTINCT   tmpid.new_id 
            ,at.test_id
            ,at.test_name
            ,'e'
            ,'e'
            ,@external_answer_source_id
            ,70
            ,50
            ,0
            ,@other_course_type_id
            ,@answer_set_id
            ,@client_id
            ,@client_id
            ,1234
            ,0
            ,now() 
        FROM  v_pmi_ods_edusoft_test_ak AS at  
        LEFT JOIN tmp_id_assign_test AS tmpid
                    ON at.test_id = tmpid.base_code
        WHERE NOT EXISTS (SELECT * 
                            FROM sam_test AS t 
                            WHERE  at.test_id = t.import_xref_code)       
              AND at.test_id IS NOT NULL
              ;
        
        
        INSERT tmp_test_list (test_id) 
        SELECT t.test_id 
        FROM v_pmi_ods_edusoft_test_ak at 
          JOIN  sam_test t ON 
            at.test_id = t.import_xref_code 
        GROUP BY t.test_id;


        INSERT sam_test_event (test_id, test_event_id, online_scoring_flag, start_date, end_date, admin_period_id, purge_flag, client_id, last_user_id, create_timestamp)
        SELECT  tmp1.test_id, pmi_f_get_next_sequence_app_db('sam_test_event', 1), 0, now(), null, 1000001, 0, @client_id, 1234, now()
        FROM    tmp_test_list as tmp1
        LEFT JOIN   sam_test_event as te
                ON      te.test_id = tmp1.test_id
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
           ,CASE WHEN at.answer IN ('L','S') THEN @bcr_question_type_id ELSE @sr_question_type_id END as question_type_id
           ,coalesce(r.rubric_id, 1000002) as rubric_id
           ,CONCAT('1-',at.question_code) as question_label
           ,at.flatfile_question_number as flatfile_question_num
           ,at.answer AS answer
        FROM   (SELECT distinct test_id, flatfile_question_number as flatfile_question_number, cast(points_possible as signed) as points_possible, answer, question_code
             FROM v_pmi_ods_edusoft_test_ak) AS  at
        JOIN    sam_test AS t   
          ON      at.test_id = t.import_xref_code
        left join    sam_rubric AS r
          on      r.moniker = concat('edusoft_', at.points_possible,' Point (', at.points_possible,')')
        WHERE NOT EXISTS (SELECT * FROM sam_answer_key ak WHERE ak.test_id = t.test_id)
              AND at.question_code NOT IN ('A','B');
        

        INSERT sam_answer_key (test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, rubric_id, answer_set_id, answer_subset_id, question_label, flatfile_question_num, answer, last_user_id, create_timestamp)    
        SELECT  test_id, test_question_id, import_xref_code, section_num, section_question_num, question_type_id, rubric_id, @answer_set_id, @answer_subset_id, question_label, flatfile_question_num, answer, 1234, now()
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
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_edusoft_test_ak', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;

    end if;

END;
//
