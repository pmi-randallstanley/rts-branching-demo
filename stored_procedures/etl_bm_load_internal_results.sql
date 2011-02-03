/*
$Rev: 8452 $ 
$Author: mike.torian $ 
$Date: 2010-04-21 09:06:02 -0400 (Wed, 21 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_internal_results.sql $
$Id: etl_bm_load_internal_results.sql 8452 2010-04-21 13:06:02Z mike.torian $ 
*/

DROP PROCEDURE IF EXISTS etl_bm_load_internal_results  //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_internal_results()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 8452 $ $Date: 2010-04-21 09:06:02 -0400 (Wed, 21 Apr 2010) $'

BEGIN

    call set_db_vars(@client_id,@state_id,@db_name,@db_name_core,@db_name_ods,@db_name_ib,@db_name_view,@db_name_pend);

    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_ods
    and     t.table_name = 'pmi_ods_scan_results_pivot_internal';

    if @table_exists > 0 then

        drop table if exists tmp_test_list;
        drop table if exists tmp_test_student;
        
        create table tmp_test_list (test_id int(11) not null);

        CREATE TABLE `tmp_test_student` (
          `test_id` int(10) NOT NULL,
          `test_event_id` int(10) NOT NULL,
          `student_id` int(10) NOT NULL,
          PRIMARY KEY  (`test_id`,`test_event_id`,`student_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        #############################################
        # get list of test_ids to process 
        #############################################

        
        INSERT tmp_test_list (test_id)
        SELECT t.test_id
        FROM   v_pmi_ods_scan_results_pivot_internal at
        JOIN   sam_test t 
                ON at.test_id = t.test_id
        GROUP BY t.test_id
        ;

        #############################################
        # sam_test_student 
        #############################################
    
        INSERT INTO sam_test_student (
                test_id
                ,test_event_id
                ,student_id
                ,last_user_id
                ,create_timestamp) 
        SELECT  te.test_id
                ,te.test_event_id
                ,s.student_id
                ,1234
                ,current_timestamp  
        FROM v_pmi_ods_scan_results_pivot_internal AS sr  
        JOIN sam_test as t      
                ON t.test_id = sr.test_id  
        JOIN sam_test_event AS te
                ON te.test_id = t.Test_id
        JOIN c_student as s
                ON sr.student_code = s.student_code
        JOIN sam_test_student as sts
                on te.test_id = sts.test_id
                and te.test_event_id = sts.test_event_id
                and s.student_id = sts.student_id
        where s.student_id is null
        GROUP BY te.test_id,te.test_event_id,s.student_id
        ON DUPLICATE KEY UPDATE last_user_id = 1234   
        ;
  
        #############################################
        # sam_student_response
        #############################################
    
        INSERT INTO sam_student_response (
                test_id
                ,test_event_id
                ,student_id
                ,test_question_id
                ,student_answer
                ,rubric_score
                ,last_user_id
                ,create_timestamp
        )  
        SELECT  t.test_id
                ,te.test_event_id
                ,s.student_id
                ,ak.test_question_id
                ,case when sm.scoring_method_code = 'c' then null else srp.student_response end as student_answer
                ,case when sm.scoring_method_code in ('s','g') and srp.student_response = ak.answer then 1  
                      when sm.scoring_method_code in ('s','g') then 0  
                      when sm.scoring_method_code = 'c' and srp.student_response REGEXP '^[0-9]' = 0 then 0  
                      when sm.scoring_method_code = 'c' and srp.student_response REGEXP '^[0-9]' != 0 then cast(srp.student_response as signed)  
                      else null end as rubric_score
                ,1234
                ,now()  
        FROM v_pmi_ods_scan_results_pivot_internal AS srp  
        JOIN sam_test as t      
                ON t.test_id = srp.test_id  
        JOIN    c_student AS s      
                ON s.student_code = srp.student_code  
        JOIN sam_test_event AS te      
                ON te.test_id = srp.test_id      
                AND te.test_event_id = srp.test_event_id  
        JOIN    sam_answer_key AS ak      
                ON ak.test_id = t.test_id      
                AND ak.flatfile_question_num = srp.flatfile_question_num  
        JOIN    sam_question_type AS qt      
                ON ak.question_type_id = qt.question_type_id   
        JOIN    sam_scoring_method AS sm      
                ON qt.scoring_method_id = sm.scoring_method_id
        LEFT JOIN  sam_student_response AS er      
                ON er.test_id = t.test_id      
                AND  er.test_event_id = te.test_event_id      
                AND  er.student_id = s.student_id      
                AND  er.test_question_id = ak.test_question_id  
        WHERE   er.student_id is null 
        or  er.gui_edit_flag <> 1  
        ON DUPLICATE KEY UPDATE 
                last_user_id = 1234                      
                ,student_answer = values(student_answer)                      
                ,rubric_score = values(rubric_score) 
        ;


        #############################################
        # insert no-responses
        #############################################

        insert  tmp_test_student (test_id,test_event_id,student_id)
        select  ts.test_id,ts.test_event_id,ts.student_id
        from    sam_test_student as ts
        join    tmp_test_list as tl
                on ts.test_id = tl.test_id
        left join sam_student_response as er
                on ts.test_id = er.test_id
                and ts.test_event_id = er.test_event_id
                and ts.student_id = er.student_id
        where er.student_id is null
        group by ts.test_id,ts.test_event_id,ts.student_id
       ;


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
        select  ak.test_id
                ,ts.test_event_id
                ,ts.student_id
                ,ak.test_question_id
                ,0
                ,0
                ,1234
                ,now()  
        from sam_answer_key as ak  
        join tmp_test_list as tmp1          
                on tmp1.test_id = ak.test_id  
        join sam_question_type as qt          
                on ak.question_type_id = qt.question_type_id  
        join sam_scoring_method as sm          
                on qt.scoring_method_id = sm.scoring_method_id          
        join tmp_test_student as ts          
                on ts.test_id = ak.test_id  
        left join sam_student_response as er          
                on er.test_id = ak.test_id          
                and er.test_event_id = ts.test_event_id          
                and er.student_id = ts.student_id          
                and er.test_question_id = ak.test_question_id  
        where er.test_id is null
        ;

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
        
        SET @sql_drop_pivot := CONCAT('TRUNCATE TABLE ',@db_name_ods,'pmi_ods_scan_results_pivot_internal');
        prepare sql_drop_pivot from @sql_drop_pivot;
            execute sql_drop_pivot;
            deallocate prepare sql_drop_pivot;


        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := '';
        set @sql_scan_log := concat('call ',@db_name_ods,'.imp_set_upload_file_status (\'','pmi_ods_scan_results_pivot_internal', '\', \'P\', \'ETL Load Successful\')');
   
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;   

        drop table if exists tmp_test_list;
        drop table if exists tmp_test_student;

    end if;
    
END;
//
