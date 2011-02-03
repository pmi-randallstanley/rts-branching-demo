/*
$Rev: 7380 $ 
$Author: randall.stanley $ 
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_edusoft_align.sql $
$Id: etl_bm_load_edusoft_align.sql 7380 2009-07-16 14:23:58Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_bm_load_edusoft_align //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_edusoft_align()
CONTAINS SQL
COMMENT '$Rev: 7380 $ $Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $'
SQL SECURITY INVOKER

BEGIN 


    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_sr_ak_bm_align_edusoft';


    drop table if exists tmp_al_list;
     
    if @table_exists > 0 then
        
        Create table tmp_al_list
         SELECT DISTINCT  bmdu.test_id as rawID, bmdu.question, bmdu.edusoft_benchmark_id, pmc.pmi_curriculum_code,
                sc.curriculum_id, st.test_id  AS pmi_test_id, sak.test_question_id
         FROM    v_pmi_ods_sr_ak_bm_align_edusoft AS bmdu
         JOIN    v_pmi_ods_edusoft_curr_xref AS pmc
                ON bmdu.edusoft_benchmark_id = pmc.edusoft_benchmark_id
         JOIN   sam_curriculum AS sc
                ON sc.curriculum_code = pmc.pmi_curriculum_code
         JOIN   sam_test AS st
                ON st.import_xref_code = bmdu.test_id
         JOIN   sam_answer_key AS sak
                ON sak.test_id = st.test_id
                and sak.flatfile_question_num = bmdu.question;
       

       
       
      INSERT INTO sam_alignment_list (test_id, test_question_id, curriculum_id, primary_flag, last_user_id, create_timestamp)
      SELECT  DISTINCT pmi_test_id, test_question_id, curriculum_id, 0, 1234, current_timestamp
      FROM   tmp_al_list
      ON DUPLICATE KEY UPDATE last_user_id = 1234;
      
      
        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_sr_ak_bm_align_edusoft', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;

    end if;

END;

//
