/*
$Rev$ 
$Author$
$Date$
$HeadURL$
$Id$
*/

drop procedure if exists etl_bm_load_intel_ak //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_intel_ak()
contains sql
sql security invoker
comment '$Rev$ $Date$'

begin
    declare v_ods_table                  varchar(64);
    declare v_ods_view                   varchar(64);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_intel_assess_ak';
    set v_ods_view = concat('v_', v_ods_table);

    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name IN ('v_pmi_ods_intel_assess_ak'
                            ,'v_pmi_ods_intel_assess_test')
    ;

    if @table_exists >= 2 then

        set @ndx = 0;
        set @last_test_id = 9999999999;

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
              ,IF(is_passage = 1, NULL, passage_id)
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
              join v_pmi_ods_intel_assess_ak oak
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
              join v_pmi_ods_intel_assess_ak oak
                on tt.external_test_id = oak.external_test_id
              join ca_ib.question q
                on q.import_xref_code = oak.external_question_id
              where q.passage_id IS NOT NULL
              group by q.passage_id) tmp
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
              ,oak.section_num
              ,count(*) as question_count
              ,1
              ,0
              ,1234
              ,NOW()
        from tmp_test tt
        join v_pmi_ods_intel_assess_ak oak
          on tt.external_test_id = oak.external_test_id
        group by tt.test_id
        ;

        set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', v_ods_table, '\', \'P\', \'ETL Load Successful\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;

    end if;

    drop table if exists `tmp_test`;
end;
//
