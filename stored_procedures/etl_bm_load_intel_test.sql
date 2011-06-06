/*
$Rev$
$Author$
$Date$
$HeadURL$
$Id$
*/

drop procedure if exists etl_bm_load_intel_test //

create definer=`dbadmin`@`localhost` procedure etl_bm_load_intel_test()
contains sql
sql security invoker
comment '$Rev$ $Date$'

begin
    declare v_ods_table                  varchar(64);
    declare v_ods_view                   varchar(64);

    declare v_course_type_id             int;
    declare v_external_answer_source_id  int;

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_intel_assess_test';
    set v_ods_view = concat('v_', v_ods_table);

    select  count(*) 
    into    @table_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name IN ('v_pmi_ods_intel_assess_test')
    ;

    drop table if exists `tmp_test`;

    if @table_exists > 0 then

        select  course_type_id
        into    v_course_type_id
        from    c_course_type
        where   moniker = 'Other'
        ;

        select  external_answer_source_id
        into    v_external_answer_source_id
        from    sam_external_answer_source
        where   moniker = 'Other'
        ;

        create table `tmp_test` (
          `test_id` int(11) NOT NULL,
          `external_test_id` varchar(50) DEFAULT NULL,
          `test_name` varchar(100) DEFAULT NULL,
          primary key (`test_id`),
          key (`external_test_id`)
        ) engine=innodb default charset=latin1
        ;

        insert into tmp_test (
            test_id
           ,external_test_id
           ,test_name
        )
        select pmi_f_get_next_sequence_app_db('sam_test', 1)
              ,ot.external_test_id
              ,ot.test_name
        from v_pmi_ods_intel_assess_test ot
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
              ,tt.external_test_id
              ,tt.test_name
              ,'e'
              ,'i'
              ,'intel assess'
              ,v_external_answer_source_id
              ,70
              ,50
              ,v_course_type_id
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

        set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', v_ods_table, '\', \'P\', \'ETL Load Successful\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;

    end if;

end;
//
