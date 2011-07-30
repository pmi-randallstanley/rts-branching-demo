/*
$Rev: 7981 $ 
$Author: randall.stanley $ 
$Date: 2009-12-09 09:47:32 -0500 (Wed, 09 Dec 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_load_pmi_pend_ola_tables.sql $
$Id: etl_bm_load_pmi_pend_ola_tables.sql 7981 2009-12-09 14:47:32Z randall.stanley $ 
 */

drop procedure if exists etl_bm_load_pmi_pend_ola_tables//

create definer=`dbadmin`@`localhost` procedure etl_bm_load_pmi_pend_ola_tables()
contains sql
sql security invoker
comment '$Rev: 7981 $ $Date: 2009-12-09 09:47:32 -0500 (Wed, 09 Dec 2009) $'

# dev1 172.31.1.33
# ec2prod 172.31.1.57

proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set @sql_pend_test_header_drop := concat(' drop table if exists  `', @db_name_core,'`.`pend_test_header`');
    set @sql_pend_test_header := concat(' CREATE TABLE `', @db_name_core,'`.`pend_test_header` ( '
                                ,'   `pend_header_id` int(11) NOT NULL, '
                                ,'   `test_name` varchar(50) NOT NULL, '
                                ,'   `test_platform_code` enum(\'o\',\'l\',\'t\') NOT NULL, '
                                ,'   `pmi_test_id` int(11) default NULL, '
                                ,'   `pmi_test_event_id` int(11) default NULL, '
                                ,'   `pmi_test_event_start_date` date default NULL, '
                                ,'   `pmi_test_event_end_date` date default NULL, '
                                ,'   `pmi_owner_id` int(11) default NULL, '
                                ,'   `create_user_id` int(11) NOT NULL, '
                                ,'   `last_user_id` int(11) NOT NULL, '
                                ,'   `create_timestamp` datetime NOT NULL default \'1980-12-31 00:00:00\', '
                                ,'   `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP, '
                                ,'   PRIMARY KEY  (`pend_header_id`) '
                                ,' ) ENGINE=FEDERATED DEFAULT CHARSET=latin1 '
                                ,' CONNECTION=\'mysql://PMIRemoteUser:pmi@172.31.1.57/', @db_name_pend, '/pend_test_header\' ' )
    ;


    prepare sql_pend_test_header_drop from @sql_pend_test_header_drop;
    execute sql_pend_test_header_drop;
    deallocate prepare sql_pend_test_header_drop;

    prepare sql_pend_test_header from @sql_pend_test_header;
    execute sql_pend_test_header;
    deallocate prepare sql_pend_test_header;

    set @sql_pend_test_event_student_drop := concat(' drop table if exists  `', @db_name_core,'`.`pend_test_event_student`');
    set @sql_pend_test_event_student := concat(' CREATE TABLE `', @db_name_core,'`.`pend_test_event_student` ( '
                                ,'   `pend_header_id` int(11) NOT NULL, '
                                ,'   `pend_test_event_item_id` int(11) NOT NULL, '
                                ,'   `sis_student_code` varchar(15) NOT NULL, '
                                ,'   `pmi_test_id` int(11) default NULL, '
                                ,'   `pmi_test_event_id` int(11) default NULL, '
                                ,'   `pmi_student_id` int(11) default NULL, '
                                ,'   `ola_status_code` enum(\'n\',\'i\',\'p\',\'f\') NOT NULL default \'n\', '
                                ,'   `ola_orig_time_allot_secs` smallint(6) default NULL, '
                                ,'   `ola_elapsed_time_secs` smallint(6) default NULL, '
                                ,'   `etl_batch_id` int(11) default NULL, '
                                ,'   `create_user_id` int(11) NOT NULL, '
                                ,'   `last_user_id` int(11) NOT NULL, '
                                ,'   `create_timestamp` datetime NOT NULL default \'1980-12-31 00:00:00\', '
                                ,'   `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP, '
                                ,'   PRIMARY KEY  (`pend_header_id`,`pend_test_event_item_id`) '
                                ,' ) ENGINE=FEDERATED DEFAULT CHARSET=latin1 '
                                ,' CONNECTION=\'mysql://PMIRemoteUser:pmi@172.31.1.57/', @db_name_pend, '/pend_test_event_student\' ')
    ;

    prepare sql_pend_test_event_student_drop from @sql_pend_test_event_student_drop;
    execute sql_pend_test_event_student_drop;
    deallocate prepare sql_pend_test_event_student_drop;

    prepare sql_pend_test_event_student from @sql_pend_test_event_student;
    execute sql_pend_test_event_student;
    deallocate prepare sql_pend_test_event_student;

    set @sql_pend_student_response_drop := concat(' drop table if exists  `', @db_name_core,'`.`pend_student_response`');
    set @sql_pend_student_response := concat(' CREATE TABLE `', @db_name_core,'`.`pend_student_response` ( '
                                ,'   `pend_header_id` int(11) NOT NULL, '
                                ,'   `pend_test_event_item_id` int(11) NOT NULL, '
                                ,'   `section_num` tinyint(4) NOT NULL, '
                                ,'   `question_num` smallint(6) NOT NULL, '
                                ,'   `sis_student_code` varchar(15) NOT NULL, '
                                ,'   `ola_display_ordinal` smallint(6) default NULL, '
                                ,'   `student_response` varchar(10) default NULL, '
                                ,'   `correct_answer` varchar(10) default NULL, '
                                ,'   `question_type_code` varchar(5) NOT NULL default \'sr\', '
                                ,'   `rubric_score` tinyint(4) default NULL, '
                                ,'   `rubric_value` decimal(6,3) default NULL, '
                                ,'   `rubric_total` decimal(6,3) default NULL, '
                                ,'   `curriculum_code` varchar(50) default NULL, '
                                ,'   `student_last_name` varchar(30) NOT NULL, '
                                ,'   `student_first_name` varchar(30) NOT NULL, '
                                ,'   `ola_elapsed_time_secs` smallint(6) default NULL, '
                                ,'   `pmi_test_id` int(11) default NULL, '
                                ,'   `pmi_test_event_id` int(11) default NULL, '
                                ,'   `pmi_student_id` int(11) default NULL, '
                                ,'   `pmi_test_question_id` int(11) default NULL, '
                                ,'   `client_id` int(11) NOT NULL, '
                                ,'   `etl_batch_id` int(11) default NULL, '
                                ,'   `create_user_id` int(11) NOT NULL, '
                                ,'   `last_user_id` int(11) NOT NULL, '
                                ,'   `create_timestamp` datetime NOT NULL default \'1980-12-31 00:00:00\', '
                                ,'   `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP, '
                                ,'   PRIMARY KEY  (`pend_header_id`,`pend_test_event_item_id`,`section_num`,`question_num`) '
                                ,' ) ENGINE=FEDERATED DEFAULT CHARSET=latin1 '
                                ,' CONNECTION=\'mysql://PMIRemoteUser:pmi@172.31.1.57/', @db_name_pend, '/pend_student_response\' ')
    ;

    prepare sql_pend_student_response_drop from @sql_pend_student_response_drop;
    execute sql_pend_student_response_drop;
    deallocate prepare sql_pend_student_response_drop;

    prepare sql_pend_student_response from @sql_pend_student_response;
    execute sql_pend_student_response;
    deallocate prepare sql_pend_student_response;

    set @sql_pend_test_header_hist_drop := concat(' drop table if exists  `', @db_name_core,'`.`pend_test_header_hist`');
    set @sql_pend_test_header_hist := concat(' CREATE TABLE `', @db_name_core,'`.`pend_test_header_hist` ( '
                                ,'   `pend_header_id` int(11) NOT NULL, '
                                ,'   `test_name` varchar(50) NOT NULL, '
                                ,'   `test_platform_code` enum(\'o\',\'l\',\'t\') NOT NULL, '
                                ,'   `pmi_test_id` int(11) default NULL, '
                                ,'   `pmi_test_event_id` int(11) default NULL, '
                                ,'   `pmi_test_event_start_date` date default NULL, '
                                ,'   `pmi_test_event_end_date` date default NULL, '
                                ,'   `pmi_owner_id` int(11) default NULL, '
                                ,'   `create_user_id` int(11) NOT NULL, '
                                ,'   `last_user_id` int(11) NOT NULL, '
                                ,'   `create_timestamp` datetime NOT NULL default \'1980-12-31 00:00:00\', '
                                ,'   `last_edit_timestamp` datetime NOT NULL default \'1980-12-31 00:00:00\', '
                                ,'   `history_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP, '
                                ,'   PRIMARY KEY  (`pend_header_id`) '
                                ,' ) ENGINE=FEDERATED DEFAULT CHARSET=latin1 '
                                ,' CONNECTION=\'mysql://PMIRemoteUser:pmi@172.31.1.57/', @db_name_pend, '/pend_test_header_hist\' ' )
    ;


    prepare sql_pend_test_header_hist_drop from @sql_pend_test_header_hist_drop;
    execute sql_pend_test_header_hist_drop;
    deallocate prepare sql_pend_test_header_hist_drop;

    prepare sql_pend_test_header_hist from @sql_pend_test_header_hist;
    execute sql_pend_test_header_hist;
    deallocate prepare sql_pend_test_header_hist;


    set @sql_pend_test_event_student_hist_drop := concat(' drop table if exists  `', @db_name_core,'`.`pend_test_event_student_hist`');
    set @sql_pend_test_event_student_hist := concat(' CREATE TABLE `', @db_name_core,'`.`pend_test_event_student_hist` ( '
                                ,'   `pend_header_id` int(11) NOT NULL, '
                                ,'   `pend_test_event_item_id` int(11) NOT NULL, '
                                ,'   `sis_student_code` varchar(15) NOT NULL, '
                                ,'   `pmi_test_id` int(11) default NULL, '
                                ,'   `pmi_test_event_id` int(11) default NULL, '
                                ,'   `pmi_student_id` int(11) default NULL, '
                                ,'   `ola_status_code` enum(\'n\',\'i\',\'p\',\'f\') NOT NULL default \'n\', '
                                ,'   `ola_orig_time_allot_secs` smallint(6) default NULL, '
                                ,'   `ola_elapsed_time_secs` smallint(6) default NULL, '
                                ,'   `etl_batch_id` int(11) default NULL, '
                                ,'   `create_user_id` int(11) NOT NULL, '
                                ,'   `last_user_id` int(11) NOT NULL, '
                                ,'   `create_timestamp` datetime NOT NULL default \'1980-12-31 00:00:00\', '
                                ,'   `last_edit_timestamp` datetime NOT NULL default \'1980-12-31 00:00:00\', '
                                ,'   `history_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP, '
                                ,'   PRIMARY KEY  (`pend_header_id`,`pend_test_event_item_id`) '
                                ,' ) ENGINE=FEDERATED DEFAULT CHARSET=latin1 '
                                ,' CONNECTION=\'mysql://PMIRemoteUser:pmi@172.31.1.57/', @db_name_pend, '/pend_test_event_student_hist\' ')
    ;

    prepare sql_pend_test_event_student_hist_drop from @sql_pend_test_event_student_hist_drop;
    execute sql_pend_test_event_student_hist_drop;
    deallocate prepare sql_pend_test_event_student_hist_drop;

    prepare sql_pend_test_event_student_hist from @sql_pend_test_event_student_hist;
    execute sql_pend_test_event_student_hist;
    deallocate prepare sql_pend_test_event_student_hist;

    set @sql_pend_student_response_hist_drop := concat(' drop table if exists  `', @db_name_core,'`.`pend_student_response_hist`');
    set @sql_pend_student_response_hist := concat(' CREATE TABLE `', @db_name_core,'`.`pend_student_response_hist` ( '
                                ,'   `pend_header_id` int(11) NOT NULL, '
                                ,'   `pend_test_event_item_id` int(11) NOT NULL, '
                                ,'   `section_num` tinyint(4) NOT NULL, '
                                ,'   `question_num` smallint(6) NOT NULL, '
                                ,'   `sis_student_code` varchar(15) NOT NULL, '
                                ,'   `ola_display_ordinal` smallint(6) default NULL, '
                                ,'   `student_response` varchar(10) default NULL, '
                                ,'   `correct_answer` varchar(10) default NULL, '
                                ,'   `question_type_code` varchar(5) NOT NULL default \'sr\', '
                                ,'   `rubric_score` tinyint(4) default NULL, '
                                ,'   `rubric_value` decimal(6,3) default NULL, '
                                ,'   `rubric_total` decimal(6,3) default NULL, '
                                ,'   `curriculum_code` varchar(50) default NULL, '
                                ,'   `student_last_name` varchar(30) NOT NULL, '
                                ,'   `student_first_name` varchar(30) NOT NULL, '
                                ,'   `ola_elapsed_time_secs` smallint(6) default NULL, '
                                ,'   `pmi_test_id` int(11) default NULL, '
                                ,'   `pmi_test_event_id` int(11) default NULL, '
                                ,'   `pmi_student_id` int(11) default NULL, '
                                ,'   `pmi_test_question_id` int(11) default NULL, '
                                ,'   `client_id` int(11) NOT NULL, '
                                ,'   `etl_batch_id` int(11) default NULL, '
                                ,'   `create_user_id` int(11) NOT NULL, '
                                ,'   `last_user_id` int(11) NOT NULL, '
                                ,'   `create_timestamp` datetime NOT NULL default \'1980-12-31 00:00:00\', '
                                ,'   `last_edit_timestamp` datetime NOT NULL default \'1980-12-31 00:00:00\', '
                                ,'   `history_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP, '
                                ,'   PRIMARY KEY  (`pend_header_id`,`pend_test_event_item_id`,`section_num`,`question_num`) '
                                ,' ) ENGINE=FEDERATED DEFAULT CHARSET=latin1 '
                                ,' CONNECTION=\'mysql://PMIRemoteUser:pmi@172.31.1.57/', @db_name_pend, '/pend_student_response_hist\' ')
    ;

    prepare sql_pend_student_response_hist_drop from @sql_pend_student_response_hist_drop;
    execute sql_pend_student_response_hist_drop;
    deallocate prepare sql_pend_student_response_hist_drop;

    prepare sql_pend_student_response_hist from @sql_pend_student_response_hist;
    execute sql_pend_student_response_hist;
    deallocate prepare sql_pend_student_response_hist;


end proc;
//
