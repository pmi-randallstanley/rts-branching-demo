DROP PROCEDURE IF EXISTS sam_purge_bm_data //

CREATE definer=`dbadmin`@`localhost` procedure sam_purge_bm_data(p_retain_events_date date)
COMMENT '$Rev: 9286 $ $Date: 2010-09-28 12:26:28 -0400 (Tue, 28 Sep 2010) $'
CONTAINS SQL
SQL SECURITY INVOKER

/*
$Rev: 9286 $ 
$Author: randall.stanley $ 
$Date: 2010-09-28 12:26:28 -0400 (Tue, 28 Sep 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/sam_purge_bm_data.sql $
$Id: sam_purge_bm_data.sql 9286 2010-09-28 16:26:28Z randall.stanley $ 
*/

BEGIN
    # This proc is to be used to reset a districts Assessment results (leading data) at a point in time
    # identified by the district after the transition to a new school year.  The proc is to be called
    # directly from the build management structure (tmp.imp_auto_run) and is intended to be invoked by
    # setting the manual_run_flag on this table for a one time execution of this proc.

    declare v_drop_sr_table_flag char(1) default 'n';    

    # Get set of events to preserve. Assumption is that events created after p_retain_events_date
    # value are for current school year. Consider using c_school_year table begin/end date range for
    # future, but the date values there will need to be accurate.
    drop table if exists `tmp_retain_sam_test_events`;
    create table `tmp_retain_sam_test_events` (
      `test_id` int(11) default null,
      `test_event_id` int(11) default null,
      `last_user_id` int(10) default null,
      `create_timestamp` datetime NOT NULL default '1980-12-31 00:00:00',
      `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
      primary key (`test_id`,`test_event_id`)
    ) 
    ;

    insert  tmp_retain_sam_test_events (
        test_id
        ,test_event_id
        ,last_user_id
        ,create_timestamp
    )
    select  test_id
        ,test_event_id
        ,last_user_id
        ,create_timestamp
        
    from    sam_test_event
    where   create_timestamp > p_retain_events_date
    ;

    set @drop_sr_table_flag := pmi_f_get_etl_setting('bmSAMPurgeDropSRTable');
    if @drop_sr_table_flag is not null then
        set v_drop_sr_table_flag = @drop_sr_table_flag;
    end if;



    # Remove student response data
    # this may be done via drop/create of the table for large districts
    # based on etl setting "bmSAMPurgeDropSRTable".
    
    if v_drop_sr_table_flag = 'y' then 
        truncate table sam_student_response;
        optimize table sam_student_response;
    else
        delete  del.*
        from    sam_student_response as del
        left join   tmp_retain_sam_test_events as tmp1
                on      del.test_id = tmp1.test_id
                and     del.test_event_id = tmp1.test_event_id
        where   tmp1.test_event_id is null
        ;
    end if;


    alter table sam_student_response_au engine = MyISAM;
    truncate table sam_student_response_au;
    alter table sam_student_response_au engine = ARCHIVE;
    
    # sam_print_job_queue - local
    delete  del.*
    from    sam_print_job_queue as del
    left join   tmp_retain_sam_test_events as tmp1
            on      del.test_id = tmp1.test_id
            and     del.test_event_id = tmp1.test_event_id
    where   del.test_event_id is not null
    and     tmp1.test_event_id is null
    ;

    # sam_form_print_list
    delete  del.*
    from    sam_form_print_list as del
    left join   tmp_retain_sam_test_events as tmp1
            on      del.test_id = tmp1.test_id
            and     del.test_event_id = tmp1.test_event_id
    where   tmp1.test_event_id is null
    ;
    
    # sam_test_event_sheet_sort_list
    delete  del.*
    from    sam_test_event_sheet_sort_list as del
    left join   tmp_retain_sam_test_events as tmp1
            on      del.test_id = tmp1.test_id
            and     del.test_event_id = tmp1.test_event_id
    where   tmp1.test_event_id is null
    ;
    
    # sam_test_event_school_list
    delete  del.*
    from    sam_test_event_school_list as del
    left join   tmp_retain_sam_test_events as tmp1
            on      del.test_id = tmp1.test_id
            and     del.test_event_id = tmp1.test_event_id
    where   tmp1.test_event_id is null
    ;
    
    # sam_test_event_schedule_list
    delete  del.*
    from    sam_test_event_schedule_list as del
    left join   tmp_retain_sam_test_events as tmp1
            on      del.test_id = tmp1.test_id
            and     del.test_event_id = tmp1.test_event_id
    where   tmp1.test_event_id is null
    ;
    
    # sam_test_student
    delete  del.*
    from    sam_test_student as del
    left join   tmp_retain_sam_test_events as tmp1
            on      del.test_id = tmp1.test_id
            and     del.test_event_id = tmp1.test_event_id
    where   tmp1.test_event_id is null
    ;

    # sam_test_event_ola
    delete  del.*
    from    sam_test_event_ola as del
    left join   tmp_retain_sam_test_events as tmp1
            on      del.test_id = tmp1.test_id
            and     del.test_event_id = tmp1.test_event_id
    where   tmp1.test_event_id is null
    ;
    
    # sam_test_event
    delete  del.*
    from    sam_test_event as del
    left join   tmp_retain_sam_test_events as tmp1
            on      del.test_id = tmp1.test_id
            and     del.test_event_id = tmp1.test_event_id
    where   tmp1.test_event_id is null
    ;
    
    # baseball card tables
    flush table rpt_baseball_detail_assessment;
    flush table rpt_baseball_detail_leading;
    flush table rpt_baseball_detail;
    truncate table rpt_baseball_detail_assessment;
    truncate table rpt_baseball_detail_leading;
    
    # cleanup utility table used in purge process
    drop table if exists `tmp_retain_sam_test_events`;

END;
//
