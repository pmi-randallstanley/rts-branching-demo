/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_test_scores.sql $
$Id: etl_rpt_test_scores.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_rpt_test_scores //

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_test_scores`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
BEGIN
    

    declare no_more_rows boolean; 
    declare v_test_id int;

    declare cur_1 cursor for 
        select  test_id 
        from    tmp_tests
        ;
    
    declare continue handler for not found 
    set no_more_rows = true;


    drop table if exists `tmp_tests`;
    drop table if exists `tmp_rpt_test_scores`;

    create table `tmp_tests` (
      `test_id` int(11) not null,
      `last_edit_timestamp` timestamp not null default current_timestamp on update current_timestamp,
      primary key  (`test_id`)
    ) engine=innodb default charset=latin1
    ;

    create table `tmp_rpt_test_scores` (
      `test_id` int(11) not null,
      `test_event_id` int(11) not null,
      `student_id` int(11) not null,
      `test_question_id` int(11) not null,
      `rubric_total` decimal(6,3) default null,
      `moniker` varchar(255) not null,
      `test_date` date default null,
      `course_type_id` int(11) default null,
      `include_wavg_calc_flag` tinyint(1) not null default '1',
      PRIMARY KEY  (`test_id`,`test_event_id`,`student_id`,`test_question_id`)
    ) engine=innodb default charset=latin1
    ;

    truncate TABLE rpt_test_scores;

    insert tmp_tests ( test_id)
    select  test_id
    from    sam_student_response
    group by test_id
    ;


    OPEN cur_1;

    loop_cur_1: LOOP
            
        fetch  cur_1 
        into   v_test_id;

        if no_more_rows then
            close cur_1;
            leave loop_cur_1;
        end if;

        truncate table tmp_rpt_test_scores;
        
        insert tmp_rpt_test_scores (
            test_id
            ,test_event_id
            ,student_id
            ,test_question_id
            ,rubric_total
            ,moniker
            ,test_date
            ,course_type_id
            ,include_wavg_calc_flag
        )
        
        select  t.test_id
            ,te.test_event_id
            ,ts.student_id
            ,ak.test_question_id
            ,r.rubric_total
            ,t.moniker
            ,te.start_date as test_date
            ,t.course_type_id
            ,te.include_wavg_calc_flag
        
        from    sam_test t
        join    sam_test_event as te
                on    te.test_id = t.test_id
        join    sam_test_student as ts
                on    ts.test_id = te.test_id
                and   ts.test_event_id = te.test_event_id
        join    sam_answer_key ak
                on    ak.test_id = t.test_id         
        join    sam_rubric r
                on    r.rubric_id = ak.rubric_id
        where   t.test_id = v_test_id
        and     t.purge_flag = 0
        and     te.purge_flag = 0
        and     r.rubric_total != 0
        ;

        insert into rpt_test_scores (
            test_id
            , test_event_id
            , student_id
            , test_name
            , test_date
            , course_type_id
            , points_earned
            , points_possible
            , include_wavg_flag
            , last_user_id
            ) 
        
        select   er.test_id
           ,er.test_event_id
           ,er.student_id
           ,min(tmp1.moniker)
           ,min(tmp1.test_date)
           ,min(tmp1.course_type_id)
           ,sum(er.rubric_value)
           ,sum(tmp1.rubric_total)
           ,min(tmp1.include_wavg_calc_flag)
           ,1234
        
        from    tmp_rpt_test_scores as tmp1
        join    sam_student_response er
                on      er.test_id = tmp1.test_id
                and     er.test_event_id = tmp1.test_event_id
                and     er.student_id = tmp1.student_id
                and     er.test_question_id = tmp1.test_question_id
        join    c_student as st
                on      st.student_id = er.student_id
                and     st.active_flag = 1
        where   er.rubric_value is not null
        group by er.test_id, er.test_event_id, er.student_id
        ;


    end loop loop_cur_1;

    optimize table rpt_test_scores;
    
    drop table if exists `tmp_tests`;
    drop table if exists `tmp_rpt_test_scores`;

END;
//
