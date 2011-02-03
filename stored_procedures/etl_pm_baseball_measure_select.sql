DROP PROCEDURE IF EXISTS etl_pm_baseball_measure_select //

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_baseball_measure_select()
COMMENT '$Rev: 9640 $ $Date: 2010-11-05 22:27:26 -0400 (Fri, 05 Nov 2010) $'
CONTAINS SQL
SQL SECURITY INVOKER

/*
$Rev: 9640 $ 
$Author: randall.stanley $ 
$Date: 2010-11-05 22:27:26 -0400 (Fri, 05 Nov 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_baseball_measure_select.sql $
$Id: etl_pm_baseball_measure_select.sql 9640 2010-11-06 02:27:26Z randall.stanley $ 
*/

BEGIN

    drop table if exists `tmp_bb_stu_measure`;
    create table `tmp_bb_stu_measure` (
      `student_id` int(10) not null,
      `bb_group_id` int(10) not null,
      `bb_measure_id` int(10) not null,
      `bb_measure_item_id` int(10) not null,
      primary key (`student_id`,`bb_group_id`, `bb_measure_id`, `bb_measure_item_id`)
    ) engine=innodb default charset=latin1;

    drop table if exists `tmp_dyn_meas_with_scores`;
    create table `tmp_dyn_meas_with_scores` (
        bb_group_id int(11) not null,
        bb_measure_id int(11) not null,
        primary key  (`bb_group_id`,`bb_measure_id`)
    );

    select  bb_group_id
    into    @bbg_lead_id
    from    pm_baseball_group
    where   bb_group_code = 'lead'
    ;

    #################################################
    # Update leading monikers so they match
    #  sam_test_admin_period.report_column_text
    #################################################

    UPDATE  pm_baseball_measure AS bbm
    JOIN    sam_test_admin_period AS tap
            ON      tap.admin_code = bbm.bb_measure_code
    LEFT JOIN   sam_test_admin_period_client as tapc
            ON      tap.admin_period_id = tapc.admin_period_id
    SET     bbm.moniker = coalesce(tapc.report_column_text, tap.report_column_text)
    WHERE   bbm.bb_group_id = @bbg_lead_id
    ;

    # Truncate filtering tables #
    
    truncate table pm_baseball_measure_select;
    truncate table pm_baseball_measure_select_teacher;
    

    ##################
    # Measures Maint #
    ##################
    # Get list of "dynamic" measures with linked scores
    insert tmp_dyn_meas_with_scores (
        bb_group_id
        ,bb_measure_id
    )

    select  bm.bb_group_id
        ,bm.bb_measure_id            
    from    pm_baseball_measure as bm
    where   bm.dynamic_creation_flag = 1
    and exists (    select  *
                    from    rpt_baseball_detail as bd
                    where   bm.bb_group_id = bd.bb_group_id
                    and     bm.bb_measure_id = bd.bb_measure_id
                )
    ;

    # Remove "dynamic" measures that have no assoc scores
    delete  del.*
    from    pm_baseball_measure as del
    left join   tmp_dyn_meas_with_scores as tmp1
            on      tmp1.bb_group_id = del.bb_group_id
            and     tmp1.bb_measure_id = del.bb_measure_id
    where   del.dynamic_creation_flag = 1
    and     tmp1.bb_measure_id is null
    ;
    
 
    #########################
    # Load Filtering Tables #
    #########################
    insert tmp_bb_stu_measure ( student_id, bb_group_id, bb_measure_id, bb_measure_item_id)
    select  student_id, bb_group_id, bb_measure_id, bb_measure_item_id
    from    rpt_baseball_detail
    group by student_id, bb_group_id, bb_measure_id, bb_measure_item_id
    ;
    
    # BB Card by Grade filter table
    insert pm_baseball_measure_select ( grade_level_id, school_id, bb_group_id, bb_measure_id, bb_measure_item_id, last_user_id, create_timestamp)
    select  sty.grade_level_id, sty.school_id, tmp1.bb_group_id, tmp1.bb_measure_id, tmp1.bb_measure_item_id, 1234, now()
    from    c_student st
    join    c_school_year as sy
            on      sy.active_flag = 1
    join    c_student_year sty
            on      sty.student_id = st.student_id
            and     sty.active_flag = 1
    join    tmp_bb_stu_measure as tmp1
            on      st.student_id = tmp1.student_id
    where    st.active_flag = 1 
    group by sty.grade_level_id, sty.school_id, tmp1.bb_group_id, tmp1.bb_measure_id, tmp1.bb_measure_item_id
    ;

    # BB Card by Teacher filter table
    insert pm_baseball_measure_select_teacher ( school_id, user_id, bb_group_id, bb_measure_id, bb_measure_item_id, last_user_id, create_timestamp)
    select  cl.school_id, cl.user_id, tmp1.bb_group_id, tmp1.bb_measure_id, tmp1.bb_measure_item_id, 1234, now()
    from    c_student st
    join    tmp_bb_stu_measure as tmp1
            on      st.student_id = tmp1.student_id
    join    c_class_enrollment as cle
            on      st.student_id = cle.student_id
    join    c_class as cl
            on      cle.class_id = cl.class_id
    where    st.active_flag = 1 
    group by cl.school_id, cl.user_id, tmp1.bb_group_id, tmp1.bb_measure_id, tmp1.bb_measure_item_id
    ;
    
    drop table if exists `tmp_bb_stu_measure`;
    drop table if exists `tmp_dyn_meas_with_scores`;
     
END;
//
