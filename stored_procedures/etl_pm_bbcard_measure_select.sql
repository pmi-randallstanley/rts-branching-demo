DROP PROCEDURE IF EXISTS etl_pm_bbcard_measure_select //

CREATE definer=`dbadmin`@`localhost` PROCEDURE etl_pm_bbcard_measure_select()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: $ $Date: $'

PROC: BEGIN 

    drop table if exists `tmp_bb_stu_measure`;
    drop table if exists `tmp_dyn_meas_with_scores`;

    create table `tmp_bb_stu_measure` (
      `student_id` int(10) not null,
      `bb_group_id` int(10) not null,
      `bb_measure_id` int(10) not null,
      `bb_measure_item_id` int(10) not null,
      `school_year_id` int(4) not null,
      primary key (`student_id`,`bb_group_id`, `bb_measure_id`, `bb_measure_item_id`, `school_year_id`),
      key `ind_tmp_bb_stu_measure` (`bb_group_id`, `bb_measure_id`, `bb_measure_item_id`, `school_year_id`)
    ) engine=innodb default charset=latin1;

    create table `tmp_dyn_meas_with_scores` (
        bb_group_id int(11) not null,
        bb_measure_id int(11) not null,
        primary key  (`bb_group_id`,`bb_measure_id`)
    );

    #################################################
    # Update leading monikers so they match
    #  sam_test_admin_period.report_column_text
    #################################################

    update  pm_bbcard_measure_item as bbmi
    join    pm_bbcard_group as bg
            on      bg.bb_group_id = bbmi.bb_group_id
            and     bg.bb_group_code in ('assessments','lagLeadStrand','lagLeadSubject')
    join    sam_test_admin_period as tap
            on      tap.admin_code = bbmi.bb_measure_item_code
    left join   sam_test_admin_period_client as tapc
            on      tap.admin_period_id = tapc.admin_period_id
    set     bbmi.moniker = coalesce(tapc.report_column_text, tap.report_column_text)
    ;

    # Truncate filtering tables #
    
    truncate table pm_bbcard_measure_select;
    truncate table pm_bbcard_measure_select_teacher;
    

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
    from    pm_bbcard_measure as bm
    where   bm.dynamic_creation_flag = 1
    and exists (    select  *
                    from    rpt_bbcard_detail as bd
                    where   bm.bb_group_id = bd.bb_group_id
                    and     bm.bb_measure_id = bd.bb_measure_id
                )
    ;

    # Remove "dynamic" measures that have no assoc scores
    delete  del.*
    from    pm_bbcard_measure_item as del
    left join   tmp_dyn_meas_with_scores as tmp1
            on      tmp1.bb_group_id = del.bb_group_id
            and     tmp1.bb_measure_id = del.bb_measure_id
    where   del.dynamic_creation_flag = 1
    and     tmp1.bb_measure_id is null
    ;

    delete  del.*
    from    pm_bbcard_measure as del
    left join   tmp_dyn_meas_with_scores as tmp1
            on      tmp1.bb_group_id = del.bb_group_id
            and     tmp1.bb_measure_id = del.bb_measure_id
    where   del.dynamic_creation_flag = 1
    and     tmp1.bb_measure_id is null
    ;
    
 
    #########################
    # Load Filtering Tables #
    #########################
    insert tmp_bb_stu_measure ( student_id, bb_group_id, bb_measure_id, bb_measure_item_id, school_year_id)
    select  student_id, bb_group_id, bb_measure_id, bb_measure_item_id, school_year_id
    from    rpt_bbcard_detail
    group by student_id, bb_group_id, bb_measure_id, bb_measure_item_id, school_year_id
    ;
    
    # BB Card by Grade filter table
    ### This currenlty puts in data by school of enrollment, will add query below to put in data by school of instructino as well.
    insert pm_bbcard_measure_select ( grade_level_id, school_id, bb_group_id, bb_measure_id, bb_measure_item_id, school_year_id, last_user_id, create_timestamp)
    select  sty.grade_level_id, sty.school_id, tmp1.bb_group_id, tmp1.bb_measure_id, tmp1.bb_measure_item_id, tmp1.school_year_id, 1234, now()
    from    c_student st
    join    c_school_year as sy
            on      sy.active_flag = 1
    join    c_student_year sty
            on      sty.student_id = st.student_id
            and     sty.active_flag = 1
    join    tmp_bb_stu_measure as tmp1
            on      st.student_id = tmp1.student_id
    where    st.active_flag = 1 
    group by sty.grade_level_id, sty.school_id, tmp1.bb_group_id, tmp1.bb_measure_id, tmp1.bb_measure_item_id, tmp1.school_year_id
    ;
    
    ###  Here is the fix to add measures based on school of instruction.  Using on duplicate key, so it will only add the net new
    insert pm_bbcard_measure_select ( grade_level_id, school_id, bb_group_id, bb_measure_id, bb_measure_item_id, school_year_id, last_user_id, create_timestamp)
    select  sty.grade_level_id, cl.school_id, tmp1.bb_group_id, tmp1.bb_measure_id, tmp1.bb_measure_item_id, tmp1.school_year_id, 1234, now()
    from    c_student st
    join    c_school_year as sy
            on      sy.active_flag = 1
    join    c_student_year sty
            on      sty.student_id = st.student_id
            and     sty.active_flag = 1
    join    c_class_enrollment ce
            on      st.student_id = ce.student_id
    join    c_class cl
            on      ce.class_id = cl.class_id
    join    tmp_bb_stu_measure as tmp1
            on      st.student_id = tmp1.student_id
    group by sty.grade_level_id, cl.school_id, tmp1.bb_group_id, tmp1.bb_measure_id, tmp1.bb_measure_item_id, tmp1.school_year_id
    on duplicate key update last_user_id = 1234
    ;

    # BB Card by Teacher filter table
#    insert pm_bbcard_measure_select_teacher ( school_id, user_id, bb_group_id, bb_measure_id, bb_measure_item_id, school_year_id, last_user_id, create_timestamp)
#    select  cl.school_id, cl.user_id, tmp1.bb_group_id, tmp1.bb_measure_id, tmp1.bb_measure_item_id, tmp1.school_year_id, 1234, now()
#    from    c_student st
#    join    tmp_bb_stu_measure as tmp1
#            on      st.student_id = tmp1.student_id
#    join    c_class_enrollment as cle
#            on      st.student_id = cle.student_id
#    join    c_class as cl
#            on      cle.class_id = cl.class_id
#    where    st.active_flag = 1 
#    group by cl.school_id, cl.user_id, tmp1.bb_group_id, tmp1.bb_measure_id, tmp1.bb_measure_item_id, tmp1.school_year_id
#    ;
    
    drop table if exists `tmp_bb_stu_measure`;
    drop table if exists `tmp_dyn_meas_with_scores`;
     
END PROC;
//
