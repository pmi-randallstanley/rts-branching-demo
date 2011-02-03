/*
$Rev: 7410 $ 
$Author: randall.stanley $ 
$Date: 2009-07-20 11:04:59 -0400 (Mon, 20 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_ayp_teacher_performance.sql $
$Id: etl_rpt_ayp_teacher_performance.sql 7410 2009-07-20 15:04:59Z randall.stanley $ 
 */

drop procedure if exists etl_rpt_ayp_teacher_performance//

create definer=`dbadmin`@`localhost` procedure etl_rpt_ayp_teacher_performance()
contains sql
sql security invoker
comment '$Rev: 7410 $ $Date: 2009-07-20 11:04:59 -0400 (Mon, 20 Jul 2009) $'


proc: begin 

    declare no_more_rows            boolean; 
    declare v_ayp_subject_id        int(11) default '0';
    declare v_max_test_yr_id        int(11) default '0';
    declare v_test_each_year_flag   tinyint(1) default '0';
    declare v_alt_score_flag        tinyint(1) default '0';
    declare v_prev_yr_id            int(11) default '0';
    declare v_curr_yr_id            int(11) default '0';
    declare v_growth_model_flag     tinyint(1) default '0';
           
    declare cur_1 cursor for 
        select  sub.ayp_subject_id
                ,max(tty.school_year_id) as max_test_year_id
                ,max(tt.test_each_year_flag) as test_each_year_flag
                ,max(sub.alt_score_flag) as alt_score_flag
        from    c_ayp_subject as sub
        join    c_ayp_test_type as tt
                on      sub.ayp_test_type_id = tt.ayp_test_type_id
        join    c_ayp_test_type_year as tty
                on      tt.ayp_test_type_id = tty.ayp_test_type_id
        join    c_course_type_ayp_subject_list as ctasl
                on      sub.ayp_subject_id = ctasl.ayp_subject_id
        where exists (  select  *
                        from    c_ayp_subject_student as ss
                        where   ss.ayp_subject_id = sub.ayp_subject_id
                     )
        group by sub.ayp_subject_id
        ;
    
    declare continue handler for not found 
    set no_more_rows = true;

    # These 3 tables will contain linkage from student
    # to state test subject via c_course_type_ayp_subject_list
    # and student's course schedule.  The tables are required
    # to eliminate errors in counting same student
    # multiple times in various levels of aggregation.
    drop table if exists `tmp_sub_user_student_list`;
    CREATE TABLE `tmp_sub_user_student_list` (
      `ayp_subject_id` int(10) NOT NULL,
      `school_id` int(10) NOT NULL,
      `user_id` int(10) NOT NULL,
      `student_id` int(10) NOT NULL,
      PRIMARY KEY  (`ayp_subject_id`,`school_id`,`user_id`,`student_id`),
      KEY `ind_tmp_sub_user_student_list` (`student_id`,`ayp_subject_id`,`school_id`,`user_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1
    ;

    drop table if exists `tmp_sub_school_student_list`;
    CREATE TABLE `tmp_sub_school_student_list` (
      `ayp_subject_id` int(10) NOT NULL,
      `school_id` int(10) NOT NULL,
      `student_id` int(10) NOT NULL,
      PRIMARY KEY  (`ayp_subject_id`,`school_id`,`student_id`),
      KEY `ind_tmp_sub_school_student_list` (`student_id`,`ayp_subject_id`,`school_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1
    ;

    drop table if exists `tmp_sub_student_list`;
    CREATE TABLE `tmp_sub_student_list` (
      `ayp_subject_id` int(10) NOT NULL,
      `student_id` int(10) NOT NULL,
      PRIMARY KEY  (`ayp_subject_id`,`student_id`),
      KEY `ind_tmp_sub_student_list` (`student_id`,`ayp_subject_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1
    ;


    truncate table rpt_ayp_teacher_performance;

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    # Determine if state uses growth model
    select  growth_model_flag
    into    v_growth_model_flag
    from    pmi_state_info
    where   state_id = @state_id
    ;
    
    # used to get NCLB info for current school year
    select  school_year_id
    into    v_curr_yr_id
    from    c_school_year
    where   active_flag = 1
    ;

    # Used to load teacher level aggregate data
    insert tmp_sub_user_student_list (
        ayp_subject_id
        ,school_id
        ,user_id
        ,student_id
    )
    select  ctsl.ayp_subject_id
        ,cl.school_id
        ,cl.user_id
        ,cle.student_id
    from    c_class as cl
    join    c_class_enrollment as cle
            on      cle.class_id = cl.class_id
    join    c_course as crs
            on      crs.course_id = cl.course_id
    join    c_course_type_ayp_subject_list as ctsl
            on      crs.course_type_id = ctsl.course_type_id
    group by ctsl.ayp_subject_id
        ,cl.school_id
        ,cl.user_id
        ,cle.student_id
    ;

    # Used to load school level aggregate data
    insert tmp_sub_school_student_list (
        ayp_subject_id
        ,school_id
        ,student_id
    )
    select  ayp_subject_id
        ,school_id
        ,student_id
    from    tmp_sub_user_student_list
    group by ayp_subject_id
        ,school_id
        ,student_id
    ;

    # Used to load district level aggregate data
    insert tmp_sub_student_list (
        ayp_subject_id
        ,student_id
    )
    select  ayp_subject_id
        ,student_id
    from    tmp_sub_school_student_list
    group by ayp_subject_id
        ,student_id
    ;

    # Populate table - looping by subject
    open cur_1;
    loop_cur_1: loop
    
        fetch  cur_1 
        into   v_ayp_subject_id, v_max_test_yr_id, v_test_each_year_flag, v_alt_score_flag;
               
        if no_more_rows then
            close cur_1;
            leave loop_cur_1;
        end if;

        if v_test_each_year_flag = 1 then
            set v_prev_yr_id = v_max_test_yr_id - 1;
        else
            set v_prev_yr_id = 0;
        end if;

        # State Test Subject, School, Teacher, Class, Grade, NCLB Group
        insert  rpt_ayp_teacher_performance (
            ayp_subject_id
            ,school_id
            ,school_cluster_id
            ,user_id
            ,class_id
            ,grade_level_id
            ,ayp_group_id
            ,school_year_id
            ,total_stu_cnt
            ,prof_cnt
            ,cohort_stu_cnt
            ,cohort_prof_cnt
            ,cohort_prev_yr_prof_cnt
            ,cohort_al_no_chg_cnt
            ,cohort_al_decr_cnt
            ,cohort_al_incr_cnt
            ,cohort_lrn_gain_cnt
            ,cohort_curr_yr_dss_total
            ,cohort_prev_yr_dss_total
            ,cohort_no_lrn_gain_dss_decr_cnt
            ,cohort_no_lrn_gain_dss_same_incr_cnt
            ,create_timestamp
        )
        
        select  ss.ayp_subject_id
            ,cl.school_id
            ,scl.cluster_id
            ,cl.user_id
            ,cl.class_id
            ,sg.grade_level_id
            ,sg.ayp_group_id
            ,v_max_test_yr_id
            ,count(*) as total_stu_cnt
            ,count(case when atta.pass_flag = 1 then ss.student_id end) as prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null then ssprev.student_id else null end) as cohort_stu_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.pass_flag = 1 then ssprev.student_id else null end) as cohort_prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and prvatta.pass_flag = 1 then ssprev.student_id end) as cohort_prev_yr_prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and ss.al_id = ssprev.al_id then ssprev.student_id end) as cohort_al_no_chg_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.client_al < prvatta.client_al then ssprev.student_id end) as cohort_al_decr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.client_al > prvatta.client_al then ssprev.student_id end) as cohort_al_incr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and ss.lrn_gain_flag = 1 then ssprev.student_id else null end) as cohort_lrn_gain_cnt
            ,sum(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 then coalesce(ss.alt_ayp_score, 0) end) as cohort_curr_yr_dss_total
            ,sum(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 then coalesce(ssprev.alt_ayp_score, 0) end) as cohort_prev_yr_dss_total
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 and ss.lrn_gain_flag = 0 and ss.alt_ayp_score < ssprev.alt_ayp_score then ssprev.student_id else null end) as cohort_no_lrn_gain_dss_decr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 and ss.lrn_gain_flag = 0 and ss.alt_ayp_score >= ssprev.alt_ayp_score then ssprev.student_id else null end) as cohort_no_lrn_gain_dss_same_incr_cnt
            ,now()
        
        from    c_ayp_subject_student as ss
        join    c_ayp_subject as sub
                on      ss.ayp_subject_id = sub.ayp_subject_id
        join    c_ayp_test_type_al as atta
                on      sub.ayp_test_type_id = atta.ayp_test_type_id
                and     ss.al_id = atta.al_id
        join    rpt_student_group as sg
                on      ss.student_id = sg.student_id
                and     sg.school_year_id = v_curr_yr_id
        join    c_ayp_group as ag
                on      sg.ayp_group_id = ag.ayp_group_id
                and     ag.ayp_accel_flag = 1
        join    c_class_enrollment as cle
                on      cle.student_id = ss.student_id
        join    c_class as cl
                on      cl.class_id = cle.class_id
        join    c_course as crs
                on      crs.course_id = cl.course_id
        join    c_school as sch
                on      cl.school_id = sch.school_id
        join    c_course_type_ayp_subject_list as ctsl
                on      crs.course_type_id = ctsl.course_type_id
                and     ss.ayp_subject_id = ctsl.ayp_subject_id
        left join   c_ayp_subject_student as ssprev
                on      ss.student_id = ssprev.student_id
                and     ss.ayp_subject_id = ssprev.ayp_subject_id
                and     ssprev.school_year_id = v_prev_yr_id
                and     ssprev.al_id is not null
        left join    c_ayp_test_type_al as prvatta
                on      sub.ayp_test_type_id = prvatta.ayp_test_type_id
                # excludes incomplete data for fl when dss_score not given, but ss is
                and     ssprev.al_id = prvatta.al_id
        left join   c_school_cluster_list as scl
                on      sch.school_id = scl.school_id
        where   ss.ayp_subject_id = v_ayp_subject_id
        and     ss.school_year_id = v_max_test_yr_id
        # excludes incomplete data for fl when dss_score not given, but ss is
        and     ss.al_id is not null
        and     ss.score_record_flag = 1
        group by ss.ayp_subject_id, cl.school_id, scl.cluster_id, cl.user_id, cl.class_id, sg.grade_level_id, sg.ayp_group_id
        ;

        # State Test Subject, School, Teacher, Grade, NCLB Group
        insert  rpt_ayp_teacher_performance (
            ayp_subject_id
            ,school_id
            ,school_cluster_id
            ,user_id
            ,class_id
            ,grade_level_id
            ,ayp_group_id
            ,school_year_id
            ,total_stu_cnt
            ,prof_cnt
            ,cohort_stu_cnt
            ,cohort_prof_cnt
            ,cohort_prev_yr_prof_cnt
            ,cohort_al_no_chg_cnt
            ,cohort_al_decr_cnt
            ,cohort_al_incr_cnt
            ,cohort_lrn_gain_cnt
            ,cohort_curr_yr_dss_total
            ,cohort_prev_yr_dss_total
            ,cohort_no_lrn_gain_dss_decr_cnt
            ,cohort_no_lrn_gain_dss_same_incr_cnt
            ,create_timestamp
        )
        
        select  ss.ayp_subject_id
            ,substulst.school_id
            ,scl.cluster_id
            ,substulst.user_id
            ,0
            ,sg.grade_level_id
            ,sg.ayp_group_id
            ,v_max_test_yr_id
            ,count(*) as total_stu_cnt
            ,count(case when atta.pass_flag = 1 then ss.student_id end) as prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null then ssprev.student_id else null end) as cohort_stu_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.pass_flag = 1 then ssprev.student_id else null end) as cohort_prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and prvatta.pass_flag = 1 then ssprev.student_id end) as cohort_prev_yr_prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and ss.al_id = ssprev.al_id then ssprev.student_id end) as cohort_al_no_chg_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.client_al < prvatta.client_al then ssprev.student_id end) as cohort_al_decr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.client_al > prvatta.client_al then ssprev.student_id end) as cohort_al_incr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and ss.lrn_gain_flag = 1 then ssprev.student_id else null end) as cohort_lrn_gain_cnt
            ,sum(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 then coalesce(ss.alt_ayp_score, 0) end) as cohort_curr_yr_dss_total
            ,sum(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 then coalesce(ssprev.alt_ayp_score, 0) end) as cohort_prev_yr_dss_total
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 and ss.lrn_gain_flag = 0 and ss.alt_ayp_score < ssprev.alt_ayp_score then ssprev.student_id else null end) as cohort_no_lrn_gain_dss_decr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 and ss.lrn_gain_flag = 0 and ss.alt_ayp_score >= ssprev.alt_ayp_score then ssprev.student_id else null end) as cohort_no_lrn_gain_dss_same_incr_cnt
            ,now()
        
        from    c_ayp_subject_student as ss
        join    c_ayp_subject as sub
                on      ss.ayp_subject_id = sub.ayp_subject_id
        join    c_ayp_test_type_al as atta
                on      sub.ayp_test_type_id = atta.ayp_test_type_id
                and     ss.al_id = atta.al_id
        join    rpt_student_group as sg
                on      ss.student_id = sg.student_id
                and     sg.school_year_id = v_curr_yr_id
        join    c_ayp_group as ag
                on      sg.ayp_group_id = ag.ayp_group_id
                and     ag.ayp_accel_flag = 1
        join    tmp_sub_user_student_list as substulst
                on      ss.student_id = substulst.student_id
                and     ss.ayp_subject_id = substulst.ayp_subject_id
        join    c_school as sch
                on      substulst.school_id = sch.school_id
        left join   c_ayp_subject_student as ssprev
                on      ss.student_id = ssprev.student_id
                and     ss.ayp_subject_id = ssprev.ayp_subject_id
                and     ssprev.school_year_id = v_prev_yr_id
                and     ssprev.al_id is not null
        left join    c_ayp_test_type_al as prvatta
                on      sub.ayp_test_type_id = prvatta.ayp_test_type_id
                # excludes incomplete data for fl when dss_score not given, but ss is
                and     ssprev.al_id = prvatta.al_id
        left join   c_school_cluster_list as scl
                on      sch.school_id = scl.school_id
        where   ss.ayp_subject_id = v_ayp_subject_id
        and     ss.school_year_id = v_max_test_yr_id
        # excludes incomplete data for fl when dss_score not given, but ss is
        and     ss.al_id is not null
        and     ss.score_record_flag = 1
        group by ss.ayp_subject_id, substulst.school_id, scl.cluster_id, substulst.user_id, sg.grade_level_id, sg.ayp_group_id
        ;

        # State Test Subject, School, Grade, NCLB Group
        insert  rpt_ayp_teacher_performance (
            ayp_subject_id
            ,school_id
            ,school_cluster_id
            ,user_id
            ,class_id
            ,grade_level_id
            ,ayp_group_id
            ,school_year_id
            ,total_stu_cnt
            ,prof_cnt
            ,cohort_stu_cnt
            ,cohort_prof_cnt
            ,cohort_prev_yr_prof_cnt
            ,cohort_al_no_chg_cnt
            ,cohort_al_decr_cnt
            ,cohort_al_incr_cnt
            ,cohort_lrn_gain_cnt
            ,cohort_curr_yr_dss_total
            ,cohort_prev_yr_dss_total
            ,cohort_no_lrn_gain_dss_decr_cnt
            ,cohort_no_lrn_gain_dss_same_incr_cnt
            ,create_timestamp
        )
        
        select  ss.ayp_subject_id
            ,substulst.school_id
            ,scl.cluster_id
            ,0
            ,0
            ,sg.grade_level_id
            ,sg.ayp_group_id
            ,v_max_test_yr_id
            ,count(*) as total_stu_cnt
            ,count(case when atta.pass_flag = 1 then ss.student_id end) as prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null then ssprev.student_id else null end) as cohort_stu_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.pass_flag = 1 then ssprev.student_id else null end) as cohort_prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and prvatta.pass_flag = 1 then ssprev.student_id end) as cohort_prev_yr_prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and ss.al_id = ssprev.al_id then ssprev.student_id end) as cohort_al_no_chg_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.client_al < prvatta.client_al then ssprev.student_id end) as cohort_al_decr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.client_al > prvatta.client_al then ssprev.student_id end) as cohort_al_incr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and ss.lrn_gain_flag = 1 then ssprev.student_id else null end) as cohort_lrn_gain_cnt
            ,sum(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 then coalesce(ss.alt_ayp_score, 0) end) as cohort_curr_yr_dss_total
            ,sum(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 then coalesce(ssprev.alt_ayp_score, 0) end) as cohort_prev_yr_dss_total
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 and ss.lrn_gain_flag = 0 and ss.alt_ayp_score < ssprev.alt_ayp_score then ssprev.student_id else null end) as cohort_no_lrn_gain_dss_decr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 and ss.lrn_gain_flag = 0 and ss.alt_ayp_score >= ssprev.alt_ayp_score then ssprev.student_id else null end) as cohort_no_lrn_gain_dss_same_incr_cnt
            ,now()
        
        from    c_ayp_subject_student as ss
        join    c_ayp_subject as sub
                on      ss.ayp_subject_id = sub.ayp_subject_id
        join    c_ayp_test_type_al as atta
                on      sub.ayp_test_type_id = atta.ayp_test_type_id
                and     ss.al_id = atta.al_id
        join    rpt_student_group as sg
                on      ss.student_id = sg.student_id
                and     sg.school_year_id = v_curr_yr_id
        join    c_ayp_group as ag
                on      sg.ayp_group_id = ag.ayp_group_id
                and     ag.ayp_accel_flag = 1
        join    tmp_sub_school_student_list as substulst
                on      ss.student_id = substulst.student_id
                and     ss.ayp_subject_id = substulst.ayp_subject_id
        join    c_school as sch
                on      substulst.school_id = sch.school_id
        left join   c_ayp_subject_student as ssprev
                on      ss.student_id = ssprev.student_id
                and     ss.ayp_subject_id = ssprev.ayp_subject_id
                and     ssprev.school_year_id = v_prev_yr_id
                and     ssprev.al_id is not null
        left join    c_ayp_test_type_al as prvatta
                on      sub.ayp_test_type_id = prvatta.ayp_test_type_id
                # excludes incomplete data for fl when dss_score not given, but ss is
                and     ssprev.al_id = prvatta.al_id
        left join   c_school_cluster_list as scl
                on      sch.school_id = scl.school_id
        where   ss.ayp_subject_id = v_ayp_subject_id
        and     ss.school_year_id = v_max_test_yr_id
        # excludes incomplete data for fl when dss_score not given, but ss is
        and     ss.al_id is not null
        and     ss.score_record_flag = 1
        group by ss.ayp_subject_id, substulst.school_id, scl.cluster_id, sg.grade_level_id, sg.ayp_group_id
        ;

        # State Test Subject, Cluster, Grade, NCLB Group
        insert  rpt_ayp_teacher_performance (
            ayp_subject_id
            ,school_id
            ,school_cluster_id
            ,user_id
            ,class_id
            ,grade_level_id
            ,ayp_group_id
            ,school_year_id
            ,total_stu_cnt
            ,prof_cnt
            ,cohort_stu_cnt
            ,cohort_prof_cnt
            ,cohort_prev_yr_prof_cnt
            ,cohort_al_no_chg_cnt
            ,cohort_al_decr_cnt
            ,cohort_al_incr_cnt
            ,cohort_lrn_gain_cnt
            ,cohort_curr_yr_dss_total
            ,cohort_prev_yr_dss_total
            ,cohort_no_lrn_gain_dss_decr_cnt
            ,cohort_no_lrn_gain_dss_same_incr_cnt
            ,create_timestamp
        )
        
        select  ss.ayp_subject_id
            ,0
            ,scl.cluster_id
            ,0
            ,0
            ,sg.grade_level_id
            ,sg.ayp_group_id
            ,v_max_test_yr_id
            ,count(*) as total_stu_cnt
            ,count(case when atta.pass_flag = 1 then ss.student_id end) as prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null then ssprev.student_id else null end) as cohort_stu_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.pass_flag = 1 then ssprev.student_id else null end) as cohort_prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and prvatta.pass_flag = 1 then ssprev.student_id end) as cohort_prev_yr_prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and ss.al_id = ssprev.al_id then ssprev.student_id end) as cohort_al_no_chg_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.client_al < prvatta.client_al then ssprev.student_id end) as cohort_al_decr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.client_al > prvatta.client_al then ssprev.student_id end) as cohort_al_incr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and ss.lrn_gain_flag = 1 then ssprev.student_id else null end) as cohort_lrn_gain_cnt
            ,sum(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 then coalesce(ss.alt_ayp_score, 0) end) as cohort_curr_yr_dss_total
            ,sum(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 then coalesce(ssprev.alt_ayp_score, 0) end) as cohort_prev_yr_dss_total
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 and ss.lrn_gain_flag = 0 and ss.alt_ayp_score < ssprev.alt_ayp_score then ssprev.student_id else null end) as cohort_no_lrn_gain_dss_decr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 and ss.lrn_gain_flag = 0 and ss.alt_ayp_score >= ssprev.alt_ayp_score then ssprev.student_id else null end) as cohort_no_lrn_gain_dss_same_incr_cnt
            ,now()
        
        from    c_ayp_subject_student as ss
        join    c_ayp_subject as sub
                on      ss.ayp_subject_id = sub.ayp_subject_id
        join    c_ayp_test_type_al as atta
                on      sub.ayp_test_type_id = atta.ayp_test_type_id
                and     ss.al_id = atta.al_id
        join    rpt_student_group as sg
                on      ss.student_id = sg.student_id
                and     sg.school_year_id = v_curr_yr_id
        join    c_ayp_group as ag
                on      sg.ayp_group_id = ag.ayp_group_id
                and     ag.ayp_accel_flag = 1
        join    tmp_sub_school_student_list as substulst
                on      ss.student_id = substulst.student_id
                and     ss.ayp_subject_id = substulst.ayp_subject_id
        join    c_school as sch
                on      substulst.school_id = sch.school_id
        left join   c_ayp_subject_student as ssprev
                on      ss.student_id = ssprev.student_id
                and     ss.ayp_subject_id = ssprev.ayp_subject_id
                and     ssprev.school_year_id = v_prev_yr_id
                and     ssprev.al_id is not null
        left join    c_ayp_test_type_al as prvatta
                on      sub.ayp_test_type_id = prvatta.ayp_test_type_id
                # excludes incomplete data for fl when dss_score not given, but ss is
                and     ssprev.al_id = prvatta.al_id
        left join   c_school_cluster_list as scl
                on      sch.school_id = scl.school_id
        where   ss.ayp_subject_id = v_ayp_subject_id
        and     ss.school_year_id = v_max_test_yr_id
        # excludes incomplete data for fl when dss_score not given, but ss is
        and     ss.al_id is not null
        and     ss.score_record_flag = 1
        group by ss.ayp_subject_id, scl.cluster_id, sg.grade_level_id, sg.ayp_group_id
        ;

        # State Test Subject, Grade, NCLB Group
        insert  rpt_ayp_teacher_performance (
            ayp_subject_id
            ,school_id
            ,school_cluster_id
            ,user_id
            ,class_id
            ,grade_level_id
            ,ayp_group_id
            ,school_year_id
            ,total_stu_cnt
            ,prof_cnt
            ,cohort_stu_cnt
            ,cohort_prof_cnt
            ,cohort_prev_yr_prof_cnt
            ,cohort_al_no_chg_cnt
            ,cohort_al_decr_cnt
            ,cohort_al_incr_cnt
            ,cohort_lrn_gain_cnt
            ,cohort_curr_yr_dss_total
            ,cohort_prev_yr_dss_total
            ,cohort_no_lrn_gain_dss_decr_cnt
            ,cohort_no_lrn_gain_dss_same_incr_cnt
            ,create_timestamp
        )
        
        select  ss.ayp_subject_id
            ,0
            ,0
            ,0
            ,0
            ,sg.grade_level_id
            ,sg.ayp_group_id
            ,v_max_test_yr_id
            ,count(*) as total_stu_cnt
            ,count(case when atta.pass_flag = 1 then ss.student_id end) as prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null then ssprev.student_id else null end) as cohort_stu_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.pass_flag = 1 then ssprev.student_id else null end) as cohort_prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and prvatta.pass_flag = 1 then ssprev.student_id end) as cohort_prev_yr_prof_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and ss.al_id = ssprev.al_id then ssprev.student_id end) as cohort_al_no_chg_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.client_al < prvatta.client_al then ssprev.student_id end) as cohort_al_decr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and atta.client_al > prvatta.client_al then ssprev.student_id end) as cohort_al_incr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and ss.lrn_gain_flag = 1 then ssprev.student_id else null end) as cohort_lrn_gain_cnt
            ,sum(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 then coalesce(ss.alt_ayp_score, 0) end) as cohort_curr_yr_dss_total
            ,sum(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 then coalesce(ssprev.alt_ayp_score, 0) end) as cohort_prev_yr_dss_total
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 and ss.lrn_gain_flag = 0 and ss.alt_ayp_score < ssprev.alt_ayp_score then ssprev.student_id else null end) as cohort_no_lrn_gain_dss_decr_cnt
            ,count(case when v_test_each_year_flag = 1 and ssprev.student_id is not null and v_growth_model_flag = 1 and v_alt_score_flag = 1 and ss.lrn_gain_flag = 0 and ss.alt_ayp_score >= ssprev.alt_ayp_score then ssprev.student_id else null end) as cohort_no_lrn_gain_dss_same_incr_cnt
            ,now()
        
        from    c_ayp_subject_student as ss
        join    c_ayp_subject as sub
                on      ss.ayp_subject_id = sub.ayp_subject_id
        join    c_ayp_test_type_al as atta
                on      sub.ayp_test_type_id = atta.ayp_test_type_id
                and     ss.al_id = atta.al_id
        join    rpt_student_group as sg
                on      ss.student_id = sg.student_id
                and     sg.school_year_id = v_curr_yr_id
        join    c_ayp_group as ag
                on      sg.ayp_group_id = ag.ayp_group_id
                and     ag.ayp_accel_flag = 1
        join    tmp_sub_student_list as substulst
                on      ss.student_id = substulst.student_id
                and     ss.ayp_subject_id = substulst.ayp_subject_id
        left join   c_ayp_subject_student as ssprev
                on      ss.student_id = ssprev.student_id
                and     ss.ayp_subject_id = ssprev.ayp_subject_id
                and     ssprev.school_year_id = v_prev_yr_id
                and     ssprev.al_id is not null
        left join    c_ayp_test_type_al as prvatta
                on      sub.ayp_test_type_id = prvatta.ayp_test_type_id
                # excludes incomplete data for fl when dss_score not given, but ss is
                and     ssprev.al_id = prvatta.al_id
        where   ss.ayp_subject_id = v_ayp_subject_id
        and     ss.school_year_id = v_max_test_yr_id
        # excludes incomplete data for fl when dss_score not given, but ss is
        and     ss.al_id is not null
        and     ss.score_record_flag = 1
        group by ss.ayp_subject_id, sg.grade_level_id, sg.ayp_group_id
        ;


    end loop loop_cur_1;

    # State Test Subject, School, Teacher, Class, NCLB Group
    # agg across grades
    insert  rpt_ayp_teacher_performance (
        ayp_subject_id
        ,school_id
        ,school_cluster_id
        ,user_id
        ,class_id
        ,grade_level_id
        ,ayp_group_id
        ,school_year_id
        ,total_stu_cnt
        ,prof_cnt
        ,cohort_stu_cnt
        ,cohort_prof_cnt
        ,cohort_prev_yr_prof_cnt
        ,cohort_al_no_chg_cnt
        ,cohort_al_decr_cnt
        ,cohort_al_incr_cnt
        ,cohort_lrn_gain_cnt
        ,cohort_curr_yr_dss_total
        ,cohort_prev_yr_dss_total
        ,cohort_no_lrn_gain_dss_decr_cnt
        ,cohort_no_lrn_gain_dss_same_incr_cnt
        ,create_timestamp
    )
    
    select  ayp_subject_id
        ,school_id
        ,school_cluster_id
        ,user_id
        ,class_id
        ,0
        ,ayp_group_id
        ,max(school_year_id)
        ,sum(total_stu_cnt)
        ,sum(prof_cnt)
        ,sum(cohort_stu_cnt)
        ,sum(cohort_prof_cnt)
        ,sum(cohort_prev_yr_prof_cnt)
        ,sum(cohort_al_no_chg_cnt)
        ,sum(cohort_al_decr_cnt)
        ,sum(cohort_al_incr_cnt)
        ,sum(cohort_lrn_gain_cnt)
        ,sum(cohort_curr_yr_dss_total)
        ,sum(cohort_prev_yr_dss_total)
        ,sum(cohort_no_lrn_gain_dss_decr_cnt)
        ,sum(cohort_no_lrn_gain_dss_same_incr_cnt)
        ,now()
    
    from    rpt_ayp_teacher_performance
    group by ayp_subject_id, school_id, school_cluster_id, user_id, class_id, ayp_group_id
    ;
        
    # update percentages
    update  rpt_ayp_teacher_performance
    set     prof_pct = round((prof_cnt / total_stu_cnt) * 100, 3)
            ,cohort_prof_pct = round((cohort_prof_cnt / cohort_stu_cnt) * 100, 3)
            ,cohort_prev_yr_prof_pct = round((cohort_prev_yr_prof_cnt / cohort_stu_cnt) * 100, 3)
            ,cohort_chg_pct = round((cohort_prof_pct - cohort_prev_yr_prof_pct), 3)
            ,cohort_al_no_chg_pct = round((cohort_al_no_chg_cnt / cohort_stu_cnt) * 100, 3)
            ,cohort_al_decr_pct = round((cohort_al_decr_cnt / cohort_stu_cnt) * 100, 3)
            ,cohort_al_incr_pct = round((cohort_al_incr_cnt / cohort_stu_cnt) * 100, 3)
            ,cohort_lrn_gain_pct = round((cohort_lrn_gain_cnt / cohort_stu_cnt) * 100, 3)
            ,cohort_curr_yr_mean_dss = round((cohort_curr_yr_dss_total / cohort_stu_cnt), 3)
            ,cohort_prev_yr_mean_dss = round((cohort_prev_yr_dss_total / cohort_stu_cnt), 3)
            ,cohort_mean_dss_net = cohort_curr_yr_mean_dss - cohort_prev_yr_mean_dss
            ,cohort_no_lrn_gain_dss_decr_pct = round((cohort_no_lrn_gain_dss_decr_cnt / cohort_stu_cnt) * 100, 3)
            ,cohort_no_lrn_gain_dss_same_incr_pct = round((cohort_no_lrn_gain_dss_same_incr_cnt / cohort_stu_cnt) * 100, 3)
    ;

    # Cleanup working tables used to load data
    drop table if exists `tmp_sub_user_student_list`;
    drop table if exists `tmp_sub_school_student_list`;
    drop table if exists `tmp_sub_student_list`;

end proc;
//
