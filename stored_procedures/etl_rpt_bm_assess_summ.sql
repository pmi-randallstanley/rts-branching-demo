/*
$Rev: 8471 $ 
$Author: randall.stanley $ 
$Date: 2010-04-29 16:00:11 -0400 (Thu, 29 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bm_assess_summ.sql $
$Id: etl_rpt_bm_assess_summ.sql 8471 2010-04-29 20:00:11Z randall.stanley $ 
 */

drop procedure if exists etl_rpt_bm_assess_summ//

create definer=`dbadmin`@`localhost` procedure etl_rpt_bm_assess_summ()
contains sql
sql security invoker
comment '$Rev: 8471 $ $Date: 2010-04-29 16:00:11 -0400 (Thu, 29 Apr 2010) $'


proc: begin 

    truncate table rpt_bm_assess_summ;

    select  school_year_id
    into    @curr_sy_id
    from    c_school_year
    where   active_flag = 1
    ;
    
    select  swatch_id
    into    @swatch_id
    from    c_color_swatch
    where   swatch_code = 'benchmark'
    ;
    

    insert  rpt_bm_assess_summ (
        ayp_subject_id
        ,admin_period_id
        ,test_id
        ,school_id
        ,grade_level_id
        ,ayp_group_id
        ,school_type_id
        ,school_cluster_id
        ,color_order
        ,color_name
        ,color_count
        ,pe
        ,pp
        ,create_timestamp
    )
    
    select  td.ayp_subject_id
        ,td.admin_period_id
        ,td.test_id
        ,sch.school_id
        ,sg.grade_level_id
        ,sg.ayp_group_id
        ,sch.school_type_id
    #    ,coalesce(scl.cluster_id, 0) as school_cluster_id
        ,scl.cluster_id
        ,sl.sort_order
        ,clr.moniker
        ,count(*) as color_count
        ,sum(td.pe)
        ,sum(td.pp)
        ,now()
    from    rpt_profile_leading_subject_stu_test_detail as td
    join    rpt_student_group as sg
            on      td.student_id = sg.student_id
            and     sg.school_year_id = @curr_sy_id
    join    c_ayp_group as ag
            on      sg.ayp_group_id = ag.ayp_group_id
            and     ag.ayp_accel_flag = 1
    join    c_school as sch
            on      sg.accessor_id = sch.school_id
    join    c_color_ayp_benchmark as cb
            on      cb.client_id = sch.client_id
            and     cb.ayp_subject_id = td.ayp_subject_id
            and     round(coalesce(td.pe, 0) / td.pp * 100, 0) between cb.min_score and cb.max_score
    join    c_color_swatch_list as sl
            on      sl.client_id = cb.client_id
            and     sl.swatch_id = @swatch_id
            and     cb.color_id = sl.color_id
    join    pmi_color as clr
            on      cb.color_id = clr.color_id
    left join   c_school_cluster_list as scl
            on      sch.school_id = scl.school_id
    group by td.ayp_subject_id, td.admin_period_id, td.test_id, sg.accessor_id, sg.grade_level_id, sg.ayp_group_id, sch.school_type_id, scl.cluster_id, cb.color_id
    ;

    # agg across grades
    insert  rpt_bm_assess_summ (
        ayp_subject_id
        ,admin_period_id
        ,test_id
        ,school_id
        ,grade_level_id
        ,ayp_group_id
        ,school_type_id
        ,school_cluster_id
        ,color_order
        ,color_name
        ,color_count
        ,create_timestamp
    )
    
    select  ayp_subject_id
        ,admin_period_id
        ,test_id
        ,school_id
        ,0 as grade_level_id
        ,ayp_group_id
        ,school_type_id
        ,school_cluster_id
        ,color_order
        ,min(color_name) as color_name
        ,sum(color_count) as color_count
        ,now()
    
    from    rpt_bm_assess_summ
    group by    ayp_subject_id
        ,admin_period_id
        ,test_id
        ,school_id
        ,ayp_group_id
        ,school_type_id
        ,school_cluster_id
        ,color_order
    ;
    
    
    # agg across schools
    insert  rpt_bm_assess_summ (
        ayp_subject_id
        ,admin_period_id
        ,test_id
        ,school_id
        ,grade_level_id
        ,ayp_group_id
        ,school_type_id
        ,school_cluster_id
        ,color_order
        ,color_name
        ,color_count
        ,create_timestamp
    )
    
    select  ayp_subject_id
        ,admin_period_id
        ,test_id
        ,0 as school_id
        ,grade_level_id
        ,ayp_group_id
        ,school_type_id
        ,school_cluster_id
        ,color_order
        ,min(color_name) as color_name
        ,sum(color_count) as color_count
        ,now()
    
    from    rpt_bm_assess_summ
    group by    ayp_subject_id
        ,admin_period_id
        ,test_id
        ,grade_level_id
        ,ayp_group_id
        ,school_type_id
        ,school_cluster_id
        ,color_order
    ;
    
    # agg across school types
    insert  rpt_bm_assess_summ (
        ayp_subject_id
        ,admin_period_id
        ,test_id
        ,school_id
        ,grade_level_id
        ,ayp_group_id
        ,school_type_id
        ,school_cluster_id
        ,color_order
        ,color_name
        ,color_count
        ,create_timestamp
    )
    
    select  ayp_subject_id
        ,admin_period_id
        ,test_id
        ,school_id
        ,grade_level_id
        ,ayp_group_id
        ,0 as school_type_id
        ,school_cluster_id
        ,color_order
        ,min(color_name) as color_name
        ,sum(color_count) as color_count
        ,now()
    
    from    rpt_bm_assess_summ
    where   school_id = 0
    group by    ayp_subject_id
        ,admin_period_id
        ,test_id
        ,school_id
        ,grade_level_id
        ,ayp_group_id
        ,school_cluster_id
        ,color_order
    ;

    # agg across school clusters
    insert  rpt_bm_assess_summ (
        ayp_subject_id
        ,admin_period_id
        ,test_id
        ,school_id
        ,grade_level_id
        ,ayp_group_id
        ,school_type_id
        ,school_cluster_id
        ,color_order
        ,color_name
        ,color_count
        ,create_timestamp
    )
    
    select  ayp_subject_id
        ,admin_period_id
        ,test_id
        ,school_id
        ,grade_level_id
        ,ayp_group_id
        ,school_type_id
        ,0 as school_cluster_id
        ,color_order
        ,min(color_name) as color_name
        ,sum(color_count) as color_count
        ,now()
    
    from    rpt_bm_assess_summ
    where   school_id = 0
    group by    ayp_subject_id
        ,admin_period_id
        ,test_id
        ,school_id
        ,grade_level_id
        ,ayp_group_id
        ,school_type_id
        ,color_order
    ;

    # Update for student_count and color pct
    # for valid school clusters
    update  rpt_bm_assess_summ as summ
    join    (   select  summ2.ayp_subject_id, summ2.admin_period_id, summ2.test_id, summ2.school_id
                        ,summ2.grade_level_id, summ2.ayp_group_id, summ2.school_type_id, summ2.school_cluster_id
                        ,sum(summ2.color_count) as student_count
                from    rpt_bm_assess_summ as summ2
                where   summ2.school_cluster_id is not null
                group by    summ2.ayp_subject_id, summ2.admin_period_id, summ2.test_id, summ2.school_id
                        ,summ2.grade_level_id, summ2.ayp_group_id, summ2.school_type_id, summ2.school_cluster_id
            ) as summcnt
            on      summ.ayp_subject_id = summcnt.ayp_subject_id
            and     summ.admin_period_id = summcnt.admin_period_id
            and     summ.test_id = summcnt.test_id
            and     summ.school_id = summcnt.school_id
            and     summ.grade_level_id = summcnt.grade_level_id
            and     summ.ayp_group_id = summcnt.ayp_group_id
            and     summ.school_type_id = summcnt.school_type_id
            and     summ.school_cluster_id = summcnt.school_cluster_id
    
    set     summ.student_count = summcnt.student_count
            ,summ.color_pct = round((cast(summ.color_count as decimal) / cast(summcnt.student_count as decimal)) * 100, 3)
    where   summ.school_cluster_id is not null
    ;
    
    # Update for student_count and color pct
    # for records without school clusters
    update  rpt_bm_assess_summ as summ
    join    (   select  summ2.ayp_subject_id, summ2.admin_period_id, summ2.test_id, summ2.school_id
                        ,summ2.grade_level_id, summ2.ayp_group_id, summ2.school_type_id
                        ,sum(summ2.color_count) as student_count
                from    rpt_bm_assess_summ as summ2
                where   summ2.school_cluster_id is null
                group by    summ2.ayp_subject_id, summ2.admin_period_id, summ2.test_id, summ2.school_id
                        ,summ2.grade_level_id, summ2.ayp_group_id, summ2.school_type_id
            ) as summcnt
            on      summ.ayp_subject_id = summcnt.ayp_subject_id
            and     summ.admin_period_id = summcnt.admin_period_id
            and     summ.test_id = summcnt.test_id
            and     summ.school_id = summcnt.school_id
            and     summ.grade_level_id = summcnt.grade_level_id
            and     summ.ayp_group_id = summcnt.ayp_group_id
            and     summ.school_type_id = summcnt.school_type_id
    
    set     summ.student_count = summcnt.student_count
            ,summ.color_pct = round((cast(summ.color_count as decimal) / cast(summcnt.student_count as decimal)) * 100, 3)
    where   summ.school_cluster_id is null
    ;

    # can now remove this working table's data
    truncate table rpt_profile_leading_subject_stu_test_detail;

end proc;
//
