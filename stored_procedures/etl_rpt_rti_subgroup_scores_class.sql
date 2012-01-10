drop procedure if exists etl_rpt_rti_subgroup_scores_class//

create definer=`dbadmin`@`localhost` procedure etl_rpt_rti_subgroup_scores_class()
contains sql
sql security invoker
comment '$Rev$ $Date$'


proc: begin 

    declare v_curr_school_year_id         int(11);

    select  school_year_id
    into    v_curr_school_year_id
    from    c_school_year
    where   active_flag = 1
    ;

    truncate table rpt_rti_subgroup_scores_class;
    
    insert rpt_rti_subgroup_scores_class (
        measure_id
        ,intervention_id
        ,class_id
        ,event_date
        ,ayp_group_id
        ,points_earned
        ,points_possible
        ,school_year_id
        ,last_user_id
    )
    
    select  si.measure_id
        ,si.intervention_id
        ,cle.class_id
        ,sipml.event_date
        ,ag.ayp_group_id
        ,sum(coalesce(sipml.score, 0)) as pe
        ,sum(coalesce(si.goal_score, 0)) as pp
        ,min(sipml.school_year_id) as school_year_id
        ,1234
    
    from    c_rti_student_interv_prog_mon_list as sipml
    join    c_rti_student_interv as si
            on      sipml.intervention_item_id = si.intervention_item_id
    join    c_student_year as sty
            on      sty.student_id = si.student_id
            and     sty.school_year_id = sipml.school_year_id
            and     sty.active_flag = 1
    join    rpt_student_group as rsg
            on      si.student_id = rsg.student_id
            and     rsg.school_year_id = sty.school_year_id
    join    c_ayp_group as ag
            on      rsg.ayp_group_id = ag.ayp_group_id
            and     ag.ayp_accel_flag = 1
    join    c_class_enrollment as cle
            on      si.student_id = cle.student_id
    group by si.measure_id, si.intervention_id, cle.class_id, event_date, ag.ayp_group_id
    ;

end proc;
//
