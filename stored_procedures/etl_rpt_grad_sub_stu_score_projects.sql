/*
$Rev: 8043 $ 
$Author: randall.stanley $ 
$Date: 2009-12-17 11:52:06 -0500 (Thu, 17 Dec 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_grad_sub_stu_score_projects.sql $
$Id: etl_rpt_grad_sub_stu_score_projects.sql 8043 2009-12-17 16:52:06Z randall.stanley $ 
 */

# ETL Calc Col Sync
drop procedure if exists etl_rpt_grad_sub_stu_score_projects //
####################################################################
# Insert rpt_grad_sub_stu_score_projects data # 
####################################################################
# call etl_rpt_grad_sub_stu_score_projects();
#select * from rpt_grad_sub_stu_score_projects  limit 100;


create definer=`dbadmin`@`localhost` procedure etl_rpt_grad_sub_stu_score_projects ()

contains sql
sql security invoker
comment '$rev: sync etl calc col metatdata with master $'

proc: begin

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
  
    truncate table rpt_grad_sub_stu_score_projects;

    #### restructure source data   ####      

    drop table if exists tmp_grad_stu_score_proj;

    create table tmp_grad_stu_score_proj (
        student_id int(11) not null
        ,ayp_subject_id int(11) 
        ,school_year_id int(11) 
        ,max_ayp_score smallint(3)
        ,max_alt_ayp_score smallint(3)
        ,min_projects_required smallint(3)
        ,max_projects_completed smallint(3)
        ,remaining_projects smallint(3)
        ,primary key uq_tmp_id_assign (student_id, ayp_subject_id)
        );

    insert into tmp_grad_stu_score_proj (
        student_id
        ,ayp_subject_id)
        
        select stu.student_id, sub.ayp_subject_id
        from c_student stu
        cross join c_ayp_subject sub
        where stu.grad_eligible_flag = 1
            and sub.grad_report_flag = 1
        group by stu.student_id, sub.ayp_subject_id
        ;

    insert into tmp_grad_stu_score_proj (
        student_id
        ,ayp_subject_id
        ,school_year_id
        ,max_ayp_score
        ,max_alt_ayp_score
        ,min_projects_required
        ,max_projects_completed
        ,remaining_projects
    )

    select  stu.student_id
        ,sub.ayp_subject_id
        ,max(school_year_id) as school_year_id
        ,max(ayp_score) as max_ayp_score
        ,max(alt_ayp_score) as max_alt_ayp_score
        ,min(projects_required) as min_projects_required
        ,max(projects_completed) as max_projects_completed
        ,case when min(projects_required) - max(projects_completed) < 0 then 0 
            else min(projects_required) - max(projects_completed) end as remaining_projects
    from c_student stu            
    join c_ayp_subject_student substu
        on stu.student_id = substu.student_id
    join c_ayp_subject  sub
        on substu.ayp_subject_id = sub.ayp_subject_id
    left join c_grad_subject_student_projects proj
        on stu.student_id = proj.student_id
        and sub.ayp_subject_id = proj.ayp_subject_id
   where stu.grad_eligible_flag = 1
        and sub.grad_report_flag = 1
    group by    substu.student_id
                ,substu.ayp_subject_id
    on duplicate key update
        school_year_id = values(school_year_id)
        ,max_ayp_score = values(max_ayp_score)
        ,max_alt_ayp_score = values(max_alt_ayp_score)
        ,min_projects_required = values(min_projects_required)
        ,max_projects_completed = values(max_projects_completed)
        ,remaining_projects = values(remaining_projects)
    ;

    ###### insert ayp_subject_id records ########

    insert into rpt_grad_sub_stu_score_projects (
        student_id
        , ayp_subject_id
        , ayp_score
        , ayp_score_color
        , alt_ayp_score
        , alt_ayp_score_color
        , projects_required
        , projects_completed
        , projects_remaining
        , projects_remaining_color
        , last_user_id
        , create_timestamp)

    select  a.student_id
            , a.ayp_subject_id
            , a.max_ayp_score
            , pc1.moniker as ayp_score_color
            , a.max_alt_ayp_score
            , pc2.moniker as ayp_alt_score_color
            , a.min_projects_required
            , a.max_projects_completed
            , a.remaining_projects
            , cp3.moniker as remaining_projects_color
            , 1234 as last_user_id
            , current_timestamp as create_timestamp
    
    from tmp_grad_stu_score_proj a
    left join c_color_ayp_subject c1
        on a.ayp_subject_id = c1.ayp_subject_id
        and a.school_year_id  between c1.begin_year and c1.end_year
        and a.max_ayp_score between c1.min_score and c1.max_score
    left join pmi_color pc1
        on c1.color_id = pc1.color_id
    left join c_color_ayp_subject c2
        on a.ayp_subject_id = c2.ayp_subject_id
        and a.school_year_id between c2.begin_year and c2.end_year
        and a.max_alt_ayp_score between c2.min_score and c2.max_score
    left join pmi_color pc2
        on c2.color_id = pc2.color_id
    left join c_color_grad_projects cproj
        on a.ayp_subject_id = cproj.ayp_subject_id
        and remaining_projects between cproj.min_projects and cproj.max_projects
    left join pmi_color cp3
        on cproj.color_id = cp3.color_id

    on duplicate key update ayp_score = values(ayp_score)
                            ,ayp_score_color = values(ayp_score_color)
                            ,alt_ayp_score = values(alt_ayp_score)
                            ,alt_ayp_score_color = values(alt_ayp_score_color)
                            ,projects_required = values(projects_required)
                            ,projects_completed = values(projects_completed)
                            ,projects_remaining = values(projects_remaining)
                            ,projects_remaining_color = values(projects_remaining_color)
                            ,last_user_id = values(last_user_id)
    ;


    ###### insert 0 ayp_subject_id records ########

     insert into rpt_grad_sub_stu_score_projects (
         student_id
         ,ayp_subject_id
         ,ayp_score
         ,alt_ayp_score
         ,projects_required
         ,projects_completed
         ,projects_remaining
         ,projects_remaining_color
         ,last_user_id
         ,create_timestamp
     )

    select  student_id
        ,a.ayp_subject_id
        ,ayp_score
        ,alt_ayp_score
        ,projects_required
        ,projects_completed
        ,remaining_projects
        ,moniker as remaining_projects_color
        ,1234 as last_user_id
        ,now() as create_timestamp
    from (  select rpt.student_id
                ,0 as ayp_subject_id
                ,sum(rpt.ayp_score) as ayp_score
                ,sum(rpt.alt_ayp_score) as alt_ayp_score
                ,sum(rpt.projects_required) as projects_required
                ,sum(rpt.projects_completed) as projects_completed
                ,sum(rpt.projects_remaining) as remaining_projects
            from rpt_grad_sub_stu_score_projects rpt
            group by rpt.student_id 
    ) a

    join c_color_grad_projects cproj
        on a.ayp_subject_id = cproj.ayp_subject_id
        and a.remaining_projects between cproj.min_projects and cproj.max_projects
    join pmi_color cp3
        on cproj.color_id = cp3.color_id
    ;

    ### Cleanup and post processing ##
    
    drop table if exists tmp_grad_stu_score_proj;

end proc;
//
