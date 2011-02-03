/*
$Rev: 8513 $ 
$Author: randall.stanley $ 
$Date: 2010-05-05 13:03:34 -0400 (Wed, 05 May 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_grad_subj_stud_proj_upd_req_projs.sql $
$Id: etl_grad_subj_stud_proj_upd_req_projs.sql 8513 2010-05-05 17:03:34Z randall.stanley $ 
 */
 
drop procedure if exists etl_grad_subj_stud_proj_upd_req_projs//

create definer=`dbadmin`@`localhost` procedure etl_grad_subj_stud_proj_upd_req_projs()
contains sql
sql security invoker
comment '$Rev: 8513 $ $Date: 2010-05-05 13:03:34 -0400 (Wed, 05 May 2010) $'

proc: begin

            drop table if exists tmp_grad_req_proj;

            CREATE TABLE `tmp_grad_req_proj` (
              `student_id` int(10) not null,
              `ayp_subject_id` int(10) not null,
              `min_grad_pass_score` int(10) not null,
              `school_year_id` int(4) not null,
              `grad_exempt_flag` tinyint(1) not null default '0',
              `ayp_score` decimal(9,3) default null,
              `sub_tested_flag` tinyint(4) not null default '0',
              `sub_passed_flag` tinyint(4) not null default '0',
              `sub_failed_flag` tinyint(4) not null default '0',
              PRIMARY KEY  (`student_id`,`ayp_subject_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;

            insert into tmp_grad_req_proj (
                    student_id
                    ,ayp_subject_id
                    ,min_grad_pass_score
                    ,school_year_id
                    ,grad_exempt_flag
                    ,ayp_score
                    ,sub_tested_flag
                    ,sub_passed_flag
                    ,sub_failed_flag
              )
            
            select stu.student_id
                    ,coalesce(substu.ayp_subject_id,99999999) as ayp_subject_id
                    ,min(min_grad_pass_score) as min_grad_pass_score
                    ,max(school_year_id) as school_year_id
                    ,max(grad_exempt_flag) as grad_exempt_flag
                    ,max(substu.ayp_score) as ayp_score
                    ,case when max(substu.ayp_score) is not null then 1 else 0 end as sub_tested_flag
                    ,case when max(substu.ayp_score) >= sub.min_grad_pass_score then 1 else 0 end as sub_passed_flag
                    ,count(case when substu.ayp_score < sub.min_grad_pass_score then substu.ayp_subject_id end) as sub_failed_flag
            from    c_student stu
            cross join  c_ayp_subject as sub
                    on      sub.grad_report_flag = 1
            left join   c_grad_subject_student_projects as gssp
                    on      stu.student_id = gssp.student_id
                    and     sub.ayp_subject_id = gssp.ayp_subject_id
            left join   c_ayp_subject_student as substu
                    on      stu.student_id = substu.student_id
                    and     sub.ayp_subject_id = substu.ayp_subject_id
            where           stu.grad_eligible_flag = 1
            group by    stu.student_id, substu.ayp_subject_id
            having  count(case when substu.ayp_score < sub.min_grad_pass_score then substu.ayp_subject_id end) > 1
            on duplicate key update
                        min_grad_pass_score = values(min_grad_pass_score)
                        ,school_year_id = values(school_year_id)
                        ,grad_exempt_flag = values(grad_exempt_flag)
                        ,ayp_score = values(ayp_score)
                        ,sub_tested_flag = values(sub_tested_flag)
                        ,sub_passed_flag = values(sub_passed_flag)
                        ,sub_failed_flag = values(sub_failed_flag);
                        
            update c_grad_subject_student_projects
                    set projects_required = 0
                    ;

            insert into c_grad_subject_student_projects (
                        student_id
                        ,ayp_subject_id
                        ,projects_required
                        ,create_timestamp
                        ,last_user_id
            )
                        
            select      dt.student_id
                        ,dt.ayp_subject_id
                        ,gsrp.num_projects
                        ,now() as create_datetime
                        ,1234 as last_user_id
            from tmp_grad_req_proj dt
            left join   c_grad_subject_required_projects gsrp
                    on      dt.ayp_subject_id = gsrp.ayp_subject_id
                    and     dt.ayp_score between gsrp.min_score and gsrp.max_score
                    and     dt.school_year_id between gsrp.begin_year and gsrp.end_year
            where dt.sub_passed_flag = 0
            on duplicate key update
                        projects_required = values(projects_required)
                        ,last_user_id = values(last_user_id)
           ;

            ## delete records where students required projects, but have since passed the test
            delete from c_grad_subject_student_projects
            where projects_required = 0;
   
        # Cleanup and post processing
        
            drop table if exists tmp_grad_req_proj;
    
end proc;
//
