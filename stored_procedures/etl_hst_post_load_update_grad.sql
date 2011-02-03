/*
$Rev: 8513 $ 
$Author: randall.stanley $ 
$Date: 2010-05-05 13:03:34 -0400 (Wed, 05 May 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_hst_post_load_update_grad.sql $
$Id: etl_hst_post_load_update_grad.sql 8513 2010-05-05 17:03:34Z randall.stanley $ 
 */

drop procedure if exists etl_hst_post_load_update_grad//

create definer=`dbadmin`@`localhost` procedure etl_hst_post_load_update_grad()
contains sql
sql security invoker
comment '$Rev: 8513 $ $Date: 2010-05-05 13:03:34 -0400 (Wed, 05 May 2010) $'


proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    drop table if exists `tmp_grad_status`;
    drop table if exists `tmp_test_type_grad_info`;
    
    CREATE TABLE `tmp_grad_status` (
      `student_id` int(10) not null,
      `ayp_subject_id` int(10) not null,
      `ayp_test_type_id` int(10) not null,
      `grad_exempt_flag` tinyint(1) not null default '0',
      `ayp_score` decimal(9,3) default null,
      `req_proj_flag` tinyint(1) not null default '0',
      `meet_req_proj_flag` tinyint(1) not null default '0',
      `sub_tested_flag` tinyint(4) not null default '0',
      `sub_passed_flag` tinyint(4) not null default '0',
      `sub_failed_cnt` tinyint(4) not null default '0',
      PRIMARY KEY  (`student_id`,`ayp_subject_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1
    ;

    CREATE TABLE `tmp_test_type_grad_info` (
      `ayp_test_type_id` int(10) NOT NULL,
      `min_grad_comp_score` decimal(9,3) default NULL,
      `test_type_grad_sub_cnt` tinyint(4) default NULL,
      `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
      PRIMARY KEY (`ayp_test_type_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1
    ;

    select  min(case when grad_status_code = 'passedall' then grad_status_id end)
            ,min(case when grad_status_code = 'combined' then grad_status_id end)
            ,min(case when grad_status_code = 'bridgePlan' then grad_status_id end)
            ,min(case when grad_status_code = 'didnotmeet' then grad_status_id end)
    
    into    @passed_all_id
            ,@combined_id
            ,@bridge_plan_id
            ,@did_not_meet_id
    
    from    c_grad_status
    where   grad_status_code in ('passedall','combined','bridgePlan','didnotmeet')
    ;


    insert tmp_test_type_grad_info (
        ayp_test_type_id
        ,min_grad_comp_score
        ,test_type_grad_sub_cnt
    )
    select  tt.ayp_test_type_id
        ,min(tt.min_grad_comp_score) as min_grad_comp_score
        ,count(distinct sub.ayp_subject_id) as test_type_grad_sub_cnt
    from    c_ayp_test_type as tt
    join    c_ayp_subject as sub
            on      tt.ayp_test_type_id = sub.ayp_test_type_id
            and     sub.grad_report_flag = 1
    group by tt.ayp_test_type_id
    ;

    insert into tmp_grad_status (
        student_id 
        ,ayp_subject_id
        ,ayp_test_type_id
        ,grad_exempt_flag
        ,ayp_score 
        ,req_proj_flag
        ,meet_req_proj_flag 
        ,sub_tested_flag
        ,sub_passed_flag
        ,sub_failed_cnt
    )
    
    select stu.student_id
        ,coalesce(substu.ayp_subject_id,99999999) as ayp_subject_id
        ,min(sub.ayp_test_type_id) as ayp_test_type_id
        ,max(stu.grad_exempt_flag) as grad_exempt_flag
        ,max(substu.ayp_score) as ayp_score
        ,case when max(gssp.projects_required) > 0 then 1 else 0 end as req_proj_flag
        ,case when max(gssp.projects_completed) >= max(gssp.projects_required) then 1 else 0 end as meet_req_proj_flag
        ,case when max(substu.ayp_score) is not null then 1 else 0 end as sub_tested_flag
        ,case when max(substu.ayp_score) >= sub.min_grad_pass_score then 1 else 0 end as sub_passed_flag
        ,count(case when substu.ayp_score < sub.min_grad_pass_score then substu.ayp_subject_id end) as sub_failed_cnt
    from    c_student stu
    cross join c_ayp_subject as sub
            on      sub.grad_report_flag = 1
    left join c_grad_subject_student_projects as gssp
            on      stu.student_id = gssp.student_id
            and     sub.ayp_subject_id = gssp.ayp_subject_id
    left join c_ayp_subject_student as substu
            on      stu.student_id = substu.student_id
            and     sub.ayp_subject_id = substu.ayp_subject_id
    where   stu.grad_eligible_flag = 1
    group by stu.student_id, substu.ayp_subject_id
    on duplicate key update grad_exempt_flag = values(grad_exempt_flag)
        ,ayp_score  = values(ayp_score)
        ,req_proj_flag  = values(req_proj_flag)
        ,meet_req_proj_flag  = values(meet_req_proj_flag)
        ,sub_tested_flag = values(sub_tested_flag)
        ,sub_passed_flag = values(sub_passed_flag)
        ,sub_failed_cnt = values(sub_failed_cnt)
    ;



    update  c_student as s
    join    (
                select  t1.student_id
                    ,t1.ayp_test_type_id
                    ,sum(t1.ayp_score) as comp_score
                    ,sum(t1.sub_tested_flag) as total_subs_tested_cnt
                    ,sum(t1.sub_passed_flag) as total_subs_passed_cnt
                    ## modified added bridge_plan_sub_eligible_cnt mt 20100303     
                    ,sum(case when t1.sub_failed_cnt >= 2 and t1.sub_passed_flag = 0 then 1 else 0 end) as bridge_plan_sub_eligible_cnt
                    ,max(case when t1.sub_failed_cnt >= 2 and t1.sub_passed_flag = 0 then 1 else 0 end) as bridge_plan_eligible_flag
                    ,case when sum(t1.meet_req_proj_flag) > 0 and sum(t1.meet_req_proj_flag) >= sum(t1.req_proj_flag) then 1 else 0 end as met_bridge_plan_flag
                from    tmp_grad_status as t1
                group by t1.student_id
                
            ) as dt
            on      s.student_id = dt.student_id
    join    tmp_test_type_grad_info as tmp1
            on      tmp1.ayp_test_type_id = dt.ayp_test_type_id
    set     s.all_grad_subs_tested_flag = case when dt.total_subs_tested_cnt < tmp1.test_type_grad_sub_cnt then 0 else 1 end
            ,s.grad_reqs_met_flag = case when s.grad_exempt_flag = 1 then 1 
                                        when dt.comp_score >= tmp1.min_grad_comp_score and dt.total_subs_tested_cnt >= tmp1.test_type_grad_sub_cnt then 1
                                        else 0 
                                    end
            ,s.grad_comp_score = case when dt.comp_score < tmp1.min_grad_comp_score then (dt.comp_score - tmp1.min_grad_comp_score) else dt.comp_score end
            ,s.bridge_plan_eligible_flag = case when s.grad_exempt_flag = 0 then dt.bridge_plan_eligible_flag else 0 end
            ,s.passed_all_grad_subs_flag = case when dt.total_subs_passed_cnt = tmp1.test_type_grad_sub_cnt then 1 else 0 end
            ## modified grad_status_id mt 20100303
            ,s.grad_status_id =  case   when dt.total_subs_passed_cnt = tmp1.test_type_grad_sub_cnt then @passed_all_id
                                        when s.grad_exempt_flag = 1 
                                            or (dt.comp_score >= tmp1.min_grad_comp_score and dt.total_subs_tested_cnt >= tmp1.test_type_grad_sub_cnt) then @combined_id
                                        when s.grad_exempt_flag = 0 
                                             and dt.total_subs_passed_cnt + dt.bridge_plan_sub_eligible_cnt = tmp1.test_type_grad_sub_cnt 
                                             and dt.bridge_plan_eligible_flag = 1 and dt.met_bridge_plan_flag = 1 then @bridge_plan_id
                                        else @did_not_meet_id
                                end 
    ;

    #Clean-up and post processing
    drop table if exists `tmp_grad_status`;
    drop table if exists `tmp_test_type_grad_info`;

end proc;
//
