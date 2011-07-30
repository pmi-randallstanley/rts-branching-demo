/*
$Rev: 9366 $ 
$Author: randall.stanley $ 
$Date: 2010-10-06 11:28:38 -0400 (Wed, 06 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bm_summ_ayp_stu_period.sql $
$Id: etl_rpt_bm_summ_ayp_stu_period.sql 9366 2010-10-06 15:28:38Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_rpt_bm_summ_ayp_stu_period//

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_bm_summ_ayp_stu_period()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 9366 $ $Date: 2010-10-06 11:28:38 -0400 (Wed, 06 Oct 2010) $'

# Agg data used in Exec Dir Curriculum Report

PROC: BEGIN 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    drop table if exists `tmp_test_question_strand`;
    create table `tmp_test_question_strand` (
      `test_id` int(10) not null,
      `test_question_id` int(10) not null,
      `ayp_subject_id` int(10) not null,
      `ayp_strand_id` int(10) not null,
      `rubric_total` float null,
      primary key (`test_id`,`test_question_id`,`ayp_subject_id`,`ayp_strand_id`)
    ) engine=innodb default charset=latin1;
      
    truncate table rpt_bm_summ_ayp_strand_stu_period;
    truncate table rpt_bm_summ_ayp_subject_stu_period;
    
    # Gather Answer Key info for calculations
    insert tmp_test_question_strand (test_id, test_question_id, ayp_subject_id, ayp_strand_id, rubric_total)
    select   t.test_id
        ,ak.test_question_id
       ,acsub.ayp_curriculum_id as ayp_subject_id
       ,acstr.ayp_curriculum_id as ayp_strand_id
       ,min(round(r.rubric_total, 3)) as rubric_total
    
    from    sam_test t
    join    sam_answer_key ak
            on    ak.test_id = t.test_id         
    join    sam_rubric r
            on    r.rubric_id = ak.rubric_id
            and   r.rubric_total != 0
    join    sam_test_event te
            on    te.test_id = t.test_id
            and   te.purge_flag = 0
    join    sam_alignment_list al
            on    ak.test_id = al.test_id
            and   ak.test_question_id = al.test_question_id
    join    sam_ayp_curriculum ac
            on    ac.ayp_curriculum_id = al.curriculum_id
    join    sam_ayp_curriculum acsub
            on    ac.lft between acsub.lft and acsub.rgt
            and   acsub.level = 'subject'
    join    sam_ayp_curriculum acstr
            on    ac.lft between acstr.lft and acstr.rgt
            and   acstr.level = 'strand'
    where   t.owner_id = t.client_id
    and     t.purge_flag = 0
    group by t.test_id, ak.test_question_id, acsub.ayp_curriculum_id, acstr.ayp_curriculum_id
    ;
    
    # Agg student performance at strand/period grouping
    insert into rpt_bm_summ_ayp_strand_stu_period (
       student_id, 
       ayp_subject_id,
       ayp_strand_id,
       admin_period_id,
       pe, 
       pp   
    ) 
    
    select   er.student_id,
       tmp1.ayp_subject_id,
       tmp1.ayp_strand_id,
        te.admin_period_id,
       sum(cast(er.rubric_value as decimal(13,3))),
       sum(cast(tmp1.rubric_total as decimal(13,3)))
    from    tmp_test_question_strand as tmp1
    join    sam_test_event te
            on      tmp1.test_id = te.test_id
            and     te.purge_flag = 0
            and     te.admin_period_id is not null
    join    sam_student_response as er
            on      te.test_id = er.test_id
            and     te.test_event_id = er.test_event_id
            and     tmp1.test_question_id = er.test_question_id
    group by er.student_id, tmp1.ayp_subject_id, tmp1.ayp_strand_id, te.admin_period_id;
    
    # Agg student performance at strand for wt avg 
    insert into rpt_bm_summ_ayp_strand_stu_period (
       student_id, 
       ayp_subject_id,
       ayp_strand_id,
       admin_period_id,
       pe, 
       pp   
    ) 
    
    select   er.student_id,
       tmp1.ayp_subject_id,
       tmp1.ayp_strand_id,
       0,
       sum(cast(er.rubric_value as decimal(13,3))),
       sum(cast(tmp1.rubric_total as decimal(13,3)))
    from    tmp_test_question_strand as tmp1
    join    sam_test_event te
            on      tmp1.test_id = te.test_id
            and     te.purge_flag = 0
            and     te.admin_period_id is not null
            and     te.include_wavg_calc_flag = 1
    join    sam_student_response as er
            on      te.test_id = er.test_id
            and     te.test_event_id = er.test_event_id
            and     tmp1.test_question_id = er.test_question_id
    group by er.student_id, tmp1.ayp_subject_id, tmp1.ayp_strand_id;

    
    
    # Agg student performance at subject level for all periods including wt avg 
    insert into rpt_bm_summ_ayp_subject_stu_period (
       student_id, 
       ayp_subject_id,
       admin_period_id,
       pe, 
       pp   
    ) 
    
    select  tmp1.student_id
        ,tmp1.ayp_subject_id
        ,tmp1.admin_period_id
        ,sum(tmp1.pe) as pe
        ,sum(tmp1.pp) as pp
        
    from    rpt_bm_summ_ayp_strand_stu_period as tmp1
    group by tmp1.student_id, tmp1.ayp_subject_id, tmp1.admin_period_id;

    drop table if exists `tmp_test_question_strand`;

END PROC;
//
