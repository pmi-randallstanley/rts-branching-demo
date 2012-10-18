/*
$Rev: 9366 $ 
$Author: randall.stanley $ 
$Date: 2010-10-06 11:28:38 -0400 (Wed, 06 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bm_scores.sql $
$Id: etl_rpt_bm_scores.sql 9366 2010-10-06 15:28:38Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_rpt_bm_scores//

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_bm_scores`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 9366 $ $Date: 2010-10-06 11:28:38 -0400 (Wed, 06 Oct 2010) $'
BEGIN
      
    DECLARE no_more_rows BOOLEAN; 
    DECLARE v_test_id int;

    DECLARE cur_1 CURSOR FOR 
        select  test_id 
        from    sam_test as t
        where   t.owner_id = t.client_id
        and exists(select 1 from sam_student_response ter where t.test_id = ter.test_id)
        group by t.test_id; 
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND 
    SET no_more_rows = TRUE;

    truncate table rpt_bm_scores;

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    drop table if exists tmp_rpt_bm_scores;
    create table tmp_rpt_bm_scores (
      `test_id` int(10) NOT NULL,
      `test_question_id` int(10) NOT NULL,
      `curriculum_id` int(10) NOT NULL,
      `ayp_subject_id` int(10) NOT NULL,
      `ayp_strand_id` int(10) NOT NULL,
      `rubric_total` float NULL,
      `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
      PRIMARY KEY (`test_id`,`test_question_id`,`curriculum_id`,`ayp_subject_id`,`ayp_strand_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;


    OPEN cur_1;

    loop_cur_1: LOOP
            
        FETCH  cur_1 
        INTO   v_test_id;

        IF no_more_rows THEN
            CLOSE cur_1;
            LEAVE loop_cur_1;
        END IF;
   
        insert tmp_rpt_bm_scores (test_id, test_question_id, curriculum_id, ayp_subject_id, ayp_strand_id, rubric_total)
        SELECT   t.test_id
           ,ak.test_question_id
           ,al.curriculum_id
           ,acsub.ayp_curriculum_id AS ayp_subject_id
           ,acstr.ayp_curriculum_id AS ayp_strand_id
           ,MIN(ROUND(r.rubric_total, 3)) as rubric_total    
        FROM     sam_test t
        JOIN     sam_answer_key ak
             ON    ak.test_id = t.test_id         
        JOIN     sam_rubric r
             ON    r.rubric_id = ak.rubric_id
             AND   r.rubric_total != 0
        JOIN     sam_test_event te
             ON    te.test_id = t.test_id
        JOIN     sam_alignment_list al
             ON    ak.test_id = al.test_id
             and   ak.test_question_id = al.test_question_id
        JOIN     sam_ayp_curriculum ac
             ON    ac.ayp_curriculum_id = al.curriculum_id
        JOIN     sam_ayp_curriculum acsub
             ON    ac.lft BETWEEN acsub.lft AND acsub.rgt
             AND   acsub.level = 'subject'
        JOIN     sam_ayp_curriculum acstr
             ON    ac.lft BETWEEN acstr.lft AND acstr.rgt
             AND   acstr.level = 'strand'
        WHERE    t.test_id = v_test_id
             AND   t.purge_flag = 0
             AND   te.purge_flag = 0
        GROUP BY t.test_id, ak.test_question_id, al.curriculum_id, acsub.ayp_curriculum_id, acstr.ayp_curriculum_id
        ;

        INSERT INTO rpt_bm_scores (
           student_id
           ,test_id
           ,curriculum_id
           ,ayp_subject_id
           ,ayp_strand_id
           ,points_earned
           ,points_possible
           ,last_user_id
        ) 
        SELECT  er.student_id
           ,tmp1.test_id
           ,tmp1.curriculum_id
           ,tmp1.ayp_subject_id
           ,tmp1.ayp_strand_id
           ,coalesce(SUM(ROUND(er.rubric_value, 3)),0)
           ,SUM(ROUND(tmp1.rubric_total, 3))
           ,1234
        from    tmp_rpt_bm_scores as tmp1
        join    sam_test_event as te
            on      tmp1.test_id = te.test_id
            and     te.purge_flag = 0
        join    sam_student_response as er
            on      te.test_id = er.test_id
            and     te.test_event_id = er.test_event_id
            and     tmp1.test_question_id = er.test_question_id
        join    c_student as st
            on      st.student_id = er.student_id
            and     st.active_flag = 1
        GROUP BY er.student_id, tmp1.test_id, tmp1.curriculum_id, tmp1.ayp_subject_id, tmp1.ayp_strand_id
        ;
 
        truncate table tmp_rpt_bm_scores;

    END LOOP loop_cur_1;

    drop table if exists tmp_rpt_bm_scores; 

END;
//
