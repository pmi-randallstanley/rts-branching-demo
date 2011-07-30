/*
$Rev: 9366 $ 
$Author: randall.stanley $ 
$Date: 2010-10-06 11:28:38 -0400 (Wed, 06 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_profile_bm_stu.sql $
$Id: etl_rpt_profile_bm_stu.sql 9366 2010-10-06 15:28:38Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_rpt_profile_bm_stu //

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_profile_bm_stu`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 9366 $ $Date: 2010-10-06 11:28:38 -0400 (Wed, 06 Oct 2010) $'
BEGIN

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    TRUNCATE TABLE rpt_profile_bm_stu;
    
    DROP TABLE IF EXISTS `tmp_rpt_bm_stu_base`;
    CREATE TABLE `tmp_rpt_bm_stu_base` (
      `test_id` int(11) NOT NULL,
      `test_event_id` int(11) NOT NULL,
      `test_question_id` int(11) NOT NULL,
      `ayp_subject_id` int(11) NOT NULL,
      `ayp_strand_id` int(11) NOT NULL,
      `ayp_curriculum_id` int(11) NOT NULL,
      `include_wavg_calc_flag` tinyint(1) NOT NULL,
      `rubric_total` decimal(6,3) default NULL,
      `sequence` tinyint(4) NOT NULL,
      `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
      PRIMARY KEY  (`test_id`,`test_event_id`,`test_question_id`,`ayp_subject_id`,`ayp_strand_id`,`ayp_curriculum_id`),
      KEY `ind_tmp_rpt_bm_stu_base` (`ayp_subject_id`,`ayp_strand_id`,`ayp_curriculum_id`,`sequence`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1
    ;
    
    insert tmp_rpt_bm_stu_base (
        test_id
        ,test_event_id
        ,test_question_id
        ,ayp_subject_id
        ,ayp_strand_id
        ,ayp_curriculum_id
        ,include_wavg_calc_flag
        ,rubric_total
        ,sequence
    )
    
    SELECT  te.test_id
        ,te.test_event_id
        ,ak.test_question_id
        ,ac2.ayp_curriculum_id as ayp_subject_id
        ,ac3.ayp_curriculum_id as ayp_strand_id
        ,ac.ayp_curriculum_id
        ,MIN(te.include_wavg_calc_flag) as include_wavg_calc_flag
        ,MIN(r.rubric_total) AS rubric_total
        ,MIN(tap.sequence) AS sequence
    
    FROM    sam_test t
    JOIN    sam_test_event AS te
            ON    te.test_id = t.test_id
    JOIN     sam_test_admin_period tap
            ON    te.admin_period_id = tap.admin_period_id
    JOIN     sam_answer_key ak
            ON    t.test_id = ak.test_id
    JOIN     sam_rubric r
            ON    ak.rubric_id = r.rubric_id
            AND   r.rubric_total != 0
    JOIN     sam_alignment_list al
            ON    ak.test_id = al.test_id
            AND   ak.test_question_id = al.test_question_id
    JOIN     sam_ayp_curriculum AS ac
            ON    al.curriculum_id = ac.ayp_curriculum_id
    JOIN     sam_ayp_curriculum AS ac2
            ON    ac.lft BETWEEN ac2.lft AND ac2.rgt
            AND   ac2.level = 'subject'
    JOIN     sam_ayp_curriculum AS ac3
            ON    ac.lft BETWEEN ac3.lft AND ac3.rgt
            AND   ac3.level = 'strand'
    WHERE   t.owner_id = t.client_id
    AND     t.purge_flag = 0
    AND     te.purge_flag = 0
    GROUP BY te.test_id
        ,te.test_event_id
        ,ak.test_question_id
        ,ac2.ayp_curriculum_id
        ,ac3.ayp_curriculum_id
        ,ac.ayp_curriculum_id
    ;
    
    
    INSERT INTO rpt_profile_bm_stu (
        ayp_subject_id
        ,ayp_strand_id
        ,student_id
        ,curriculum_id
        ,bm_01_pe
        ,bm_01_pp
        ,bm_02_pe
        ,bm_02_pp
        ,bm_03_pe
        ,bm_03_pp
        ,bm_04_pe
        ,bm_04_pp
        ,bm_05_pe
        ,bm_05_pp
        ,bm_06_pe
        ,bm_06_pp
        ,bm_07_pe
        ,bm_07_pp
        ,bm_08_pe
        ,bm_08_pp
        ,total_pe
        ,total_pp
        ,bm_01_color
        ,bm_02_color
        ,bm_03_color
        ,bm_04_color
        ,bm_05_color
        ,bm_06_color
        ,bm_07_color
        ,bm_08_color
        ,last_user_id
    ) 
    SELECT  dt.ayp_subject_id
        ,dt.ayp_strand_id
        ,dt.student_id
        ,dt.ayp_curriculum_id
        ,min(case when dt.sequence = 1 then dt.pe end) as bm_01_pe
        ,min(case when dt.sequence = 1 then dt.pp end) as bm_01_pp
        ,min(case when dt.sequence = 2 then dt.pe end) as bm_02_pe
        ,min(case when dt.sequence = 2 then dt.pp end) as bm_02_pp
        ,min(case when dt.sequence = 3 then dt.pe end) as bm_03_pe
        ,min(case when dt.sequence = 3 then dt.pp end) as bm_03_pp
        ,min(case when dt.sequence = 4 then dt.pe end) as bm_04_pe
        ,min(case when dt.sequence = 4 then dt.pp end) as bm_04_pp
        ,min(case when dt.sequence = 5 then dt.pe end) as bm_05_pe
        ,min(case when dt.sequence = 5 then dt.pp end) as bm_05_pp
        ,min(case when dt.sequence = 6 then dt.pe end) as bm_06_pe
        ,min(case when dt.sequence = 6 then dt.pp end) as bm_06_pp
        ,min(case when dt.sequence = 7 then dt.pe end) as bm_07_pe
        ,min(case when dt.sequence = 7 then dt.pp end) as bm_07_pp
        ,min(case when dt.sequence = 8 then dt.pe end) as bm_08_pe
        ,min(case when dt.sequence = 8 then dt.pp end) as bm_08_pp
        ,sum(coalesce(case when dt.include_wavg_calc_flag = 1 then dt.pe end, 0)) as total_pe
        ,sum(case when dt.include_wavg_calc_flag = 1 then dt.pp end) as total_pp
        ,min(case when dt.sequence = 1 then c.moniker end) as bm_01_color
        ,min(case when dt.sequence = 2 then c.moniker end) as bm_02_color
        ,min(case when dt.sequence = 3 then c.moniker end) as bm_03_color
        ,min(case when dt.sequence = 4 then c.moniker end) as bm_04_color
        ,min(case when dt.sequence = 5 then c.moniker end) as bm_05_color
        ,min(case when dt.sequence = 6 then c.moniker end) as bm_06_color
        ,min(case when dt.sequence = 7 then c.moniker end) as bm_07_color
        ,min(case when dt.sequence = 8 then c.moniker end) as bm_08_color
        ,1234
            
        FROM    (
                                SELECT   tmp1.ayp_subject_id
                                    ,tmp1.ayp_strand_id
                                    ,tmp1.ayp_curriculum_id
                                    ,tmp1.sequence
                                    ,er.student_id
                                    ,tmp1.include_wavg_calc_flag
                                    ,CAST(SUM(er.rubric_value) AS DECIMAL(9,3)) AS pe
                                    ,SUM(tmp1.rubric_total) AS pp
                                    
                                FROM    tmp_rpt_bm_stu_base AS tmp1
                                JOIN    sam_student_response er
                                                ON    tmp1.test_id = er.test_id
                                                AND   tmp1.test_event_id = er.test_event_id
                                                AND   tmp1.test_question_id = er.test_question_id
        
                                GROUP BY tmp1.ayp_subject_id
                                    ,tmp1.ayp_strand_id
                                    ,tmp1.ayp_curriculum_id
                                    ,tmp1.sequence
                                    ,er.student_id
                        ) AS dt   
        JOIN     c_color_ayp_benchmark cb
                        ON    cb.ayp_subject_id = dt.ayp_subject_id
                        AND   ROUND(COALESCE(dt.pe, 0) / dt.pp * 100, 0) BETWEEN cb.min_score AND cb.max_score
        JOIN     pmi_color c
                        ON    cb.color_id = c.color_id
                        
        GROUP BY dt.ayp_subject_id
            ,dt.ayp_strand_id
            ,dt.student_id
            ,dt.ayp_curriculum_id
    ;
    
    
    UPDATE   rpt_profile_bm_stu AS  rpbms
    JOIN     c_color_ayp_benchmark cb
             ON    rpbms.ayp_subject_id = cb.ayp_subject_id
             AND   ROUND(COALESCE(rpbms.total_pe, 0) / rpbms.total_pp * 100, 0) BETWEEN cb.min_score AND cb.max_score
    JOIN     pmi_color c
             ON    c.color_id = cb.color_id
    SET rpbms.wavg_color = c.moniker
    ;


DROP TABLE IF EXISTS `tmp_rpt_bm_stu_base`;
    
END;
//
