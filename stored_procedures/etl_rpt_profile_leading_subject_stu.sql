/*
$Rev: 8471 $ 
$Author: randall.stanley $ 
$Date: 2010-04-29 16:00:11 -0400 (Thu, 29 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_profile_leading_subject_stu.sql $
$Id: etl_rpt_profile_leading_subject_stu.sql 8471 2010-04-29 20:00:11Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_rpt_profile_leading_subject_stu //

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_profile_leading_subject_stu`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 8471 $ $Date: 2010-04-29 16:00:11 -0400 (Thu, 29 Apr 2010) $'

BEGIN

    truncate table rpt_profile_leading_subject_stu;
    truncate table rpt_profile_leading_subject_stu_test_detail;
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    insert  rpt_profile_leading_subject_stu_test_detail (
        ayp_subject_id
        ,student_id
        ,admin_period_id
        ,test_id
        ,pe
        ,pp
        ,include_wavg_calc_flag
    )
    
    select  ac2.ayp_curriculum_id AS ayp_subject_id
           ,er.student_id
           ,te.admin_period_id
           ,te.test_id
           ,round(sum(er.rubric_value), 3) as pe
           ,round(sum(r.rubric_total)) as pp
           ,max(te.include_wavg_calc_flag) AS include_wavg_calc_flag
    from    sam_test as t
    join    sam_test_event as te
            on      te.test_id = t.test_id
            and     te.purge_flag = 0
    join    sam_test_admin_period as tap
            on      te.admin_period_id = tap.admin_period_id
    join    sam_answer_key as ak
            on      t.test_id = ak.test_id
    join    sam_rubric as r
            on      ak.rubric_id = r.rubric_id
            and     r.rubric_total != 0
    join    sam_alignment_list as al
            on      ak.test_id = al.test_id
            and     ak.test_question_id = al.test_question_id
    join    sam_ayp_curriculum as ac
            on      al.curriculum_id = ac.ayp_curriculum_id
    join    sam_ayp_curriculum as ac2
            on      ac.lft between ac2.lft and ac2.rgt
            and     ac2.level = 'subject'
    join    sam_student_response as er
            on      ak.test_id = er.test_id
            and     ak.test_question_id = er.test_question_id
            and     te.test_event_id = er.test_event_id
    join    c_student as st
            on      st.student_id = er.student_id
            and     st.active_flag = 1
    where   t.owner_id = t.client_id
    and     t.purge_flag = 0
    group by ac2.ayp_curriculum_id
       ,er.student_id
       ,te.admin_period_id
       ,te.test_id
    ;
    
    # calc wtavg values
    insert  rpt_profile_leading_subject_stu_test_detail (
        ayp_subject_id
        ,student_id
        ,admin_period_id
        ,test_id
        ,pe
        ,pp
        ,include_wavg_calc_flag
    )
    
    select  ayp_subject_id
        ,student_id
        ,0
        ,0
        ,sum(pe)
        ,sum(pp)
        ,1
    from    rpt_profile_leading_subject_stu_test_detail
    where   include_wavg_calc_flag = 1
    group by  ayp_subject_id, student_id
    ;
    
    # calc admin period avg values
    insert  rpt_profile_leading_subject_stu_test_detail (
        ayp_subject_id
        ,student_id
        ,admin_period_id
        ,test_id
        ,pe
        ,pp
        ,include_wavg_calc_flag
    )
    
    select  ayp_subject_id
        ,student_id
        ,admin_period_id
        ,0
        ,sum(pe)
        ,sum(pp)
        ,max(include_wavg_calc_flag) as include_wavg_calc_flag
    from    rpt_profile_leading_subject_stu_test_detail
    where   admin_period_id != 0
    group by  ayp_subject_id, student_id, admin_period_id
    ;
    
    INSERT INTO rpt_profile_leading_subject_stu (
        ayp_subject_id
        ,student_id 
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
    #    ,total_pe
    #    ,total_pp
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
    select  dt2.ayp_subject_id
        ,dt2.student_id
        ,dt2.bm_01_pe
        ,dt2.bm_01_pp
        ,dt2.bm_02_pe
        ,dt2.bm_02_pp
        ,dt2.bm_03_pe
        ,dt2.bm_03_pp
        ,dt2.bm_04_pe
        ,dt2.bm_04_pp
        ,dt2.bm_05_pe
        ,dt2.bm_05_pp
        ,dt2.bm_06_pe
        ,dt2.bm_06_pp
        ,dt2.bm_07_pe
        ,dt2.bm_07_pp
        ,dt2.bm_08_pe
        ,dt2.bm_08_pp
    #    ,dt2.total_pe
    #    ,dt2.total_pp
        ,dt2.bm_01_color
        ,dt2.bm_02_color
        ,dt2.bm_03_color
        ,dt2.bm_04_color
        ,dt2.bm_05_color
        ,dt2.bm_06_color
        ,dt2.bm_07_color
        ,dt2.bm_08_color
        ,1234
    from (
            select   dt.ayp_subject_id
                ,dt.student_id
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
    #            ,sum(CASE WHEN dt.include_wavg_calc_flag = 1 THEN (coalesce(dt.pe, 0)) END) as total_pe
    #            ,sum(CASE WHEN dt.include_wavg_calc_flag = 1 THEN dt.pp end) as total_pp
                ,min(case when dt.sequence = 1 then c.moniker end) as bm_01_color
                ,min(case when dt.sequence = 2 then c.moniker end) as bm_02_color
                ,min(case when dt.sequence = 3 then c.moniker end) as bm_03_color
                ,min(case when dt.sequence = 4 then c.moniker end) as bm_04_color
                ,min(case when dt.sequence = 5 then c.moniker end) as bm_05_color
                ,min(case when dt.sequence = 6 then c.moniker end) as bm_06_color
                ,min(case when dt.sequence = 7 then c.moniker end) as bm_07_color
                ,min(case when dt.sequence = 8 then c.moniker end) as bm_08_color
           
        from     (
                    select  td.ayp_subject_id
                        ,td.student_id
                        ,tap.sequence
    #                    MAX(te.include_wavg_calc_flag) AS include_wavg_calc_flag
                        ,sum(td.pe) as pe
                        ,sum(td.pp) as pp
    
            from    rpt_profile_leading_subject_stu_test_detail as td
            join    sam_test_admin_period as tap
                    on      td.admin_period_id = tap.admin_period_id
            where   td.test_id != 0
            group by td.ayp_subject_id,
               td.student_id,
               tap.sequence
            ) dt   
        join     c_color_ayp_benchmark cb
                 on    cb.ayp_subject_id = dt.ayp_subject_id
                 and   round(coalesce(dt.pe, 0) / dt.pp * 100, 0) between cb.min_score and cb.max_score
        join     pmi_color c
                 on    cb.color_id = c.color_id
        group by dt.ayp_subject_id,
           dt.student_id
        ) as dt2
    ON DUPLICATE KEY UPDATE last_user_id = 1234
       ,bm_01_pe = values(bm_01_pe)
       ,bm_01_pp = values(bm_01_pp)
       ,bm_02_pe = values(bm_02_pe)
       ,bm_02_pp = values(bm_02_pp)
       ,bm_03_pe = values(bm_03_pe)
       ,bm_03_pp = values(bm_03_pp)
       ,bm_04_pe = values(bm_04_pe)
       ,bm_04_pp = values(bm_04_pp)
       ,bm_05_pe = values(bm_05_pe)
       ,bm_05_pp = values(bm_05_pp)
       ,bm_06_pe = values(bm_06_pe)
       ,bm_06_pp = values(bm_06_pp)
       ,bm_07_pe = values(bm_07_pe)
       ,bm_07_pp = values(bm_07_pp)
       ,bm_08_pe = values(bm_08_pe)
       ,bm_08_pp = values(bm_08_pp)
    #   ,total_pe = dt2.total_pe
    #   ,total_pp = dt2.total_pp 
       ,bm_01_color = values(bm_01_color)
       ,bm_02_color = values(bm_02_color)
       ,bm_03_color = values(bm_03_color)
       ,bm_04_color = values(bm_04_color)
       ,bm_05_color = values(bm_05_color)
       ,bm_06_color = values(bm_06_color)
       ,bm_07_color = values(bm_07_color)
       ,bm_08_color = values(bm_08_color)
    ;
    
    update  rpt_profile_leading_subject_stu as rplss
    join    rpt_profile_leading_subject_stu_test_detail as td
            on      rplss.ayp_subject_id = td.ayp_subject_id
            and     rplss.student_id = td.student_id
            and     td.admin_period_id = 0
            and     td.test_id = 0
    set     rplss.total_pe = td.pe
            ,rplss.total_pp = td.pp
    ;
    
    update  rpt_profile_leading_subject_stu as  rplss
    join    c_color_ayp_benchmark cb
            on      rplss.ayp_subject_id = cb.ayp_subject_id
            and     round(coalesce(rplss.total_pe, 0) / rplss.total_pp * 100, 0) between cb.min_score and cb.max_score
    join    pmi_color c
            on      c.color_id = cb.color_id
    set     rplss.wavg_color = c.moniker
            ,rplss.ayp_on_track_flag = cb.ayp_on_track_flag
    ;
    
        
END;
//
