/*
$Rev: 7459 $ 
$Author: randall.stanley $ 
$Date: 2009-07-28 09:46:25 -0400 (Tue, 28 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_ayp_accel_summ_tables.sql $
$Id: etl_rpt_ayp_accel_summ_tables.sql 7459 2009-07-28 13:46:25Z randall.stanley $ 
*/


DROP PROCEDURE IF EXISTS etl_rpt_ayp_accel_summ_tables //

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_ayp_accel_summ_tables()
COMMENT '$Rev: 7459 $ $Date: 2009-07-28 09:46:25 -0400 (Tue, 28 Jul 2009) $'
CONTAINS SQL
SQL SECURITY INVOKER

BEGIN

    select  count(*) 
    into    @view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = 'v_pmi_ods_ayp_safe_harbor'
    ;

    truncate table rpt_nclb_group_enroll_acc_grade;
    truncate TABLE rpt_nclb_group_results_acc_grade;
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    # school level
    insert rpt_nclb_group_enroll_acc_grade (accessor_id, ayp_group_id, grade_level_id, school_year_id
        ,curr_enroll_count, ayp_enroll_count, last_user_id, create_timestamp)
    select  rsg.accessor_id, rsg.ayp_group_id, rsg.grade_level_id, rsg.school_year_id
        ,count(rsg.student_id) as curr_enroll_count, 0, 1234, now()
    from    rpt_student_group as rsg
    join    c_school_year as sy
            on      rsg.school_year_id = sy.school_year_id
            and     sy.active_flag = 1
    group by rsg.accessor_id, rsg.ayp_group_id, rsg.grade_level_id, rsg.school_year_id
    ;

    # school level.  Need to pick up school, group, grade level that have entries in ayp
    #   enrollment, but not in current enrollment.
    insert rpt_nclb_group_enroll_acc_grade (accessor_id, ayp_group_id, grade_level_id, school_year_id
        ,curr_enroll_count, ayp_enroll_count, last_user_id, create_timestamp)
 
    select  rsag.accessor_id, rsag.ayp_group_id, rsag.grade_level_id, rsag.school_year_id
            , 0, 0, 1234, now()
    from        rpt_student_ayp_group rsag
    join        c_school_year as sy
                on      rsag.school_year_id = sy.school_year_id
                and     sy.active_flag = 1
    left join   rpt_nclb_group_enroll_acc_grade rpt
                on      rsag.accessor_id = rpt.accessor_id
                and     rsag.grade_level_id = rpt.grade_level_id
                and     rsag.ayp_group_id = rpt.ayp_group_id
    where       rpt.accessor_id is null
                and     rsag.accessor_id != @client_id   # This is needed b/c some districts have client id in rpt_student_ayp_group, and district insert below will bomb on dup key.
    group by    rsag.accessor_id, rsag.ayp_group_id,rsag.grade_level_id,rsag.school_year_id  
    ;

    update  rpt_nclb_group_enroll_acc_grade as eag
    join    (
                select  rsg.accessor_id, rsg.ayp_group_id, rsg.grade_level_id, rsg.school_year_id
                    ,count(rsg.student_id) as ayp_enroll_count, 0, 1234, now()
                from    rpt_student_ayp_group as rsg
                join    c_school_year as sy
                        on      rsg.school_year_id = sy.school_year_id
                        and     sy.active_flag = 1
                group by rsg.accessor_id, rsg.ayp_group_id, rsg.grade_level_id, rsg.school_year_id
            ) as dt
            on      eag.accessor_id = dt.accessor_id
            and     eag.ayp_group_id = dt.ayp_group_id
            and     eag.grade_level_id = dt.grade_level_id
    set    eag.ayp_enroll_count = dt.ayp_enroll_count
    ;

    # district level
    insert rpt_nclb_group_enroll_acc_grade (accessor_id, ayp_group_id, grade_level_id, school_year_id
        ,curr_enroll_count, ayp_enroll_count, last_user_id, create_timestamp)

    select  @client_id, ayp_group_id, grade_level_id, school_year_id
        ,sum(curr_enroll_count), sum(ayp_enroll_count), 1234, now()
    from    rpt_nclb_group_enroll_acc_grade as src
    group by src.ayp_group_id, src.grade_level_id, src.school_year_id
    ;

    # school level
    insert rpt_nclb_group_results_acc_grade (ayp_group_id, accessor_id, grade_level_id, ayp_subject_id, last_user_id, ayp_year_id)
    SELECT sag.ayp_group_id,
    sag.accessor_id,
    sag.grade_level_id,
    sub.ayp_subject_id,
    1234,
    min(sy.school_year_id) as ayp_year_id
    FROM    rpt_student_group AS sag
    JOIN   c_ayp_group ag
        ON   ag.ayp_group_id = sag.ayp_group_id
        AND  ayp_accel_flag = 1
    JOIN   c_school_year sy
        ON   sy.active_flag = 1
    JOIN   c_student_year csy
        ON   csy.school_year_id = sy.school_year_id
        AND  csy.student_id = sag.student_id
        AND  csy.active_flag = 1
    JOIN   c_student_school_list AS sschl
        ON   sschl.student_id = csy.student_id
        AND  sschl.school_year_id = csy.school_year_id
        AND  sschl.enrolled_school_flag = 1
        AND  sschl.active_flag = 1
    JOIN   c_ayp_subject sub
    JOIN   c_ayp_test_type_year atty
        ON   sub.ayp_test_type_id = sub.ayp_test_type_id
        AND  atty.ayp_reporting_flag = 1
    GROUP BY sag.accessor_id, sub.ayp_subject_id, sag.ayp_group_id, sag.grade_level_id;

    # School level.  Need to pick up school, group, grade level and subject  that have entries in ayp
    #   enrollment, but not in current enrollment.
    insert rpt_nclb_group_results_acc_grade (ayp_group_id, accessor_id, grade_level_id, ayp_subject_id, last_user_id, ayp_year_id)
    SELECT rsag.ayp_group_id
            ,rsag.accessor_id
            ,rsag.grade_level_id
            ,sub.ayp_subject_id
            ,1234
            ,min(sy.school_year_id) as ayp_year_id
    FROM      rpt_student_ayp_group AS rsag
    JOIN      c_ayp_group ag
              ON        ag.ayp_group_id = rsag.ayp_group_id
              AND       ayp_accel_flag = 1
    JOIN      c_school_year sy
              ON        sy.active_flag = 1
    JOIN      c_student_year csy
              ON        csy.school_year_id = sy.school_year_id
              AND       csy.student_id = rsag.student_id
              AND       csy.active_flag = 1
    JOIN      c_student_school_list AS sschl
              ON        sschl.student_id = csy.student_id
              AND       sschl.school_year_id = csy.school_year_id
              AND       sschl.enrolled_school_flag = 1
              AND       sschl.active_flag = 1
    JOIN      c_ayp_subject sub
    JOIN      c_ayp_test_type_year atty
              ON        sub.ayp_test_type_id = sub.ayp_test_type_id
              AND       atty.ayp_reporting_flag = 1
    LEFT JOIN rpt_nclb_group_results_acc_grade rpt
              ON        rsag.accessor_id = rpt.accessor_id
              AND       rsag.grade_level_id = rpt.grade_level_id
              AND       rsag.ayp_group_id = rpt.ayp_group_id
              AND       sub.ayp_subject_id = rpt.ayp_subject_id
    WHERE     rpt.accessor_id is null
    GROUP BY  rsag.accessor_id, sub.ayp_subject_id, rsag.ayp_group_id, rsag.grade_level_id
    on duplicate key update last_user_id = values(last_user_id)
    ;
    
    # district level
    insert rpt_nclb_group_results_acc_grade (ayp_group_id, accessor_id, grade_level_id, ayp_subject_id, last_user_id, ayp_year_id)
    SELECT sag.ayp_group_id,
    @client_id,
    sag.grade_level_id,
    sub.ayp_subject_id,
    1234,
    min(sy.school_year_id) as ayp_year_id
    FROM    rpt_student_group AS sag
    JOIN   c_ayp_group ag
        ON   ag.ayp_group_id = sag.ayp_group_id
        AND  ayp_accel_flag = 1
    JOIN   c_school_year sy
        ON   sy.active_flag = 1
    JOIN   c_student_year csy
        ON   csy.school_year_id = sy.school_year_id
        AND  csy.student_id = sag.student_id
        AND  csy.active_flag = 1
    JOIN   c_student_school_list AS sschl
        ON   sschl.student_id = csy.student_id
        AND  sschl.school_year_id = csy.school_year_id
        AND  sschl.enrolled_school_flag = 1
        AND  sschl.active_flag = 1
    JOIN   c_ayp_subject sub
    JOIN   c_ayp_test_type_year atty
        ON   sub.ayp_test_type_id = sub.ayp_test_type_id
        AND  atty.ayp_reporting_flag = 1
    GROUP BY sub.ayp_subject_id, sag.ayp_group_id, sag.grade_level_id
    on duplicate key update last_user_id = values(last_user_id)
    ;


    # District level.  Need to pick up client, group, grade level and subject that have entries in ayp
    #   enrollment, but not in current enrollment.
    insert rpt_nclb_group_results_acc_grade (ayp_group_id, accessor_id, grade_level_id, ayp_subject_id, last_user_id, ayp_year_id)
    SELECT rsag.ayp_group_id
            ,@client_id
            ,rsag.grade_level_id
            ,sub.ayp_subject_id
            ,1234
            ,min(sy.school_year_id) as ayp_year_id
    FROM      rpt_student_ayp_group AS rsag
    JOIN      c_ayp_group ag
              ON        ag.ayp_group_id = rsag.ayp_group_id
              AND       ayp_accel_flag = 1
    JOIN      c_school_year sy
              ON        sy.active_flag = 1
    JOIN      c_student_year csy
              ON        csy.school_year_id = sy.school_year_id
              AND       csy.student_id = rsag.student_id
              AND       csy.active_flag = 1
    JOIN      c_student_school_list AS sschl
              ON        sschl.student_id = csy.student_id
              AND       sschl.school_year_id = csy.school_year_id
              AND       sschl.enrolled_school_flag = 1
              AND       sschl.active_flag = 1
    JOIN      c_ayp_subject sub
    JOIN      c_ayp_test_type_year atty
              ON        sub.ayp_test_type_id = sub.ayp_test_type_id
              AND       atty.ayp_reporting_flag = 1
    LEFT JOIN rpt_nclb_group_results_acc_grade rpt
              ON        @client_id = rpt.accessor_id
              AND       rsag.grade_level_id = rpt.grade_level_id
              AND       rsag.ayp_group_id = rpt.ayp_group_id
              AND       sub.ayp_subject_id = rpt.ayp_subject_id
    WHERE     rpt.accessor_id is null
    GROUP BY  sub.ayp_subject_id, rsag.ayp_group_id, rsag.grade_level_id
    on duplicate key update last_user_id = values(last_user_id)
    ;

    ##################
    ## Lagging Curr ##
    ##################
    # school level
    UPDATE rpt_nclb_group_results_acc_grade t
        JOIN   (SELECT m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id,
                            COUNT(*) AS cnt,
                            sum(case when  atta.pass_flag = 1 then 1 else 0 end) as passed_cnt,
                            sum(case when  atta.pass_flag = 0 then 1 else 0 end) as failed_cnt
                            FROM    c_ayp_subject_student AS ss
                                    JOIN   c_student_year csy
                                        ON   csy.student_id = ss.student_id
                                        AND  csy.active_flag = 1
                                    JOIN   c_student_school_list AS sschl
                                        ON   sschl.student_id = csy.student_id
                                        AND  sschl.school_year_id = csy.school_year_id
                                        AND  sschl.enrolled_school_flag = 1
                                        AND  sschl.active_flag = 1
                                    JOIN   c_school_year sy
                                        ON   csy.school_year_id = sy.school_year_id
                                        AND  sy.active_flag = 1
                                    JOIN   c_ayp_subject as sub
                                        ON   sub.ayp_subject_id = ss.ayp_subject_id
                                    JOIN   c_ayp_test_type_year atty
                                        ON   sub.ayp_test_type_id = atty.ayp_test_type_id
                                        AND  ss.school_year_id = atty.school_year_id
                                        AND  atty.ayp_reporting_flag = 1
                                    JOIN   c_ayp_test_type_al as atta
                                        ON   atta.ayp_test_type_id = sub.ayp_test_type_id
                                    JOIN   c_ayp_achievement_level as aal
                                        ON   aal.al_id = atta.al_id
                                        AND  aal.al_id = ss.al_id
                                    JOIN   pmi_color AS clr
                                        ON   clr.color_id = atta.color_id
                                    JOIN   rpt_student_group AS sag
                                        ON   sag.student_id = ss.student_id
                                        AND  sag.school_year_id = csy.school_year_id
                                    JOIN   rpt_nclb_group_results_acc_grade m
                                        ON   m.ayp_subject_id = ss.ayp_subject_id
                                        AND  m.ayp_group_id = sag.ayp_group_id
                                        AND  m.grade_level_id = csy.grade_level_id
                                        AND  m.accessor_id = csy.school_id
                            WHERE   ss.al_id IS NOT NULL
                            AND     ss.score_record_flag = 1
                            GROUP BY m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id) dt
            ON   t.ayp_group_id = dt.ayp_group_id
            AND  t.accessor_id = dt.accessor_id
            AND  t.grade_level_id = dt.grade_level_id
            AND  t.ayp_subject_id = dt.ayp_subject_id
    SET t.curr_enrl_lag_group_count = dt.failed_cnt + dt.passed_cnt,
        t.curr_enrl_lag_prof_count = dt.passed_cnt;

    # district level
    UPDATE rpt_nclb_group_results_acc_grade t
        JOIN   (SELECT m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id,
                            COUNT(*) AS cnt,
                            sum(case when  atta.pass_flag = 1 then 1 else 0 end) as passed_cnt,
                            sum(case when  atta.pass_flag = 0 then 1 else 0 end) as failed_cnt
                            FROM    c_ayp_subject_student AS ss
                                    JOIN   c_student_year csy
                                        ON   csy.student_id = ss.student_id
                                        AND  csy.active_flag = 1
                                    JOIN   c_school_year sy
                                        ON   csy.school_year_id = sy.school_year_id
                                        AND  sy.active_flag = 1
                                    JOIN   c_ayp_subject as sub
                                        ON   sub.ayp_subject_id = ss.ayp_subject_id
                                    JOIN   c_ayp_test_type_year atty
                                        ON   sub.ayp_test_type_id = atty.ayp_test_type_id
                                        AND  ss.school_year_id = atty.school_year_id
                                        AND  atty.ayp_reporting_flag = 1
                                    JOIN   c_ayp_test_type_al as atta
                                        ON   atta.ayp_test_type_id = sub.ayp_test_type_id
                                    JOIN   c_ayp_achievement_level as aal
                                        ON   aal.al_id = atta.al_id
                                        AND  aal.al_id = ss.al_id
                                    JOIN   pmi_color AS clr
                                        ON   clr.color_id = atta.color_id
                                    JOIN   rpt_student_group AS sag
                                        ON   sag.student_id = ss.student_id
                                        AND  sag.school_year_id = csy.school_year_id
                                    JOIN   rpt_nclb_group_results_acc_grade m
                                        ON   m.ayp_subject_id = ss.ayp_subject_id
                                        AND  m.ayp_group_id = sag.ayp_group_id
                                        AND  m.grade_level_id = csy.grade_level_id
                                        AND  m.accessor_id = @client_id
                            WHERE   ss.al_id IS NOT NULL
                            AND     ss.score_record_flag = 1
                            GROUP BY m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id) dt
            ON   t.ayp_group_id = dt.ayp_group_id
            AND  t.accessor_id = dt.accessor_id
            AND  t.grade_level_id = dt.grade_level_id
            AND  t.ayp_subject_id = dt.ayp_subject_id
    SET t.curr_enrl_lag_group_count = dt.failed_cnt + dt.passed_cnt,
        t.curr_enrl_lag_prof_count = dt.passed_cnt;
            
    
    #################
    ## Lagging AYP ##
    #################
    # school level
    UPDATE rpt_nclb_group_results_acc_grade t
        JOIN   (SELECT m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id,
                            COUNT(*) AS cnt,
                            sum(case when  atta.pass_flag = 1 then 1 else 0 end) as passed_cnt,
                            sum(case when  atta.pass_flag = 0 then 1 else 0 end) as failed_cnt
                            FROM    c_ayp_subject_student AS ss
                                    JOIN   c_student_year csy
                                        ON   csy.student_id = ss.student_id
                                        AND  csy.active_flag = 1
                                    JOIN   c_student_school_list AS sschl
                                        ON   sschl.student_id = csy.student_id
                                        AND  sschl.school_year_id = csy.school_year_id
                                        AND  sschl.enrolled_school_flag = 1
                                        AND  sschl.active_flag = 1
                                    JOIN   c_school_year sy
                                        ON   csy.school_year_id = sy.school_year_id
                                        AND  sy.active_flag = 1
                                    JOIN   c_ayp_subject as sub
                                        ON   sub.ayp_subject_id = ss.ayp_subject_id
                                    JOIN   c_ayp_test_type_year atty
                                        ON   sub.ayp_test_type_id = atty.ayp_test_type_id
                                        AND  ss.school_year_id = atty.school_year_id
                                        AND  atty.ayp_reporting_flag = 1
                                    JOIN   c_ayp_test_type_al as atta
                                        ON   atta.ayp_test_type_id = sub.ayp_test_type_id
                                    JOIN   c_ayp_achievement_level as aal
                                        ON   aal.al_id = atta.al_id
                                        AND  aal.al_id = ss.al_id
                                    JOIN   pmi_color AS clr
                                        ON   clr.color_id = atta.color_id
                                    JOIN   rpt_student_ayp_group AS sag
                                        ON   sag.student_id = ss.student_id
                                        AND  sag.school_year_id = csy.school_year_id
                                    JOIN   rpt_nclb_group_results_acc_grade m
                                        ON   m.ayp_subject_id = ss.ayp_subject_id
                                        AND  m.ayp_group_id = sag.ayp_group_id
                                        AND  m.grade_level_id = csy.grade_level_id
                                        AND  m.accessor_id = csy.school_id
                            WHERE   ss.al_id IS NOT NULL
                            AND     ss.score_record_flag = 1
                            GROUP BY m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id) dt
            ON   t.ayp_group_id = dt.ayp_group_id
            AND  t.accessor_id = dt.accessor_id
            AND  t.grade_level_id = dt.grade_level_id
            AND  t.ayp_subject_id = dt.ayp_subject_id
    SET t.ayp_enrl_lag_group_count = dt.failed_cnt + dt.passed_cnt,
        t.ayp_enrl_lag_prof_count = dt.passed_cnt;
        
    # district level
    UPDATE rpt_nclb_group_results_acc_grade t
        JOIN   (SELECT m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id,
                            COUNT(*) AS cnt,
                            sum(case when  atta.pass_flag = 1 then 1 else 0 end) as passed_cnt,
                            sum(case when  atta.pass_flag = 0 then 1 else 0 end) as failed_cnt
                            FROM    c_ayp_subject_student AS ss
                                    JOIN   c_student_year csy
                                        ON   csy.student_id = ss.student_id
                                        AND  csy.active_flag = 1
                                    JOIN   c_school_year sy
                                        ON   csy.school_year_id = sy.school_year_id
                                        AND  sy.active_flag = 1
                                    JOIN   c_ayp_subject as sub
                                        ON   sub.ayp_subject_id = ss.ayp_subject_id
                                    JOIN   c_ayp_test_type_year atty
                                        ON   sub.ayp_test_type_id = atty.ayp_test_type_id
                                        AND  ss.school_year_id = atty.school_year_id
                                        AND  atty.ayp_reporting_flag = 1
                                    JOIN   c_ayp_test_type_al as atta
                                        ON   atta.ayp_test_type_id = sub.ayp_test_type_id
                                    JOIN   c_ayp_achievement_level as aal
                                        ON   aal.al_id = atta.al_id
                                        AND  aal.al_id = ss.al_id
                                    JOIN   pmi_color AS clr
                                        ON   clr.color_id = atta.color_id
                                    JOIN   rpt_student_ayp_group AS sag
                                        ON   sag.student_id = ss.student_id
                                        AND  sag.school_year_id = csy.school_year_id
                                    JOIN   rpt_nclb_group_results_acc_grade m
                                        ON   m.ayp_subject_id = ss.ayp_subject_id
                                        AND  m.ayp_group_id = sag.ayp_group_id
                                        AND  m.grade_level_id = csy.grade_level_id
                                        AND  m.accessor_id = @client_id
                            WHERE   ss.al_id IS NOT NULL
                            AND     ss.score_record_flag = 1
                            GROUP BY m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id) dt
            ON   t.ayp_group_id = dt.ayp_group_id
            AND  t.accessor_id = dt.accessor_id
            AND  t.grade_level_id = dt.grade_level_id
            AND  t.ayp_subject_id = dt.ayp_subject_id
    SET t.ayp_enrl_lag_group_count = dt.failed_cnt + dt.passed_cnt,
        t.ayp_enrl_lag_prof_count = dt.passed_cnt;
        
    
    
    ##################
    ## Leading Curr ##
    ##################
    #school level
    UPDATE rpt_nclb_group_results_acc_grade t
        JOIN   (SELECT m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id,
                            COUNT(*) AS cnt,
                            sum(case when  ss.ayp_on_track_flag = 1 then 1 else 0 end) as passed_cnt,
                            sum(case when  ss.ayp_on_track_flag = 0 then 1 else 0 end) as failed_cnt
                            FROM    rpt_profile_leading_subject_stu AS ss
                                    JOIN   c_school_year sy
                                        ON   sy.active_flag = 1
                                    JOIN   c_student_year csy
                                        ON   csy.school_year_id = sy.school_year_id
                                        AND  csy.student_id = ss.student_id
                                        AND  csy.active_flag = 1
                                    JOIN   c_student_school_list AS sschl
                                        ON   sschl.student_id = csy.student_id
                                        AND  sschl.school_year_id = csy.school_year_id
                                        AND  sschl.enrolled_school_flag = 1
                                        AND  sschl.active_flag = 1
                                    JOIN   c_ayp_subject as sub
                                        ON   sub.ayp_subject_id = ss.ayp_subject_id
                                    JOIN   c_ayp_test_type_year atty
                                        ON   sub.ayp_test_type_id = atty.ayp_test_type_id
                                        AND  atty.ayp_reporting_flag = 1
                                    JOIN   rpt_student_group AS sag
                                        ON   sag.student_id = ss.student_id
                                        AND  sag.school_year_id = sy.school_year_id
                                    JOIN   rpt_nclb_group_results_acc_grade m
                                        ON   m.ayp_subject_id = ss.ayp_subject_id
                                        AND  m.ayp_group_id = sag.ayp_group_id
                                        AND  m.grade_level_id = csy.grade_level_id
                                        AND  m.accessor_id = csy.school_id
                            WHERE   ss.total_pp IS NOT NULL
                            GROUP BY m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id) dt
            ON   t.ayp_group_id = dt.ayp_group_id
            AND  t.accessor_id = dt.accessor_id
            AND  t.grade_level_id = dt.grade_level_id
            AND  t.ayp_subject_id = dt.ayp_subject_id
    SET t.curr_enrl_bm_pass_count = dt.passed_cnt,
        t.curr_enrl_bm_group_count = dt.passed_cnt + dt.failed_cnt;

    #district level
    UPDATE rpt_nclb_group_results_acc_grade t
        JOIN   (SELECT m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id,
                            COUNT(*) AS cnt,
                            sum(case when  ss.ayp_on_track_flag = 1 then 1 else 0 end) as passed_cnt,
                            sum(case when  ss.ayp_on_track_flag = 0 then 1 else 0 end) as failed_cnt
                            FROM    rpt_profile_leading_subject_stu AS ss
                                    JOIN   c_school_year sy
                                        ON   sy.active_flag = 1
                                    JOIN   c_student_year csy
                                        ON   csy.school_year_id = sy.school_year_id
                                        AND  csy.student_id = ss.student_id
                                        AND  csy.active_flag = 1
                                    JOIN   c_ayp_subject as sub
                                        ON   sub.ayp_subject_id = ss.ayp_subject_id
                                    JOIN   c_ayp_test_type_year atty
                                        ON   sub.ayp_test_type_id = atty.ayp_test_type_id
                                        AND  atty.ayp_reporting_flag = 1
                                    JOIN   rpt_student_group AS sag
                                        ON   sag.student_id = ss.student_id
                                        AND  sag.school_year_id = sy.school_year_id
                                    JOIN   rpt_nclb_group_results_acc_grade m
                                        ON   m.ayp_subject_id = ss.ayp_subject_id
                                        AND  m.ayp_group_id = sag.ayp_group_id
                                        AND  m.grade_level_id = csy.grade_level_id
                                        AND  m.accessor_id = @client_id
                            WHERE   ss.total_pp IS NOT NULL
                            GROUP BY m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id) dt
            ON   t.ayp_group_id = dt.ayp_group_id
            AND  t.accessor_id = dt.accessor_id
            AND  t.grade_level_id = dt.grade_level_id
            AND  t.ayp_subject_id = dt.ayp_subject_id
    SET t.curr_enrl_bm_pass_count = dt.passed_cnt,
        t.curr_enrl_bm_group_count = dt.passed_cnt + dt.failed_cnt;
        
    #################
    ## Leading AYP ##
    #################
    # school level
    UPDATE rpt_nclb_group_results_acc_grade t
        JOIN   (SELECT m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id,
                            COUNT(*) AS cnt,
                            sum(case when  ss.ayp_on_track_flag = 1 then 1 else 0 end) as passed_cnt,
                            sum(case when  ss.ayp_on_track_flag = 0 then 1 else 0 end) as failed_cnt
                            FROM    rpt_profile_leading_subject_stu AS ss
                                    JOIN   c_school_year sy
                                        ON   sy.active_flag = 1
                                    JOIN   c_student_year csy
                                        ON   csy.school_year_id = sy.school_year_id
                                        AND  csy.student_id = ss.student_id
                                        AND  csy.active_flag = 1
                                    JOIN   c_student_school_list AS sschl
                                        ON   sschl.student_id = csy.student_id
                                        AND  sschl.school_year_id = csy.school_year_id
                                        AND  sschl.enrolled_school_flag = 1
                                        AND  sschl.active_flag = 1
                                    JOIN   c_ayp_subject as sub
                                        ON   sub.ayp_subject_id = ss.ayp_subject_id
                                    JOIN   c_ayp_test_type_year atty
                                        ON   sub.ayp_test_type_id = atty.ayp_test_type_id
                                        AND  atty.ayp_reporting_flag = 1
                                    JOIN   rpt_student_ayp_group AS sag
                                        ON   sag.student_id = ss.student_id
                                        AND  sag.school_year_id = sy.school_year_id
                                    JOIN   rpt_nclb_group_results_acc_grade m
                                        ON   m.ayp_subject_id = ss.ayp_subject_id
                                        AND  m.ayp_group_id = sag.ayp_group_id
                                        AND  m.grade_level_id = sag.grade_level_id
                                        AND  m.accessor_id = sag.accessor_id
                            WHERE   ss.total_pp IS NOT NULL
                            GROUP BY m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id) dt
            ON   t.ayp_group_id = dt.ayp_group_id
            AND  t.accessor_id = dt.accessor_id
            AND  t.grade_level_id = dt.grade_level_id
            AND  t.ayp_subject_id = dt.ayp_subject_id
    SET t.ayp_enrl_bm_pass_count = dt.passed_cnt,
        t.ayp_enrl_bm_group_count = dt.passed_cnt + dt.failed_cnt;

    # district level
    UPDATE rpt_nclb_group_results_acc_grade t
        JOIN   (SELECT m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id,
                            COUNT(*) AS cnt,
                            sum(case when  ss.ayp_on_track_flag = 1 then 1 else 0 end) as passed_cnt,
                            sum(case when  ss.ayp_on_track_flag = 0 then 1 else 0 end) as failed_cnt
                            FROM    rpt_profile_leading_subject_stu AS ss
                                    JOIN   c_school_year sy
                                        ON   sy.active_flag = 1
                                    JOIN   c_student_year csy
                                        ON   csy.school_year_id = sy.school_year_id
                                        AND  csy.student_id = ss.student_id
                                        AND  csy.active_flag = 1
                                    JOIN   c_ayp_subject as sub
                                        ON   sub.ayp_subject_id = ss.ayp_subject_id
                                    JOIN   c_ayp_test_type_year atty
                                        ON   sub.ayp_test_type_id = atty.ayp_test_type_id
                                        AND  atty.ayp_reporting_flag = 1
                                    JOIN   rpt_student_ayp_group AS sag
                                        ON   sag.student_id = ss.student_id
                                        AND  sag.school_year_id = sy.school_year_id
                                    JOIN   rpt_nclb_group_results_acc_grade m
                                        ON   m.ayp_subject_id = ss.ayp_subject_id
                                        AND  m.ayp_group_id = sag.ayp_group_id
                                        AND  m.grade_level_id = sag.grade_level_id
                                        AND  m.accessor_id = @client_id
                            WHERE   ss.total_pp IS NOT NULL
                            GROUP BY m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.accessor_id) dt
            ON   t.ayp_group_id = dt.ayp_group_id
            AND  t.accessor_id = dt.accessor_id
            AND  t.grade_level_id = dt.grade_level_id
            AND  t.ayp_subject_id = dt.ayp_subject_id
    SET t.ayp_enrl_bm_pass_count = dt.passed_cnt,
        t.ayp_enrl_bm_group_count = dt.passed_cnt + dt.failed_cnt;
        
    #################
    ## Safe Harbor ##
    #################
    if @view_exists > 0 then

        select  count(*)
        into    @safe_harbor_data_exists
        from    v_pmi_ods_ayp_safe_harbor
        ;

        if @safe_harbor_data_exists > 0 then
        
            UPDATE rpt_nclb_group_results_acc_grade t
                JOIN    rpt_nclb_group_enroll_acc_grade g
                        ON  t.accessor_id = g.accessor_id
                        AND t.ayp_group_id = g.ayp_group_id
                        AND t.grade_level_id = g.grade_level_id
                JOIN (SELECT  CASE WHEN d.school_code IS NULL THEN c.client_id ELSE da.accessor_id END AS accessor_id,
                                sub.ayp_subject_id,
                                g.ayp_group_id,
                                d.percentage_of_students
                        FROM v_pmi_ods_ayp_safe_harbor as d
                            JOIN pmi_admin.pmi_client c
                                ON c.client_code = d.client_code
                            JOIN c_ayp_group g
                                ON g.ayp_group_code = d.nclb_code
                            JOIN c_ayp_subject sub
                                ON sub.ayp_subject_code = d.high_stakes_subject_code
                            LEFT JOIN c_school s
                                ON s.school_code = d.school_code
                            LEFT JOIN c_data_accessor da
                                ON da.accessor_id = s.school_id
                                AND da.accessor_type_code = 's'
                        WHERE EXISTS (SELECT *
                                        FROM c_data_accessor da2
                                        WHERE da2.client_id = c.client_id)
                                        AND percentage_of_students IS NOT null) dt
                    ON  t.accessor_id = dt.accessor_id
                    AND t.ayp_subject_id = dt.ayp_subject_id
                    AND t.ayp_group_id = dt.ayp_group_id
            SET curr_enrl_safe_count = (((100 -(dt.percentage_of_students - (dt.percentage_of_students * .1)))*.01)*g.curr_enroll_count),
                ayp_enrl_safe_count = (((100 -(dt.percentage_of_students - (dt.percentage_of_students * .1)))*.01)*g.ayp_enroll_count);

        end if;
        
    end if;
    
END;
//
