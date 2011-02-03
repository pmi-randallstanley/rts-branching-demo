/*
$Rev: 7984 $ 
$Author: randall.stanley $ 
$Date: 2009-12-09 09:58:39 -0500 (Wed, 09 Dec 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_matrix.sql $
$Id: etl_rpt_matrix.sql 7984 2009-12-09 14:58:39Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_rpt_matrix //

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_matrix(rebuild int)
COMMENT '$Rev: 7984 $ $Date: 2009-12-09 09:58:39 -0500 (Wed, 09 Dec 2009) $'
CONTAINS SQL
SQL SECURITY INVOKER
BEGIN

    IF rebuild = 1
        THEN 

            #####################
            ## Matrix Buildout ##
            #####################
            -- Only need to run to rebuild raw table
            TRUNCATE TABLE rpt_matrix_2;

 
            DROP TABLE IF EXISTS tmp_matrix;
          
            CREATE TABLE `tmp_matrix` (
              `ayp_subject_id` int(10) NOT NULL,
              `ayp_year_id` int(10) NOT NULL,
              `ayp_group_id` int(10) NOT NULL,
              `measure_id` int(10) NOT NULL,
              `grouping_id` int(10) NOT NULL,
              KEY `ayp_year_id` (`ayp_year_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
            
            INSERT  tmp_matrix
            SELECT    cass.ayp_subject_id
                  ,tty.school_year_id
                  ,ag.ayp_group_id
                  ,mm.measure_id
                  ,mg.grouping_id
            FROM c_ayp_subject AS cass
                JOIN c_ayp_test_type_year tty
                  ON tty.ayp_test_type_id = cass.ayp_test_type_id
                      AND tty.matrix_reporting_flag = 1
                JOIN l_matrix_grouping mg
                JOIN l_matrix_measure mm
                JOIN c_ayp_group ag
                ON   ag.matrix_use_flag = 1;
        
            insert rpt_matrix_2 (ayp_group_id, school_id, grade_level_id, ayp_subject_id, school_year_id, grouping_id, measure_id, last_user_id)
              SELECT tm.ayp_group_id,
              sy.school_id,
              sy.grade_level_id,
              tm.ayp_subject_id,
              sy.school_year_id,
              tm.grouping_id,
              tm.measure_id,
              1234
              FROM (select distinct school_id, grade_level_id, school_year_id from c_student_year) AS sy 
                join  tmp_matrix tm
                  on    sy.school_year_id BETWEEN tm.ayp_year_id - 1
                                  AND  tm.ayp_year_id + 1
              ON DUPLICATE KEY update last_user_id = 1234;                     

              
            INSERT rpt_matrix_2 (ayp_group_id, school_id, grade_level_id, ayp_subject_id, school_year_id, grouping_id, measure_id, last_user_id)
                SELECT DISTINCT
                    m.ayp_group_id,
                    m.school_id,
                    0,
                    m.ayp_subject_id,
                    m.school_year_id,
                    m.grouping_id,
                    m.measure_id,
                    1234
                FROM rpt_matrix_2 m
                WHERE m.grade_level_id <> 0;
                
            INSERT rpt_matrix_2 (ayp_group_id, school_id, grade_level_id, ayp_subject_id, school_year_id, grouping_id, measure_id, last_user_id)
                SELECT DISTINCT
                    m.ayp_group_id,
                    0,
                    m.grade_level_id,
                    m.ayp_subject_id,
                    m.school_year_id,
                    m.grouping_id,
                    m.measure_id,
                    1234
                FROM rpt_matrix_2 m
                WHERE m.school_id <> 0;
            
            /*    
            INSERT rpt_matrix_2 (ayp_group_id, school_id, grade_level_id, ayp_subject_id, school_year_id, grouping_id, measure_id, last_user_id)
                SELECT DISTINCT
                    0,
                    0,
                    m.grade_level_id,
                    m.ayp_subject_id,
                    m.school_year_id,
                    m.grouping_id,
                    m.measure_id,
                    1234
                FROM rpt_matrix_2 m
                WHERE m.school_id <> 0 AND m.grade_level_id <> 0;
            */  
        END IF;


    ##################################
    ## Clear out all current values ##
    ##################################
    UPDATE rpt_matrix_2 m SET
    m.agl_cnt = NULL,
    m.bgl_cnt = NULL,
    m.matrix_value = NULL,
    m.matrix_color_id = NULL,
    m.active_flag = 0;


    ######################
    ## Lagging Standard ##
    ######################
    UPDATE rpt_matrix_2 rm
        JOIN   (SELECT m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.grouping_id,
                            m.measure_id,
                            m.school_year_id,
                            m.school_id,
                            COUNT(*) AS cnt,
                            sum(case when  atta.pass_flag = 1 then 1 else 0 end) as passed_cnt,
                            sum(case when  atta.pass_flag = 0 then 1 else 0 end) as failed_cnt
                            FROM    c_ayp_subject_student AS ss
                                    JOIN   c_student_year csy
                                        ON   csy.student_id = ss.student_id
                                        AND  EXISTS   (  SELECT    *
                                                            FROM   c_class_enrollment AS cle
                                                            WHERE   cle.student_id = csy.student_id)
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
                                    JOIN   c_ayp_test_type_al as atta
                                        ON   atta.ayp_test_type_id = sub.ayp_test_type_id
                                    JOIN   c_ayp_achievement_level as aal
                                        ON   aal.al_id = atta.al_id
                                        AND  aal.al_id = ss.al_id
                                    JOIN   pmi_color AS clr
                                        ON   clr.color_id = atta.color_id
                                    JOIN   rpt_student_group AS sag
                                        ON   sag.student_id = ss.student_id
--                                         AND  sag.school_year_id = csy.school_year_id
                                    JOIN   rpt_matrix_2 m
                                        ON   m.ayp_subject_id = ss.ayp_subject_id
                                        AND  m.ayp_group_id = sag.ayp_group_id
                                        AND  m.school_year_id = ss.school_year_id
                                        AND  m.grade_level_id = csy.grade_level_id
                                        AND  m.school_id = csy.school_id
                                    JOIN   l_matrix_grouping mg
                                        ON   m.grouping_id = mg.grouping_id
                                        AND  mg.moniker = 'lagging'
                                    JOIN   l_matrix_measure mm
                                        ON   m.measure_id = mm.measure_id
                                        AND  mm.moniker in ('standard', 'self')
                            WHERE   ss.al_id IS NOT NULL
                            AND     ss.score_record_flag = 1
                         --   AND  atta.pass_flag = 0
                            GROUP BY m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.grouping_id,
                            m.measure_id,
                            m.school_year_id,
                            m.school_id) dt
            ON   rm.ayp_group_id = dt.ayp_group_id
            AND  rm.school_id = dt.school_id
            AND  rm.grade_level_id = dt.grade_level_id
            AND  rm.school_year_id = dt.school_year_id
            AND  rm.ayp_subject_id = dt.ayp_subject_id
            AND  rm.grouping_id = dt.grouping_id
            AND  rm.measure_id = dt.measure_id
    SET rm.bgl_cnt = dt.failed_cnt,
        rm.agl_cnt = dt.passed_cnt;
    
  
    ####################
    ## State Standard ##
    ####################
    UPDATE rpt_matrix_2 rm
        JOIN   (SELECT m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.grouping_id,
                            m.measure_id,
                            m.school_year_id,
                            m.school_id,
                            COUNT(*) AS cnt,
                            sum(case when  atta.pass_flag = 1 then 1 else 0 end) as passed_cnt,
                            sum(case when  atta.pass_flag = 0 then 1 else 0 end) as failed_cnt
                            FROM    c_ayp_subject_student AS ss
                                    JOIN   c_student_year csy
                                        ON   csy.student_id = ss.student_id
                                        AND  csy.school_year_id = ss.school_year_id
                                    JOIN   c_school_year sy
                                        ON   csy.school_year_id = sy.school_year_id
                                    JOIN   c_ayp_subject as sub
                                        ON   sub.ayp_subject_id = ss.ayp_subject_id
                                    JOIN   c_ayp_test_type_al as atta
                                        ON   atta.ayp_test_type_id = sub.ayp_test_type_id
                                    JOIN   c_ayp_achievement_level as aal
                                        ON   aal.al_id = atta.al_id
                                        AND  aal.al_id = ss.al_id
                                    JOIN   pmi_color AS clr
                                        ON   clr.color_id = atta.color_id
                                    JOIN   rpt_student_group AS sag
                                        ON   sag.student_id = ss.student_id
                                    JOIN   rpt_matrix_2 m
                                        ON   m.ayp_subject_id = ss.ayp_subject_id
                                        AND  m.ayp_group_id = sag.ayp_group_id
                                        AND  m.school_year_id = ss.school_year_id
                                        AND  m.grade_level_id = csy.grade_level_id
                                        AND  m.school_id = csy.school_id
                                    JOIN   l_matrix_grouping mg
                                        ON   m.grouping_id = mg.grouping_id
                                        AND  mg.moniker = 'state'
                                    JOIN   l_matrix_measure mm
                                        ON   m.measure_id = mm.measure_id
                                        AND  mm.moniker = 'standard'
                            WHERE   ss.al_id IS NOT NULL
                                   --     AND  atta.pass_flag = 0 
                            AND     ss.score_record_flag = 1
                            GROUP BY m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.grouping_id,
                            m.measure_id,
                            m.school_year_id,
                            m.school_id) dt
            ON   rm.ayp_group_id = dt.ayp_group_id
            AND  rm.school_id = dt.school_id
            AND  rm.grade_level_id = dt.grade_level_id
            AND  rm.school_year_id = dt.school_year_id
            AND  rm.ayp_subject_id = dt.ayp_subject_id
            AND  rm.grouping_id = dt.grouping_id
            AND  rm.measure_id = dt.measure_id
    SET rm.bgl_cnt = dt.failed_cnt,
        rm.agl_cnt = dt.passed_cnt;
    
    ######################
    ## Cohort Standard  ##
    ######################
    UPDATE rpt_matrix_2 rm
        JOIN   (SELECT m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.grouping_id,
                            m.measure_id,
                            m.school_year_id,
                            m.school_id,
                            COUNT(*) AS cnt,
                            sum(case when  atta.pass_flag = 1 then 1 else 0 end) as passed_cnt,
                            sum(case when  atta.pass_flag = 0 then 1 else 0 end) as failed_cnt
                            FROM    c_ayp_subject_student AS ss
                                    JOIN   c_school_year sy
                                        ON   sy.active_flag = 1
                                    JOIN   c_student_year csy
                                        ON   csy.school_year_id = sy.school_year_id
                                        AND  csy.student_id = ss.student_id
                                        AND  EXISTS   (  SELECT    *
                                                            FROM   c_class_enrollment AS cle
                                                            WHERE   cle.student_id = csy.student_id)
                                    JOIN   c_student_school_list AS sschl
                                        ON   sschl.student_id = csy.student_id
                                        AND  sschl.school_year_id = csy.school_year_id
                                        AND  sschl.enrolled_school_flag = 1
                                        AND  sschl.active_flag = 1
                                    JOIN   c_ayp_subject as sub
                                        ON   sub.ayp_subject_id = ss.ayp_subject_id
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
                                    JOIN   rpt_matrix_2 m
                                        ON   m.ayp_subject_id = ss.ayp_subject_id
                                        AND  m.ayp_group_id = sag.ayp_group_id
                                        AND  m.school_year_id = ss.school_year_id
                                        AND  m.grade_level_id = csy.grade_level_id
                                        AND  m.school_id = csy.school_id
                                    JOIN   l_matrix_grouping mg
                                        ON   m.grouping_id = mg.grouping_id
                                        AND  mg.moniker = 'cohort'
                                    JOIN   l_matrix_measure mm
                                        ON   m.measure_id = mm.measure_id
                                        AND  mm.moniker = 'standard'
                            WHERE   ss.al_id IS NOT NULL
                            AND     ss.score_record_flag = 1
                                --        AND  atta.pass_flag = 0
                                        AND  EXISTS ( SELECT *
                                                        FROM    c_ayp_subject_student AS ss_m1
                                                            JOIN  c_ayp_subject_student AS ss_c
                                                                ON   ss_c.student_id = ss_m1.student_id
                                                                AND  ss_c.ayp_subject_id = ss_m1.ayp_subject_id
                                                                AND  ss_c.school_year_id = ss_m1.school_year_id + 1
                                                                AND  ss_c.score_record_flag = 1
                                                            JOIN   c_ayp_subject s
                                                                ON   s.ayp_subject_id = ss_m1.ayp_subject_id
                                                            JOIN   (SELECT ayp_test_type_id, MAX(school_year_id) AS school_year_id FROM c_ayp_test_type_year  WHERE matrix_reporting_flag = 1 GROUP BY ayp_test_type_id) ty2
                                                                ON   s.ayp_test_type_id = ty2.ayp_test_type_id
                                                        WHERE   ss_c.school_year_id = ty2.school_year_id
                                                            AND  ss_c.ayp_subject_id = ss.ayp_subject_id
                                                            AND  ss_c.student_id = ss.student_id
                                                            AND  ss_m1.al_id IS NOT NULL
                                                            AND  ss_c.al_id IS NOT NULL
                                                            AND  ss_m1.score_record_flag = 1
                                                                )
                            GROUP BY m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.grouping_id,
                            m.measure_id,
                            m.school_year_id,
                            m.school_id) dt
            ON   rm.ayp_group_id = dt.ayp_group_id
            AND  rm.school_id = dt.school_id
            AND  rm.grade_level_id = dt.grade_level_id
            AND  rm.school_year_id = dt.school_year_id
            AND  rm.ayp_subject_id = dt.ayp_subject_id
            AND  rm.grouping_id = dt.grouping_id
            AND  rm.measure_id = dt.measure_id
    SET rm.bgl_cnt = dt.failed_cnt,
        rm.agl_cnt = dt.passed_cnt;
    
    ######################
    ## Leading Standard ##
    ######################
    UPDATE rpt_matrix_2 rm
        JOIN   (SELECT m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.grouping_id,
                            m.measure_id,
                            m.school_year_id,
                            m.school_id,
                            COUNT(*) AS cnt,
                            sum(case when  ss.ayp_on_track_flag = 1 then 1 else 0 end) as passed_cnt,
                            sum(case when  ss.ayp_on_track_flag = 0 then 1 else 0 end) as failed_cnt
                            FROM    rpt_profile_leading_subject_stu AS ss
                                    JOIN   c_school_year sy
                                        ON   sy.active_flag = 1
                                    JOIN   c_student_year csy
                                        ON   csy.school_year_id = sy.school_year_id
                                        AND  csy.student_id = ss.student_id
                                        AND  EXISTS   (  SELECT    *
                                                            FROM   c_class_enrollment AS cle
                                                            WHERE   cle.student_id = csy.student_id)
                                    JOIN   c_student_school_list AS sschl
                                        ON   sschl.student_id = csy.student_id
                                        AND  sschl.school_year_id = csy.school_year_id
                                        AND  sschl.enrolled_school_flag = 1
                                        AND  sschl.active_flag = 1
                                    JOIN   c_ayp_subject as sub
                                        ON   sub.ayp_subject_id = ss.ayp_subject_id
                                    JOIN   rpt_student_group AS sag
                                        ON   sag.student_id = ss.student_id
                                        AND  sag.school_year_id = sy.school_year_id
                                    JOIN   rpt_matrix_2 m
                                        ON   m.ayp_subject_id = ss.ayp_subject_id
                                        AND  m.ayp_group_id = sag.ayp_group_id
                                        AND  m.school_year_id = sy.school_year_id
                                        AND  m.grade_level_id = csy.grade_level_id
                                        AND  m.school_id = csy.school_id
                                    JOIN   l_matrix_grouping mg
                                        ON   m.grouping_id = mg.grouping_id
                                        AND  mg.moniker = 'Leading'
                                    JOIN   l_matrix_measure mm
                                        ON   m.measure_id = mm.measure_id
                                        AND  mm.moniker = 'standard'
                            WHERE   ss.total_pp IS NOT NULL
                            GROUP BY m.ayp_group_id,
                            m.ayp_subject_id,
                            m.grade_level_id,
                            m.grouping_id,
                            m.measure_id,
                            m.school_year_id,
                            m.school_id) dt
            ON   rm.ayp_group_id = dt.ayp_group_id
            AND  rm.school_id = dt.school_id
            AND  rm.grade_level_id = dt.grade_level_id
            AND  rm.school_year_id = dt.school_year_id
            AND  rm.ayp_subject_id = dt.ayp_subject_id
            AND  rm.grouping_id = dt.grouping_id
            AND  rm.measure_id = dt.measure_id
    SET rm.bgl_cnt = dt.failed_cnt,
        rm.agl_cnt = dt.passed_cnt;
  
    ##################################
    ## Update Grade = 0 AND Dis = 0 ##
    ##################################
    SELECT measure_id INTO @Standard_mid FROM l_matrix_measure WHERE moniker = 'Standard';
    SELECT measure_id INTO @Self_mid     FROM l_matrix_measure WHERE moniker = 'Self';
    SELECT measure_id INTO @District_mid FROM l_matrix_measure WHERE moniker = 'District';
    SELECT measure_id INTO @Cnt_mid      FROM l_matrix_measure WHERE moniker = 'Cnt';
    
    
    UPDATE rpt_matrix_2 rm
        JOIN   (SELECT m.ayp_group_id,
                            0 AS grade_level_id,
                            m.ayp_subject_id,
                            m.school_year_id,
                            m.grouping_id,
                            m.school_id,
                            SUM(m.bgl_cnt) AS bgl_cnt,
                            SUM(m.agl_cnt) AS agl_cnt
                        FROM    rpt_matrix_2 m
                        WHERE m.measure_id = @Standard_mid AND m.grade_level_id <> 0
                        GROUP BY    m.ayp_group_id,
                            m.ayp_subject_id,
                            m.school_year_id,
                            m.grouping_id,
                            m.school_id) dt
            ON   rm.ayp_group_id = dt.ayp_group_id
            AND  rm.grade_level_id = dt.grade_level_id
            AND  rm.school_year_id = dt.school_year_id
            AND  rm.ayp_subject_id = dt.ayp_subject_id
            AND  rm.grouping_id = dt.grouping_id
            AND  rm.school_id = dt.school_id
    SET rm.agl_cnt = dt.agl_cnt,
        rm.bgl_cnt = dt.bgl_cnt;
            
    UPDATE rpt_matrix_2 rm
        JOIN   (SELECT m.ayp_group_id,
                            m.grade_level_id,
                            m.ayp_subject_id,
                            m.school_year_id,
                            m.grouping_id,
                            0 AS school_id,
                            SUM(m.bgl_cnt) AS bgl_cnt,
                            SUM(m.agl_cnt) AS agl_cnt
                        FROM    rpt_matrix_2 m
                        WHERE m.measure_id = @Standard_mid AND m.school_id <> 0
                        GROUP BY    m.ayp_group_id,
                            m.ayp_subject_id,
                            m.school_year_id,
                            m.grouping_id,
                            m.grade_level_id) dt
            ON   rm.ayp_group_id = dt.ayp_group_id
            AND  rm.grade_level_id = dt.grade_level_id
            AND  rm.school_year_id = dt.school_year_id
            AND  rm.ayp_subject_id = dt.ayp_subject_id
            AND  rm.grouping_id = dt.grouping_id
            AND  rm.school_id = dt.school_id
    SET rm.agl_cnt = dt.agl_cnt,
        rm.bgl_cnt = dt.bgl_cnt;        
            
    #############################
    ## Update district agl\bgl ##
    #############################
    UPDATE rpt_matrix_2 rm
        JOIN   (SELECT m.ayp_group_id,
                            m.grade_level_id,
                            m.ayp_subject_id,
                            m.school_year_id,
                            m.grouping_id,
                            SUM(m.bgl_cnt) AS bgl_cnt,
                            SUM(m.agl_cnt) AS agl_cnt
                        FROM    rpt_matrix_2 m
                        WHERE   m.measure_id = @Standard_mid
                        GROUP BY    m.ayp_group_id,
                            m.grade_level_id,
                            m.ayp_subject_id,
                            m.school_year_id,
                            m.grouping_id) dt
            ON   rm.ayp_group_id = dt.ayp_group_id
            AND  rm.grade_level_id = dt.grade_level_id
            AND  rm.school_year_id = dt.school_year_id
            AND  rm.ayp_subject_id = dt.ayp_subject_id
            AND  rm.grouping_id = dt.grouping_id
            AND  rm.measure_id = @District_mid
    SET rm.agl_cnt = dt.agl_cnt,
        rm.bgl_cnt = dt.bgl_cnt;
    
    ##########################
    ## Update Count         ##
    ##########################
    UPDATE rpt_matrix_2 rm
        JOIN   (SELECT m.ayp_group_id,
                            m.grade_level_id,
                            m.ayp_subject_id,
                            m.school_year_id,
                            m.grouping_id,
                            m.school_id,
                            SUM(m.bgl_cnt) AS bgl_cnt,
                            SUM(m.agl_cnt) AS agl_cnt
                        FROM    rpt_matrix_2 m
                        WHERE   m.measure_id = @Standard_mid
                        GROUP BY    m.ayp_group_id,
                            m.grade_level_id,
                            m.ayp_subject_id,
                            m.school_year_id,
                            m.grouping_id,
                            m.school_id) dt
            ON   rm.ayp_group_id = dt.ayp_group_id
            AND  rm.grade_level_id = dt.grade_level_id
            AND  rm.school_year_id = dt.school_year_id
            AND  rm.ayp_subject_id = dt.ayp_subject_id
            AND  rm.grouping_id = dt.grouping_id
            AND  rm.school_id = dt.school_id
            AND  rm.measure_id = @Cnt_mid
    SET rm.agl_cnt = dt.agl_cnt,
        rm.bgl_cnt = dt.bgl_cnt;
    
    ##########################
    ## Update Self          ##
    ##########################
    UPDATE rpt_matrix_2 rm
        JOIN   (SELECT m.ayp_group_id,
                            m.grade_level_id,
                            m.ayp_subject_id,
                            m.school_year_id,
                            m.grouping_id,
                            m.school_id,
                            SUM(m.bgl_cnt) AS bgl_cnt,
                            SUM(m.agl_cnt) AS agl_cnt
                        FROM    rpt_matrix_2 m
                        WHERE   m.measure_id = @Standard_mid
                        GROUP BY    m.ayp_group_id,
                            m.grade_level_id,
                            m.ayp_subject_id,
                            m.school_year_id,
                            m.grouping_id,
                            m.school_id) dt
            ON   rm.ayp_group_id = dt.ayp_group_id
            AND  rm.grade_level_id = dt.grade_level_id
            AND  rm.school_year_id - 1 = dt.school_year_id
            AND  rm.ayp_subject_id = dt.ayp_subject_id
            AND  rm.grouping_id = dt.grouping_id
            AND  rm.school_id = dt.school_id
            AND  rm.measure_id = @Self_mid
    SET rm.agl_cnt = dt.agl_cnt,
        rm.bgl_cnt = dt.bgl_cnt;
    
   


    #############################################
    ## Calculate matrix value based on measure ##
    #############################################
    UPDATE rpt_matrix_2 m
        JOIN   l_matrix_measure mm
            ON   m.measure_id = mm.measure_id
    SET matrix_value = CASE mm.moniker
                            WHEN  'standard' THEN (agl_cnt/(bgl_cnt + agl_cnt)) * 100
                            WHEN  'self'     THEN (agl_cnt/(bgl_cnt + agl_cnt)) * 100
                            WHEN  'district' THEN (agl_cnt/(bgl_cnt + agl_cnt)) * 100
                            WHEN  'cnt'      THEN bgl_cnt + agl_cnt
                        END;
    
    ##########################
    ## Update District\Self ##
    ##########################
    UPDATE rpt_matrix_2 rm
        JOIN   (SELECT m.ayp_group_id,
                            m.grade_level_id,
                            m.ayp_subject_id,
                            m.grouping_id,
                            m.school_id,
                            m.school_year_id,
                            m.matrix_value
                        FROM    rpt_matrix_2 m
                        WHERE   m.measure_id = @Standard_mid) dt
            ON   rm.ayp_group_id = dt.ayp_group_id
            AND  rm.grade_level_id = dt.grade_level_id
            AND  rm.ayp_subject_id = dt.ayp_subject_id
            AND  rm.grouping_id = dt.grouping_id
            AND  rm.school_id = dt.school_id
            AND  rm.school_year_id = dt.school_year_id
            AND  rm.measure_id in (@District_mid, @Self_mid)
    SET rm.matrix_value = dt.matrix_value - rm.matrix_value;
    
    #########################
    ## Set the active Year ##
    #########################
    UPDATE rpt_matrix_2 m set
    m.active_flag = 0;
    
    UPDATE rpt_matrix_2 m 
        JOIN   l_matrix_grouping mg
            ON   m.grouping_id = mg.grouping_id
            AND  mg.moniker IN ('cohort', 'lagging', 'state')
        JOIN   (SELECT s.ayp_subject_id, MAX(atty.school_year_id) AS school_year_id 
                    FROM c_ayp_test_type_year atty
                        JOIN   c_ayp_subject s
                            ON   s.ayp_test_type_id = atty.ayp_test_type_id
                    WHERE atty.matrix_reporting_flag = 1
                    GROUP BY s.ayp_subject_id) dt
            ON   m.ayp_subject_id = dt.ayp_subject_id
            AND  m.school_year_id = dt.school_year_id
    set m.active_flag = 1;


    UPDATE rpt_matrix_2 m 
        JOIN   l_matrix_grouping mg
            ON   m.grouping_id = mg.grouping_id
            AND  mg.moniker IN ('Leading')
        JOIN   c_school_year AS sy
            ON   sy.active_flag = 1
            AND  m.school_year_id = sy.school_year_id
    set m.active_flag = 1;


    ##########################
    ## Remove Lagging\Self  ##
    ##########################
    /*
    SELECT grouping_id INTO @Lagging_gid FROM l_matrix_grouping WHERE moniker = 'Lagging';

    UPDATE rpt_matrix_2 m
        SET matrix_value = NULL
        WHERE m.measure_id = @Self_mid
        AND  m.grouping_id = @Lagging_gid;
    */   

    ##########################
    ## Populate colors      ##
    ##########################
    UPDATE rpt_matrix_2 m
        LEFT JOIN c_grade_level gl
            ON   m.grade_level_id = gl.grade_level_id
        JOIN   c_school_year sy
            ON   m.school_year_id = sy.school_year_id
        JOIN   l_color_matrix cm
                    ON   cm.ayp_subject_id = m.ayp_subject_id
                    AND  cm.grouping_id = m.grouping_id
                    AND  cm.measure_id = m.measure_id
                    AND  COALESCE(gl.grade_sequence, 0) BETWEEN cm.begin_grade_sequence AND cm.end_grade_sequence
                    AND  m.school_year_id BETWEEN cm.begin_year AND cm.end_year
                    AND  m.matrix_value BETWEEN cm.min_range AND cm.max_range
    SET m.matrix_color_id = cm.color_id;
    
    IF rebuild = 1
        THEN 
        
        delete from rpt_matrix_2 where matrix_value is null;
    
    END IF;    
        

        
    
END;    
//
