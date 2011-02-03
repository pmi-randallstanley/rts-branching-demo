/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_state_test_scores_eoct.sql $
$Id: etl_pm_state_test_scores_eoct.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
 */

####################################################################
# Insert EOCT data into pm_state_test_scores.
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_pm_state_test_scores_eoct//

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_state_test_scores_eoct()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'

PROC: BEGIN 


    # Test for backfilling student_year...
    SELECT CASE WHEN MIN(CAST(right(odsimp.testdate, 2) AS signed) + 2000) < sy.school_year_id THEN 1 ELSE 0 END AS backfill_flag INTO @backfill_flag
    FROM v_pmi_ods_ga_eoct AS odsimp
    JOIN c_school_year AS sy
        ON sy.active_flag = 1;


    IF (@backfill_flag = 1) THEN

        ######################################
        ## load c_student_year from ods     ##
        ## used to back fill student info   ##
        ######################################
        SELECT  c.client_id, c.state_id
        INTO    @client_id, @state_id
        FROM    pmi_admin.pmi_dsn AS db
        JOIN    pmi_admin.pmi_client AS c
                ON      c.dsn_core_id = db.dsn_id
                AND NOT EXISTS  (   SELECT  *
                                    FROM    pmi_admin.pmi_client AS c2
                                    WHERE   c2.dsn_core_id = db.dsn_id
                                    AND     c2.client_id > c.client_id
                                )
        WHERE   db.db_name = database();


        INSERT c_student_year
            (student_id
            ,school_year_id
            ,school_id
            ,grade_level_id
            ,last_user_id
            ,client_id
            ,create_timestamp)    
            SELECT 
                s.student_id
                ,sy.school_year_id
                ,COALESCE(sch.school_id, csty.school_id) AS school_id
                ,gl.grade_level_id
                ,1234
                ,@client_id
                ,now()
            FROM    v_pmi_ods_ga_eoct AS odsimp
            JOIN    c_student AS s
                ON s.student_state_code = odsimp.gtid   -- STUDENT IDs...
            JOIN    c_school_year sy
                ON   CONCAT((CAST(right(odsimp.testdate, 2) AS signed) + 2000), '-',
                        (CASE LEFT(odsimp.testdate, 3)
                                WHEN 'SPR' THEN '04'
                                WHEN 'SUM' THEN '07'
                                WHEN 'FAL' THEN '10'
                                WHEN 'WIN' THEN '12'
                                ELSE '02'
                            END), '-01')  BETWEEN sy.begin_date AND sy.end_date
            JOIN  c_school_year AS csy
                ON csy.active_flag = 1
            JOIN    c_student_year AS csty
                ON    csty.student_id = s.student_id
                AND   csty.school_year_id = csy.school_year_id
            JOIN c_grade_level AS cgl
                ON   cgl.grade_level_id = csty.grade_level_id
            JOIN c_grade_level AS gl
                ON gl.grade_sequence = cgl.grade_sequence - (csy.school_year_id - sy.school_year_id)
            LEFT JOIN c_school AS sch
                ON sch.moniker = odsimp.sch_name
            LEFT JOIN    c_student_year AS sty
                ON    sty.student_id = s.student_id
                AND   sty.school_year_id = sy.school_year_id
            WHERE   odsimp.grade_conversion IS NOT NULL
                AND sty.school_year_id IS NULL
            GROUP BY s.student_id
                ,sy.school_year_id
                ,school_id
                ,grade_level_id
         ON DUPLICATE KEY UPDATE last_user_id = 1234;

        INSERT c_student_year
            (student_id
            ,school_year_id
            ,school_id
            ,grade_level_id
            ,last_user_id
            ,client_id
            ,create_timestamp)    
            SELECT 
                s.student_id
                ,sy.school_year_id
                ,COALESCE(sch.school_id, csty.school_id) AS school_id
                ,gl.grade_level_id
                ,1234
                ,@client_id
                ,now()
            FROM    v_pmi_ods_ga_eoct AS odsimp
            JOIN    c_student AS s
                ON s.student_state_code = odsimp.ssn  -- STUDENT IDs...
            JOIN    c_school_year sy
                ON   CONCAT((CAST(right(odsimp.testdate, 2) AS signed) + 2000), '-',
                        (CASE LEFT(odsimp.testdate, 3)
                                WHEN 'SPR' THEN '04'
                                WHEN 'SUM' THEN '07'
                                WHEN 'FAL' THEN '10'
                                WHEN 'WIN' THEN '12'
                                ELSE '02'
                            END), '-01')  BETWEEN sy.begin_date AND sy.end_date
            JOIN  c_school_year AS csy
                ON csy.active_flag = 1
            JOIN    c_student_year AS csty
                ON    csty.student_id = s.student_id
                AND   csty.school_year_id = csy.school_year_id
            JOIN c_grade_level AS cgl
                ON   cgl.grade_level_id = csty.grade_level_id
            JOIN c_grade_level AS gl
                ON gl.grade_sequence = cgl.grade_sequence - (csy.school_year_id - sy.school_year_id)
            LEFT JOIN c_school AS sch
                ON sch.moniker = odsimp.sch_name
            LEFT JOIN    c_student_year AS sty
                ON    sty.student_id = s.student_id
                AND   sty.school_year_id = sy.school_year_id
            WHERE   odsimp.grade_conversion IS NOT NULL
                AND sty.school_year_id IS NULL
            GROUP BY s.student_id
                ,sy.school_year_id
                ,school_id
                ,grade_level_id
         ON DUPLICATE KEY UPDATE last_user_id = 1234;

        INSERT c_student_year
            (student_id
            ,school_year_id
            ,school_id
            ,grade_level_id
            ,last_user_id
            ,client_id
            ,create_timestamp)    
            SELECT 
                s.student_id
                ,sy.school_year_id
                ,COALESCE(sch.school_id, csty.school_id) AS school_id
                ,gl.grade_level_id
                ,1234
                ,@client_id
                ,now()
            FROM    v_pmi_ods_ga_eoct AS odsimp
            JOIN    c_student AS s
                ON s.fid_code = odsimp.ssn  -- STUDENT IDs...
            JOIN    c_school_year sy
                ON   CONCAT((CAST(right(odsimp.testdate, 2) AS signed) + 2000), '-',
                        (CASE LEFT(odsimp.testdate, 3)
                                WHEN 'SPR' THEN '04'
                                WHEN 'SUM' THEN '07'
                                WHEN 'FAL' THEN '10'
                                WHEN 'WIN' THEN '12'
                                ELSE '02'
                            END), '-01')  BETWEEN sy.begin_date AND sy.end_date
            JOIN  c_school_year AS csy
                ON csy.active_flag = 1
            JOIN    c_student_year AS csty
                ON    csty.student_id = s.student_id
                AND   csty.school_year_id = csy.school_year_id
            JOIN c_grade_level AS cgl
                ON   cgl.grade_level_id = csty.grade_level_id
            JOIN c_grade_level AS gl
                ON gl.grade_sequence = cgl.grade_sequence - (csy.school_year_id - sy.school_year_id)
            LEFT JOIN c_school AS sch
                ON sch.moniker = odsimp.sch_name
            LEFT JOIN    c_student_year AS sty
                ON    sty.student_id = s.student_id
                AND   sty.school_year_id = sy.school_year_id
            WHERE   odsimp.grade_conversion IS NOT NULL
                AND sty.school_year_id IS NULL
            GROUP BY s.student_id
                ,sy.school_year_id
                ,school_id
                ,grade_level_id
         ON DUPLICATE KEY UPDATE last_user_id = 1234;


    END IF;
    
    ##############################################################
    # Insert EOCT scores - student joined on gtid
    ##############################################################
    INSERT INTO pm_state_test_scores (
        test_id
        ,subject_id
        ,student_id
        ,test_year
        ,test_month
        ,score
        ,last_user_id
        ,create_timestamp
        )

        SELECT 
            t.test_id
            ,tsub.subject_id
            ,st.student_id
            ,(CAST(right(odsimp.testdate, 2) AS signed) + 2000) AS test_year
            ,CASE LEFT(odsimp.testdate, 3)
                    WHEN 'SPR' THEN 4
                    WHEN 'SUM' THEN 7
                    WHEN 'FAL' THEN 10
                    WHEN 'WIN' THEN 12
                    ELSE 2
                END AS test_month
            ,odsimp.grade_conversion AS score
            ,1234
            ,now()
        FROM v_pmi_ods_ga_eoct AS odsimp
        JOIN pm_state_test AS t
            ON  t.test_code = 'eoct'
        JOIN  c_student AS st
            ON st.student_state_code = odsimp.gtid  -- CHANGED: KHD - added multiple joins to find the right one...
        JOIN pm_state_test_subject AS tsub
            ON lcase(right(tsub.subject_code, 3)) = lcase(CASE WHEN length(odsimp.section) = 3 
                                                                THEN odsimp.section
                                                                ELSE mid(odsimp.section,3,3) END)
        WHERE   t.test_id = tsub.test_id
            AND odsimp.grade_conversion IS NOT NULL
            AND odsimp.gtid <> 0
    ON DUPLICATE key UPDATE last_user_id = 1234
        ,score = odsimp.grade_conversion;
        
    ##############################################################
    # Insert EOCT scores - student joined on ssn to state code
    ##############################################################
    INSERT INTO pm_state_test_scores (
        test_id
        ,subject_id
        ,student_id
        ,test_year
        ,test_month
        ,score
        ,last_user_id
        ,create_timestamp
        )

        SELECT 
            t.test_id
            ,tsub.subject_id
            ,st.student_id
            ,(CAST(right(odsimp.testdate, 2) AS signed) + 2000) AS test_year
            ,CASE LEFT(odsimp.testdate, 3)
                    WHEN 'SPR' THEN 4
                    WHEN 'SUM' THEN 7
                    WHEN 'FAL' THEN 10
                    WHEN 'WIN' THEN 12
                    ELSE 2
                END AS test_month
            ,odsimp.grade_conversion AS score
            ,1234
            ,now()
        FROM v_pmi_ods_ga_eoct AS odsimp
        JOIN pm_state_test AS t
            ON  t.test_code = 'eoct'
        JOIN  c_student AS st
            ON st.student_state_code = odsimp.ssn
        JOIN pm_state_test_subject AS tsub
            ON lcase(right(tsub.subject_code, 3)) = lcase(CASE WHEN length(odsimp.section) = 3 
                                                                THEN odsimp.section
                                                                ELSE mid(odsimp.section,3,3) END)
        WHERE   t.test_id = tsub.test_id
            AND odsimp.grade_conversion IS NOT NULL
            AND odsimp.ssn <> 0
    ON DUPLICATE key UPDATE last_user_id = 1234
        ,score = odsimp.grade_conversion;
        
    ##############################################################
    # Insert EOCT scores - student joined on ssn to fid_code
    ##############################################################
    INSERT INTO pm_state_test_scores (
        test_id
        ,subject_id
        ,student_id
        ,test_year
        ,test_month
        ,score
        ,last_user_id
        ,create_timestamp
        )

        SELECT 
            t.test_id
            ,tsub.subject_id
            ,st.student_id
            ,(CAST(right(odsimp.testdate, 2) AS signed) + 2000) AS test_year
            ,CASE LEFT(odsimp.testdate, 3)
                    WHEN 'SPR' THEN 4
                    WHEN 'SUM' THEN 7
                    WHEN 'FAL' THEN 10
                    WHEN 'WIN' THEN 12
                    ELSE 2
                END AS test_month
            ,odsimp.grade_conversion AS score
            ,1234
            ,now()
        FROM v_pmi_ods_ga_eoct AS odsimp
        JOIN pm_state_test AS t
            ON  t.test_code = 'eoct'
        JOIN  c_student AS st
            ON st.fid_code = odsimp.ssn
--        JOIN  c_student_year AS sty
--            ON sty.student_id = st.student_id
        JOIN pm_state_test_subject AS tsub
            ON lcase(right(tsub.subject_code, 3)) = lcase(CASE WHEN length(odsimp.section) = 3 
                                                                THEN odsimp.section
                                                                ELSE mid(odsimp.section,3,3) END)
        WHERE   t.test_id = tsub.test_id
            AND odsimp.grade_conversion IS NOT NULL
            AND odsimp.ssn <> 0
    ON DUPLICATE key UPDATE last_user_id = 1234
        ,score = odsimp.grade_conversion     ;   


END PROC;
//
