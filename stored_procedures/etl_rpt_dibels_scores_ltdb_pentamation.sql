/*
$Rev: 7380 $ 
$Author: randall.stanley $ 
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_dibels_scores_ltdb_pentamation.sql $
$Id: etl_rpt_dibels_scores_ltdb_pentamation.sql 7380 2009-07-16 14:23:58Z randall.stanley $ 
 */

####################################################################
# Insert Dibels data into rpt tables.
# 
####################################################################
DROP PROCEDURE IF EXISTS etl_rpt_dibels_scores_ltdb_pentamation//

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_dibels_scores_ltdb_pentamation()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7380 $ $Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $'

PROC: BEGIN 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend); 

    ######################################
    ## Pull Dibles scores from ltdb     ##
    ## into temp table                  ##
    ######################################
    DROP TEMPORARY TABLE IF EXISTS tmp_pmi_ods_ltdb_pentamation_dibels;
    
    CREATE TEMPORARY TABLE tmp_pmi_ods_ltdb_pentamation_dibels (
    student_id int(10) NOT NULL,
    subtest_name varchar(15) NOT NULL,
    test_date datetime NOT NULL default '1980-12-31 00:00:00',
    period tinyint(4) NOT NULL,
    score01 varchar(11) default NULL,
    score15 varchar(11) default NULL,
      PRIMARY KEY  (`student_id`, subtest_name, test_date),
      INDEX `ind_student_id_ltdb_dibels` (`student_id`),
      KEY `ind_subtest_name_ltdb_dibels` (`subtest_name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

    INSERT tmp_pmi_ods_ltdb_pentamation_dibels (
        student_id
        ,subtest_name
        ,test_date
        ,period
        ,score01
        ,score15)
    SELECT DISTINCT 
        student_id
        ,subtest_name
        ,str_to_date(test_date, '%m/%d/%Y') AS test_date
        -- This case is used to pull the correct periods (1-3) from the months the tests were taken...
        ,CASE WHEN month(str_to_date(test_date, '%m/%d/%Y')) BETWEEN  7 AND 10 THEN 1
                WHEN month(str_to_date(test_date, '%m/%d/%Y')) BETWEEN 11 AND 12 THEN 2
                WHEN month(str_to_date(test_date, '%m/%d/%Y')) BETWEEN  1 AND  1 THEN 2
                WHEN month(str_to_date(test_date, '%m/%d/%Y')) BETWEEN  2 AND  6 THEN 3 END AS period
        ,score01
        ,score15
    FROM v_pmi_ods_ltdb_pentamation
    WHERE test_name = 'DIBELS';


    ##############################################################
    # Insert Dibels scores
    ##############################################################
    INSERT INTO rpt_dibels_scores (
        measure_period_id
        ,student_id
        ,school_year_id
        ,score
        ,score_color
        ,last_user_id
        ,create_timestamp
        )
        SELECT 
            dmp.measure_period_id
            ,st.student_id
            ,sty.school_year_id
            ,dods.score01 AS score
            ,NULL AS score_color
            ,1234
            ,now()
        FROM tmp_pmi_ods_ltdb_pentamation_dibels AS dods
        JOIN c_student AS st
            ON   st.student_code = dods.student_id
        JOIN c_school_year AS sy
            ON dods.test_date BETWEEN sy.begin_date AND sy.end_date
        -- Added this derived table so we only get the min year for each grade level per student
        JOIN (  SELECT student_id, grade_level_id, min(school_year_id) AS school_year_id
                FROM c_student_year AS dsty
                GROUP BY dsty.student_id, dsty.grade_level_id ) AS sty
            ON   sty.student_id = st.student_id
            AND  sty.school_year_id = sy.school_year_id
        JOIN c_grade_level AS gl
            ON   gl.grade_level_id = sty.grade_level_id
            
        JOIN pm_dibels_measure AS dm
            ON   dm.measure_code = dods.subtest_name
        JOIN pm_dibels_assess_freq_period AS dafp
            -- REWRITE:  Need to pull this ID from pmi_client_settings, once we have it in place (pmDibelAssessFreqId)...
            ON  dafp.freq_id = 1000001
            AND dafp.period_code = dods.period
        JOIN pm_dibels_measure_period AS dmp FORCE INDEX (uq_pm_dibels_measure_period)
            ON   dm.measure_id = dmp.measure_id
            AND  dafp.freq_id   = dmp.freq_id
            AND  dafp.period_id = dmp.period_id
            AND  gl.grade_level_id = dmp.grade_level_id
        WHERE dods.score01 IS NOT NULL

    ON DUPLICATE key UPDATE last_user_id = 1234
        ,score = dods.score01;

    DROP TEMPORARY TABLE IF EXISTS tmp_pmi_ods_ltdb_pentamation_dibels;

    -- Update imp_upload_log
    SET @sql_string := '';
    SET @sql_string := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_ltdb_pentamation', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
    prepare sql_string from @sql_string;
    execute sql_string;
    deallocate prepare sql_string;     

END PROC;
//
