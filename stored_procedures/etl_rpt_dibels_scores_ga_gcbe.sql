/*
$Rev: 7380 $ 
$Author: randall.stanley $ 
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_dibels_scores_ga_gcbe.sql $
$Id: etl_rpt_dibels_scores_ga_gcbe.sql 7380 2009-07-16 14:23:58Z randall.stanley $ 
 */

###############################################
# insert DIBELS data for ga_gcbe
#
###############################################
 /* List of test measures...for reference
    ? ISF: Initial Sounds Fluency
    + LNF: Letter Naming Fluency
    + PSF: Phoneme Segmentation Fluency
    + NWF: Nonsense Word Fluency
    + ORF: DIBELS Oral Reading Fluency
    * RTF: Retell Fluency
    * WUF: Word Use Fluency
 */
 
DROP PROCEDURE IF EXISTS etl_rpt_dibels_score_ga_gcbe//

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_dibels_score_ga_gcbe()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7380 $ $Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $'

BEGIN 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend); 
  
  
    ##############################################################
    # Determine student_ids for dibels scoring
    ##############################################################
    
    select COALESCE(cast(value as SIGNED),0) into @pmDibelAssessFreqId from pmi_client_settings where client_setting_code = 'pmDibelAssessFreqId';
    
    
    -- Create temp table for holding students
    DROP TABLE IF EXISTS tmp_etl_rpt_dibels_scores_students;
    
    CREATE TABLE tmp_etl_rpt_dibels_scores_students (
        student_id int(10) NOT NULL,
        student_code   varchar(15) NOT NULL,
        admin_period   varchar(5) NOT NULL,
         PRIMARY KEY  (student_id));
   

    -- Get unique student_id's depending upon how client supplies student identifier

        INSERT INTO tmp_etl_rpt_dibels_scores_students (student_id, student_code,admin_period)
        SELECT st.student_id, st.student_code,
        CASE 
          WHEN dods.admin_period = 'BOY' THEN 1
          WHEN dods.admin_period = 'MOY' THEN 2
          WHEN dods.admin_period = 'EOY' THEN 3 
        END as admin_period
        FROM v_pmi_ods_dibels_ga_gcbe AS dods
            JOIN c_student AS st
               ON   dods.student_code = st.student_code
        WHERE dods.student_code IS NOT NULL
        GROUP BY st.student_id, st.student_code;


    -- Create temp table for holding students grade levels and year
    DROP TABLE IF EXISTS tmp_student_grade_level_school_year;

     CREATE TABLE tmp_student_grade_level_school_year(
            student_id int(10), 
            grade_level_id int(10), 
            school_year_id int(10));
    
     INSERT INTO tmp_student_grade_level_school_year (student_id, grade_level_id, school_year_id)
     SELECT student_id, grade_level_id, min(school_year_id) AS school_year_id
     FROM c_student_year AS dsty
     GROUP BY dsty.student_id, dsty.grade_level_id; 
    
     CREATE INDEX ind_tmp_stud_grade_level  
            ON  tmp_student_grade_level_school_year (student_id, grade_level_id);


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
            dt.measure_period_id
            ,dt.student_id
            ,dt.school_year_id
            ,dt.score
            ,NULL AS score_color
            ,1234
            ,now()
            
    FROM (
            SELECT DISTINCT
                xdmp.measure_period_id
                ,st.student_id
                ,sty.school_year_id
                ,(CASE
                    WHEN dm.measure_code like 'ISF' THEN dods.isf_score
                    WHEN dm.measure_code like 'LNF' THEN dods.lnf_score
                    WHEN dm.measure_code like 'PSF' THEN dods.psf_score
                    WHEN dm.measure_code like 'NWF' THEN dods.nwf_score
                    WHEN dm.measure_code like 'ORF' THEN dods.orf_score
                    WHEN dm.measure_code like 'RTF' THEN dods.rtf_score 
                    WHEN dm.measure_code like 'WUF' THEN dods.wuf_score
                  END
                ) AS score
         FROM v_pmi_ods_dibels_ga_gcbe AS dods
            JOIN tmp_etl_rpt_dibels_scores_students AS st
                ON   st.student_code = dods.student_code
            JOIN tmp_student_grade_level_school_year AS sty
                ON   sty.student_id = st.student_id 
            JOIN c_grade_level AS gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN pm_dibels_measure AS dm
            JOIN pm_dibels_assess_freq_period AS dafp
                ON   dafp.freq_id = @pmDibelAssessFreqId
                AND  dafp.period_code = st.admin_period
            JOIN pm_dibels_measure_period AS xdmp
                ON   dm.measure_id  = xdmp.measure_id
                AND dafp.period_id = xdmp.period_id
                AND  dafp.freq_id   = xdmp.freq_id
                AND  gl.grade_level_id = xdmp.grade_level_id
            WHERE sty.grade_level_id = gl.grade_level_id
        ) AS dt
       WHERE dt.score IS NOT NULL
        ON DUPLICATE key UPDATE last_user_id = 1234
            ,score = dt.score;


        -- Cleanup
        DROP TABLE IF EXISTS tmp_etl_rpt_dibels_scores_students;
        DROP TABLE IF EXISTS tmp_student_grade_level_school_year;
  
          -- Update imp_upload_log
          SET @sql_string := '';
          SET @sql_string := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_dibels_ga_gcbe', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
        
          prepare sql_string from @sql_string;
          execute sql_string;
          deallocate prepare sql_string;     



END;
//
