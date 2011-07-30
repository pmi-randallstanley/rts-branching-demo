/*
$Rev: 7380 $ 
$Author: randall.stanley $ 
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_dibels_scores.sql $
$Id: etl_rpt_dibels_scores.sql 7380 2009-07-16 14:23:58Z randall.stanley $ 
 */

####################################################################
# Insert Dibels data into rpt tables.
# 
####################################################################
 /* List of test measures...
    ? ISF: Initial Sounds Fluency
    + LNF: Letter Naming Fluency
    + PSF: Phoneme Segmentation Fluency
    + NWF: Nonsense Word Fluency
    + ORF: DIBELS Oral Reading Fluency
    * RTF: Retell Fluency
    * WUF: Word Use Fluency
 */
DROP PROCEDURE IF EXISTS etl_rpt_dibels_scores//

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_dibels_scores (rebuild int, backfill int)
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7380 $ $Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $'

PROC: BEGIN 

   -- Set variable to indicate how client provides student identifier - default to student_id
   DECLARE v_pm_use_stu_state_code_dibels char(1) default 'n';
    
    -- Lookup how client provides student identifier     
    SET @pm_use_stu_state_code_dibels := pmi_f_get_etl_setting('pmUseStuStateCodeDibels');
    
    IF @pm_use_stu_state_code_dibels = 'y' THEN 
        -- Client provides student identifier as student_state_code
        SET v_pm_use_stu_state_code_dibels = 'y';
    END IF;
     
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  COALESCE(cast(value as SIGNED),0) 
    into    @dibels_assess_freq_id 
    from    pmi_client_settings 
    where   client_setting_code = 'pmDibelAssessFreqId'
    and     client_id = @client_id
    ;
    

    /*
    IF rebuild = 1
    THEN 
        TRUNCATE TABLE rpt_dibels_scores;
    END IF;
    */

    IF backfill = 1
    THEN 
        ######################################
        ## load c_student_year from dibels  ##
        ## used to back fill student info   ##
        ######################################

        SELECT school_year_id 
        INTO @curyear
        FROM c_school_year
        WHERE c_school_year.active_flag = 1;
        
        # Backfill 2005-2007 student_year based on 2008 school_year data...
        SET @year := @curyear;
        
        backfiller:
        WHILE @year > 2005 DO
            SET @year := @year - 1;
            

            IF v_pm_use_stu_state_code_dibels = 'y' THEN 
                # student_state_code
                INSERT IGNORE INTO c_student_year (
                    student_id
                    ,school_year_id
                    ,school_id
                    ,grade_level_id
                    ,lep_flag
                    ,swd_flag
                    ,econ_disadv_flag
                    ,title1_flag
                    ,migrant_flag
                    ,gifted_flag
                    ,last_user_id
                    ,last_edit_timestamp
                    ,client_id
                    )
                SELECT csty.student_id
                        ,scy.school_year_id
                        ,csty.school_id
                        ,gl.grade_level_id
                        ,csty.lep_flag
                        ,csty.swd_flag
                        ,csty.econ_disadv_flag
                        ,csty.title1_flag
                        ,csty.migrant_flag
                        ,csty.gifted_flag
                        ,1234
                        ,now()
                        ,csty.client_id
                FROM   c_student_year csty
                JOIN   c_student AS st
                    ON   st.student_id = csty.student_id
                JOIN   v_pmi_ods_dibels AS dods
                    ON   st.student_state_code = dods.id
                JOIN   c_grade_level cgl
                    ON   cgl.grade_level_id = csty.grade_level_id
                    AND  cgl.grade_sequence <= 6
                JOIN   c_school_year scy
                    ON   scy.school_year_id = @year
                JOIN   c_grade_level gl
                    ON   gl.grade_sequence = cgl.grade_sequence - (csty.school_year_id - scy.school_year_id)
                LEFT JOIN   c_student_year as sty
                    ON      csty.student_id = sty.student_id
                    AND     sty.school_year_id = @year
                WHERE csty.school_year_id = @year + 1
                AND     sty.student_id is null
                ;
            ELSE
                # student_code
                INSERT IGNORE INTO c_student_year (
                    student_id
                    ,school_year_id
                    ,school_id
                    ,grade_level_id
                    ,lep_flag
                    ,swd_flag
                    ,econ_disadv_flag
                    ,title1_flag
                    ,migrant_flag
                    ,gifted_flag
                    ,last_user_id
                    ,last_edit_timestamp
                    ,client_id
                    )
                SELECT csty.student_id
                        ,scy.school_year_id
                        ,csty.school_id
                        ,gl.grade_level_id
                        ,csty.lep_flag
                        ,csty.swd_flag
                        ,csty.econ_disadv_flag
                        ,csty.title1_flag
                        ,csty.migrant_flag
                        ,csty.gifted_flag
                        ,1234
                        ,now()
                        ,csty.client_id
                FROM   c_student_year AS csty
                JOIN   c_student AS st
                    ON   st.student_id = csty.student_id
                JOIN   v_pmi_ods_dibels AS dods
                    ON   st.student_code = dods.id
                JOIN   c_grade_level AS cgl
                    ON   cgl.grade_level_id = csty.grade_level_id
                    AND  cgl.grade_sequence <= 6
                JOIN   c_school_year AS scy
                    ON   scy.school_year_id = @year
                JOIN   c_grade_level AS gl
                    ON   gl.grade_sequence = cgl.grade_sequence - (csty.school_year_id - scy.school_year_id)
                LEFT JOIN   c_student_year AS sty
                    ON      csty.student_id = sty.student_id
                    AND     sty.school_year_id = @year
                WHERE csty.school_year_id = @year + 1
                AND     sty.student_id is null
                ;

            END IF;
            
        END WHILE backfiller;
            
    END IF;

    ##############################################################
    # Determine student_ids for dibels scoring
    ##############################################################
    
    -- Create temp table for holding students
    DROP TABLE IF EXISTS tmp_etl_rpt_dibels_scores_students;
    
    CREATE TABLE tmp_etl_rpt_dibels_scores_students (
        student_id int(10) NOT NULL,
        student_code   varchar(15) NOT NULL,
         PRIMARY KEY  (student_id));
   
 
    -- Get unique student_id's depending upon how client supplies student identifier
    IF v_pm_use_stu_state_code_dibels = 'y' THEN 
        INSERT INTO tmp_etl_rpt_dibels_scores_students (student_id, student_code)
        SELECT st.student_id, st.student_code
        FROM v_pmi_ods_dibels AS dods
            JOIN c_student AS st
                ON   dods.id = st.student_state_code
        WHERE DODS.id IS NOT NULL
        GROUP BY st.student_id, st.student_code;
    ELSE
        INSERT INTO tmp_etl_rpt_dibels_scores_students (student_id, student_code)
        SELECT st.student_id, st.student_code
        FROM v_pmi_ods_dibels AS dods
            JOIN c_student AS st
                ON   dods.id = st.student_code
        WHERE DODS.id IS NOT NULL
        GROUP BY st.student_id, st.student_code;
   END IF;                          


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
    
                        WHEN gl.grade_code = 'KG'
                          THEN (CASE
                            WHEN dm.measure_code like 'LNF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.lnf_k_1, dods.lnf_k_2, dods.lnf_k_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.lnf_k_4, dods.lnf_k_5, dods.lnf_k_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.lnf_k_7, dods.lnf_k_8, dods.lnf_k_9, dods.lnf_k_10)
                                END)
                    
                            WHEN dm.measure_code like 'ISF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.isf_k_1, dods.isf_k_2, dods.isf_k_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.isf_k_4, dods.isf_k_5, dods.isf_k_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.isf_k_7, dods.isf_k_8, dods.isf_k_9, dods.isf_k_10)
                                END)
                    
                            WHEN dm.measure_code like 'NWF'
                              THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.nwf_k_1, dods.nwf_k_2, dods.nwf_k_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.nwf_k_4, dods.nwf_k_5, dods.nwf_k_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.nwf_k_7, dods.nwf_k_8, dods.nwf_k_9, dods.nwf_k_10)
                                END)
                    
                            WHEN dm.measure_code like 'PSF'
                              THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.psf_k_1, dods.psf_k_2, dods.psf_k_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.psf_k_4, dods.psf_k_5, dods.psf_k_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.psf_k_7, dods.psf_k_8, dods.psf_k_9, dods.psf_k_10)
                                END)
                    
    --                        WHEN dm.measure_code like 'ORF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.orf_k_1, dods.orf_k_2, dods.orf_k_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.orf_k_4, dods.orf_k_5, dods.orf_k_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.orf_k_7, dods.orf_k_8, dods.orf_k_9, dods.orf_k_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'RTF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.rtf_k_1, dods.rtf_k_2, dods.rtf_k_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.rtf_k_4, dods.rtf_k_5, dods.rtf_k_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.rtf_k_7, dods.rtf_k_8, dods.rtf_k_9, dods.rtf_k_10)
    --                            END)
                    
                            WHEN dm.measure_code like 'WUF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.wuf_k_1, dods.wuf_k_2, dods.wuf_k_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.wuf_k_4, dods.wuf_k_5, dods.wuf_k_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.wuf_k_7, dods.wuf_k_8, dods.wuf_k_9, dods.wuf_k_10)
                                END)
                    
                            END)
    
                        WHEN gl.grade_code = '1'
                          THEN (CASE
                            WHEN dm.measure_code like 'LNF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.lnf_1st_1, dods.lnf_1st_2, dods.lnf_1st_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.lnf_1st_4, dods.lnf_1st_5, dods.lnf_1st_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.lnf_1st_7, dods.lnf_1st_8, dods.lnf_1st_9, dods.lnf_1st_10)
                                END)
                    
    --                        WHEN dm.measure_code like 'ISF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.isf_1st_1, dods.isf_1st_2, dods.isf_1st_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.isf_1st_4, dods.isf_1st_5, dods.isf_1st_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.isf_1st_7, dods.isf_1st_8, dods.isf_1st_9, dods.isf_1st_10)
    --                            END)
                    
                            WHEN dm.measure_code like 'NWF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.nwf_1st_1, dods.nwf_1st_2, dods.nwf_1st_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.nwf_1st_4, dods.nwf_1st_5, dods.nwf_1st_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.nwf_1st_7, dods.nwf_1st_8, dods.nwf_1st_9, dods.nwf_1st_10)
                                END)
                    
                            WHEN dm.measure_code like 'PSF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.psf_1st_1, dods.psf_1st_2, dods.psf_1st_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.psf_1st_4, dods.psf_1st_5, dods.psf_1st_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.psf_1st_7, dods.psf_1st_8, dods.psf_1st_9, dods.psf_1st_10)
                                END)
                    
                            WHEN dm.measure_code like 'ORF'
                              THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.orf_1st_1, dods.orf_1st_2, dods.orf_1st_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.orf_1st_4, dods.orf_1st_5, dods.orf_1st_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.orf_1st_7, dods.orf_1st_8, dods.orf_1st_9, dods.orf_1st_10)
                                END)
                    
                            WHEN dm.measure_code like 'RTF'
                              THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.rtf_1st_1, dods.rtf_1st_2, dods.rtf_1st_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.rtf_1st_4, dods.rtf_1st_5, dods.rtf_1st_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.rtf_1st_7, dods.rtf_1st_8, dods.rtf_1st_9, dods.rtf_1st_10)
                                END)
                    
                            WHEN dm.measure_code like 'WUF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.wuf_1st_1, dods.wuf_1st_2, dods.wuf_1st_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.wuf_1st_4, dods.wuf_1st_5, dods.wuf_1st_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.wuf_1st_7, dods.wuf_1st_8, dods.wuf_1st_9, dods.wuf_1st_10)
                                END)
                    
                            END)
                    
                        WHEN gl.grade_code = '2'
                          THEN (CASE
    --                        WHEN dm.measure_code like 'LNF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.lnf_2nd_1, dods.lnf_2nd_2, dods.lnf_2nd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.lnf_2nd_4, dods.lnf_2nd_5, dods.lnf_2nd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.lnf_2nd_7, dods.lnf_2nd_8, dods.lnf_2nd_9, dods.lnf_2nd_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'ISF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.isf_2nd_1, dods.isf_2nd_2, dods.isf_2nd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.isf_2nd_4, dods.isf_2nd_5, dods.isf_2nd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.isf_2nd_7, dods.isf_2nd_8, dods.isf_2nd_9, dods.isf_2nd_10)
    --                            END)
                    
                            WHEN dm.measure_code like 'NWF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.nwf_2nd_1, dods.nwf_2nd_2, dods.nwf_2nd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.nwf_2nd_4, dods.nwf_2nd_5, dods.nwf_2nd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.nwf_2nd_7, dods.nwf_2nd_8, dods.nwf_2nd_9, dods.nwf_2nd_10)
                                END)
                    
    --                        WHEN dm.measure_code like 'PSF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.psf_2nd_1, dods.psf_2nd_2, dods.psf_2nd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.psf_2nd_4, dods.psf_2nd_5, dods.psf_2nd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.psf_2nd_7, dods.psf_2nd_8, dods.psf_2nd_9, dods.psf_2nd_10)
    --                            END)
                    
                            WHEN dm.measure_code like 'ORF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.orf_2nd_1, dods.orf_2nd_2, dods.orf_2nd_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.orf_2nd_4, dods.orf_2nd_5, dods.orf_2nd_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.orf_2nd_7, dods.orf_2nd_8, dods.orf_2nd_9, dods.orf_2nd_10)
                                END)
                    
                            WHEN dm.measure_code like 'RTF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.rtf_2nd_1, dods.rtf_2nd_2, dods.rtf_2nd_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.rtf_2nd_4, dods.rtf_2nd_5, dods.rtf_2nd_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.rtf_2nd_7, dods.rtf_2nd_8, dods.rtf_2nd_9, dods.rtf_2nd_10)
                                END)
                    
                            WHEN dm.measure_code like 'WUF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.wuf_2nd_1, dods.wuf_2nd_2, dods.wuf_2nd_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.wuf_2nd_4, dods.wuf_2nd_5, dods.wuf_2nd_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.wuf_2nd_7, dods.wuf_2nd_8, dods.wuf_2nd_9, dods.wuf_2nd_10)
                                END)
                    
                            END)
                    
                        WHEN gl.grade_code = '3'
                          THEN (CASE
    --                        WHEN dm.measure_code like 'LNF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.lnf_3rd_1, dods.lnf_3rd_2, dods.lnf_3rd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.lnf_3rd_4, dods.lnf_3rd_5, dods.lnf_3rd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.lnf_3rd_7, dods.lnf_3rd_8, dods.lnf_3rd_9, dods.lnf_3rd_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'ISF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.isf_3rd_1, dods.isf_3rd_2, dods.isf_3rd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.isf_3rd_4, dods.isf_3rd_5, dods.isf_3rd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.isf_3rd_7, dods.isf_3rd_8, dods.isf_3rd_9, dods.isf_3rd_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'NWF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.nwf_3rd_1, dods.nwf_3rd_2, dods.nwf_3rd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.nwf_3rd_4, dods.nwf_3rd_5, dods.nwf_3rd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.nwf_3rd_7, dods.nwf_3rd_8, dods.nwf_3rd_9, dods.nwf_3rd_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'PSF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.psf_3rd_1, dods.psf_3rd_2, dods.psf_3rd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.psf_3rd_4, dods.psf_3rd_5, dods.psf_3rd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.psf_3rd_7, dods.psf_3rd_8, dods.psf_3rd_9, dods.psf_3rd_10)
    --                            END)
                    
                            WHEN dm.measure_code like 'ORF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.orf_3rd_1, dods.orf_3rd_2, dods.orf_3rd_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.orf_3rd_4, dods.orf_3rd_5, dods.orf_3rd_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.orf_3rd_7, dods.orf_3rd_8, dods.orf_3rd_9, dods.orf_3rd_10)
                                END)
                    
                            WHEN dm.measure_code like 'RTF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.rtf_3rd_1, dods.rtf_3rd_2, dods.rtf_3rd_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.rtf_3rd_4, dods.rtf_3rd_5, dods.rtf_3rd_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.rtf_3rd_7, dods.rtf_3rd_8, dods.rtf_3rd_9, dods.rtf_3rd_10)
                                END)
                    
                            WHEN dm.measure_code like 'WUF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.wuf_3rd_1, dods.wuf_3rd_2, dods.wuf_3rd_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.wuf_3rd_4, dods.wuf_3rd_5, dods.wuf_3rd_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.wuf_3rd_7, dods.wuf_3rd_8, dods.wuf_3rd_9, dods.wuf_3rd_10)
                                END)
                    
                            END)
                            
                            
                        WHEN gl.grade_code = '4'
                          THEN (CASE
    --                        WHEN dm.measure_code like 'LNF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.lnf_4th_1, dods.lnf_4th_2, dods.lnf_4th_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.lnf_4th_4, dods.lnf_4th_5, dods.lnf_4th_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.lnf_4th_7, dods.lnf_4th_8, dods.lnf_4th_9, dods.lnf_4th_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'ISF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.isf_4th_1, dods.isf_4th_2, dods.isf_4th_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.isf_4th_4, dods.isf_4th_5, dods.isf_4th_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.isf_4th_7, dods.isf_4th_8, dods.isf_4th_9, dods.isf_4th_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'NWF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.nwf_4th_1, dods.nwf_4th_2, dods.nwf_4th_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.nwf_4th_4, dods.nwf_4th_5, dods.nwf_4th_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.nwf_4th_7, dods.nwf_4th_8, dods.nwf_4th_9, dods.nwf_4th_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'PSF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.psf_4th_1, dods.psf_4th_2, dods.psf_4th_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.psf_4th_4, dods.psf_4th_5, dods.psf_4th_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.psf_4th_7, dods.psf_4th_8, dods.psf_4th_9, dods.psf_4th_10)
    --                            END)
                    
                            WHEN dm.measure_code like 'ORF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.orf_4th_1, dods.orf_4th_2, dods.orf_4th_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.orf_4th_4, dods.orf_4th_5, dods.orf_4th_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.orf_4th_7, dods.orf_4th_8, dods.orf_4th_9, dods.orf_4th_10)
                                END)
                    
                            WHEN dm.measure_code like 'RTF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.rtf_4th_1, dods.rtf_4th_2, dods.rtf_4th_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.rtf_4th_4, dods.rtf_4th_5, dods.rtf_4th_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.rtf_4th_7, dods.rtf_4th_8, dods.rtf_4th_9, dods.rtf_4th_10)
                                END)
                    
--                            WHEN dm.measure_code like 'WUF'
--                              THEN (CASE
--                                WHEN dafp.period_code = '1'
--                                  THEN COALESCE(dods.wuf_4th_1, dods.wuf_4th_2, dods.wuf_4th_3)
--                                WHEN dafp.period_code = '2'
--                                  THEN COALESCE(dods.wuf_4th_4, dods.wuf_4th_5, dods.wuf_4th_6)
--                                WHEN dafp.period_code = '3'
--                                  THEN COALESCE(dods.wuf_4th_7, dods.wuf_4th_8, dods.wuf_4th_9, dods.wuf_4th_10)
--                                END)
                    
                            END)                            
                         WHEN gl.grade_code = '5'
                          THEN (CASE
    --                        WHEN dm.measure_code like 'LNF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.lnf_5th_1, dods.lnf_5th_2, dods.lnf_5th_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.lnf_5th_4, dods.lnf_5th_5, dods.lnf_5th_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.lnf_5th_7, dods.lnf_5th_8, dods.lnf_5th_9, dods.lnf_5th_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'ISF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.isf_5th_1, dods.isf_5th_2, dods.isf_5th_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.isf_5th_4, dods.isf_5th_5, dods.isf_5th_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.isf_5th_7, dods.isf_5th_8, dods.isf_5th_9, dods.isf_5th_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'NWF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.nwf_5th_1, dods.nwf_5th_2, dods.nwf_5th_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.nwf_5th_4, dods.nwf_5th_5, dods.nwf_5th_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.nwf_5th_7, dods.nwf_5th_8, dods.nwf_5th_9, dods.nwf_5th_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'PSF'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.psf_5th_1, dods.psf_5th_2, dods.psf_5th_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.psf_5th_4, dods.psf_5th_5, dods.psf_5th_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.psf_5th_7, dods.psf_5th_8, dods.psf_5th_9, dods.psf_5th_10)
    --                            END)
                    
                            WHEN dm.measure_code like 'ORF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.orf_5th_1, dods.orf_5th_2, dods.orf_5th_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.orf_5th_4, dods.orf_5th_5, dods.orf_5th_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.orf_5th_7, dods.orf_5th_8, dods.orf_5th_9, dods.orf_5th_10)
                                END)
                    
                            WHEN dm.measure_code like 'RTF'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.rtf_5th_1, dods.rtf_5th_2, dods.rtf_5th_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.rtf_5th_4, dods.rtf_5th_5, dods.rtf_5th_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.rtf_5th_7, dods.rtf_5th_8, dods.rtf_5th_9, dods.rtf_5th_10)
                                END)
                    
--                            WHEN dm.measure_code like 'WUF'
--                              THEN (CASE
--                                WHEN dafp.period_code = '1'
--                                  THEN COALESCE(dods.wuf_5th_1, dods.wuf_5th_2, dods.wuf_5th_3)
--                                WHEN dafp.period_code = '2'
--                                  THEN COALESCE(dods.wuf_5th_4, dods.wuf_5th_5, dods.wuf_5th_6)
--                                WHEN dafp.period_code = '3'
--                                  THEN COALESCE(dods.wuf_5th_7, dods.wuf_5th_8, dods.wuf_5th_9, dods.wuf_5th_10)
--                                END)
                    
                            END)                               
                    
                        END) AS score
         FROM v_pmi_ods_dibels AS dods
            JOIN tmp_etl_rpt_dibels_scores_students AS st
                ON   st.student_code = dods.id
            JOIN tmp_student_grade_level_school_year AS sty
                ON   sty.student_id = st.student_id 
            JOIN c_grade_level AS gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN pm_dibels_measure AS dm
            JOIN pm_dibels_assess_freq_period AS dafp
                ON   dafp.freq_id = @dibels_assess_freq_id
            JOIN pm_dibels_measure_period AS xdmp
                ON   dm.measure_id  = xdmp.measure_id
                AND  dafp.period_id = xdmp.period_id
                AND  dafp.freq_id   = xdmp.freq_id
                AND  gl.grade_level_id = xdmp.grade_level_id
            WHERE sty.grade_level_id = gl.grade_level_id
        ) AS dt
       WHERE dt.score IS NOT NULL
        ON DUPLICATE key UPDATE last_user_id = 1234
            ,score = values(score);


        -- Cleanup
        DROP TABLE IF EXISTS tmp_etl_rpt_dibels_scores_students;
        DROP TABLE IF EXISTS tmp_student_grade_level_school_year;

        -- Update imp_upload_log
        SET @sql_string := '';
        SET @sql_string := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_dibels', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
        
        prepare sql_string from @sql_string;
        execute sql_string;
        deallocate prepare sql_string;     

END PROC;
//
