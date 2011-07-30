/*
$Rev: 7380 $ 
$Author: randall.stanley $ 
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_idel_scores.sql $
$Id: etl_rpt_idel_scores.sql 7380 2009-07-16 14:23:58Z randall.stanley $ 
 */

####################################################################
# Insert idel(Spanish Dibels) data into rpt tables.
# 
####################################################################
 /* List of test measures...
    ? fsi:    Initial Sounds Fluency (isf)
    + fnl:    Letter Naming Fluency (lnf)
    + fsftlp: Phoneme Segmentation Fluency (psf)
    + fpstsl: Nonsense Word Fluency (nwf)
    + flo:    DIBELS Oral Reading Fluency (orf)
    * fro:    Retell Fluency (rtf)
    * fup:    Word Use Fluency (fup)
 */
DROP PROCEDURE IF EXISTS etl_rpt_idel_scores//

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_idel_scores (rebuild int, backfill int)
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7380 $ $Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $'

PROC: BEGIN 

   -- Set variable to indicate how client provides student identifier - default to student_id
   DECLARE v_pm_use_stu_state_code_idel char(1) default 'n';
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    /*
    IF rebuild = 1
    THEN 
        TRUNCATE TABLE rpt_idel_scores;
    END IF;
    */

    IF backfill = 1
    THEN 
        ######################################
        ## load c_student_year from idel    ##
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
                
        SELECT school_year_id 
        INTO @curyear
        FROM c_school_year
        WHERE c_school_year.active_flag = 1;
        
        # Backfill 2005-2007 student_year based on 2008 school_year data...
        SET @year := @curyear;
        
        backfiller:
        WHILE @year > 2005 DO
            SET @year := @year - 1;
            
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
            JOIN   v_pmi_ods_dibels_idel AS dods
                ON   st.student_state_code = dods.id
            JOIN   c_grade_level cgl
                ON   cgl.grade_level_id = csty.grade_level_id
                AND  cgl.grade_sequence <= 6
            JOIN   c_school_year scy
                ON   scy.school_year_id = @year
            JOIN   c_grade_level gl
                ON   gl.grade_sequence = cgl.grade_sequence - (csty.school_year_id - scy.school_year_id)
            WHERE csty.school_year_id = @year + 1
                AND not exists ( select * 
                                from c_student_year sty
                                where  sty.school_year_id = @year
                                    and sty.student_id = csty.student_id );
                                    
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
            JOIN   v_pmi_ods_dibels_idel AS dods
                ON   st.student_code = dods.id
            JOIN   c_grade_level cgl
                ON   cgl.grade_level_id = csty.grade_level_id
                AND  cgl.grade_sequence <= 6
            JOIN   c_school_year scy
                ON   scy.school_year_id = @year
            JOIN   c_grade_level gl
                ON   gl.grade_sequence = cgl.grade_sequence - (csty.school_year_id - scy.school_year_id)
            WHERE csty.school_year_id = @year + 1
                AND not exists ( select * 
                                from c_student_year sty
                                where  sty.school_year_id = @year
                                    and sty.student_id = csty.student_id );                                    
        END WHILE backfiller;
            
    END IF;

    ##############################################################
    # Determine student_ids for idel scoring
    ##############################################################
    
    -- Lookup how client provides student identifier     
    SET @pm_use_stu_state_code_idel := pmi_f_get_etl_setting('pmUseStuStateCodeidel');
    
    IF @pm_use_stu_state_code_idel = 'y' THEN 
        -- Client provides student identifier as student_state_code
        SET v_pm_use_stu_state_code_idel = 'y';
    END IF;
     
    -- Create temp table for holding students
    DROP TABLE IF EXISTS tmp_etl_rpt_idel_scores_students;
    
    CREATE TABLE tmp_etl_rpt_idel_scores_students (
        student_id int(10) NOT NULL,
        student_code   varchar(15) NOT NULL,
         PRIMARY KEY  (student_id));
   
 
    -- Get unique student_id's depending upon how client supplies student identifier
    IF v_pm_use_stu_state_code_idel = 'y' THEN 
        INSERT INTO tmp_etl_rpt_idel_scores_students (student_id, student_code)
        SELECT st.student_id, st.student_code
        FROM v_pmi_ods_dibels_idel AS dods
            JOIN c_student AS st
                ON   dods.id = st.student_state_code
        WHERE DODS.id IS NOT NULL
        GROUP BY st.student_id, st.student_code;
    ELSE
        INSERT INTO tmp_etl_rpt_idel_scores_students (student_id, student_code)
        SELECT st.student_id, st.student_code
        FROM v_pmi_ods_dibels_idel AS dods
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
    # Insert idel scores
    ##############################################################

    INSERT INTO rpt_idel_scores (
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
                            WHEN dm.measure_code like 'fnl'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.fnl_k_1, dods.fnl_k_2, dods.fnl_k_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.fnl_k_4, dods.fnl_k_5, dods.fnl_k_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.fnl_k_7, dods.fnl_k_8, dods.fnl_k_9, dods.fnl_k_10)
                                END)
                    
                            WHEN dm.measure_code like 'fsi'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.fsi_k_1, dods.fsi_k_2, dods.fsi_k_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.fsi_k_4, dods.fsi_k_5, dods.fsi_k_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.fsi_k_7, dods.fsi_k_8, dods.fsi_k_9, dods.fsi_k_10)
                                END)
                    
                            WHEN dm.measure_code like 'fpstsl'
                              THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.fps_tsl_k_1, dods.fps_tsl_k_2, dods.fps_tsl_k_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.fps_tsl_k_4, dods.fps_tsl_k_5, dods.fps_tsl_k_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.fps_tsl_k_7, dods.fps_tsl_k_8, dods.fps_tsl_k_9, dods.fps_tsl_k_10)
                                END)
                    
                            WHEN dm.measure_code like 'fsftlp'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.fsf_tlp_k_1, dods.fsf_tlp_k_2, dods.fsf_tlp_k_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.fsf_tlp_k_4, dods.fsf_tlp_k_5, dods.fsf_tlp_k_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.fsf_tlp_k_7, dods.fsf_tlp_k_8, dods.fsf_tlp_k_9, dods.fsf_tlp_k_10)
                                END)
                    
    --                        WHEN dm.measure_code like 'flo'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.flo_k_1, dods.flo_k_2, dods.flo_k_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.flo_k_4, dods.flo_k_5, dods.flo_k_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.flo_k_7, dods.flo_k_8, dods.flo_k_9, dods.flo_k_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'fro'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.fro_k_1, dods.fro_k_2, dods.fro_k_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.fro_k_4, dods.fro_k_5, dods.fro_k_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.fro_k_7, dods.fro_k_8, dods.fro_k_9, dods.fro_k_10)
    --                            END)
                    
                            WHEN dm.measure_code like 'fup'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.fup_k_1, dods.fup_k_2, dods.fup_k_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.fup_k_4, dods.fup_k_5, dods.fup_k_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.fup_k_7, dods.fup_k_8, dods.fup_k_9, dods.fup_k_10)
                                END)
                    
                            END)
    
                        WHEN gl.grade_code = '1'
                          THEN (CASE
                            WHEN dm.measure_code like 'fnl'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.fnl_1st_1, dods.fnl_1st_2, dods.fnl_1st_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.fnl_1st_4, dods.fnl_1st_5, dods.fnl_1st_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.fnl_1st_7, dods.fnl_1st_8, dods.fnl_1st_9, dods.fnl_1st_10)
                                END)
                    
    --                        WHEN dm.measure_code like 'fsi'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.fsi_1st_1, dods.fsi_1st_2, dods.fsi_1st_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.fsi_1st_4, dods.fsi_1st_5, dods.fsi_1st_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.fsi_1st_7, dods.fsi_1st_8, dods.fsi_1st_9, dods.fsi_1st_10)
    --                            END)
                    
                            WHEN dm.measure_code like 'fpstsl'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.fps_tsl_1st_1, dods.fps_tsl_1st_2, dods.fps_tsl_1st_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.fps_tsl_1st_4, dods.fps_tsl_1st_5, dods.fps_tsl_1st_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.fps_tsl_1st_7, dods.fps_tsl_1st_8, dods.fps_tsl_1st_9, dods.fps_tsl_1st_10)
                                END)
                    
                            WHEN dm.measure_code like 'fsftlp'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.fsf_tlp_1st_1, dods.fsf_tlp_1st_2, dods.fsf_tlp_1st_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.fsf_tlp_1st_4, dods.fsf_tlp_1st_5, dods.fsf_tlp_1st_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.fsf_tlp_1st_7, dods.fsf_tlp_1st_8, dods.fsf_tlp_1st_9, dods.fsf_tlp_1st_10)
                                END)
                    
                            WHEN dm.measure_code like 'flo'
                              THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.flo_1st_1, dods.flo_1st_2, dods.flo_1st_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.flo_1st_4, dods.flo_1st_5, dods.flo_1st_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.flo_1st_7, dods.flo_1st_8, dods.flo_1st_9, dods.flo_1st_10)
                                END)
                    
                            WHEN dm.measure_code like 'fro'
                              THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.fro_1st_1, dods.fro_1st_2, dods.fro_1st_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.fro_1st_4, dods.fro_1st_5, dods.fro_1st_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.fro_1st_7, dods.fro_1st_8, dods.fro_1st_9, dods.fro_1st_10)
                                END)
                    
                            WHEN dm.measure_code like 'fup'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.fup_1st_1, dods.fup_1st_2, dods.fup_1st_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.fup_1st_4, dods.fup_1st_5, dods.fup_1st_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.fup_1st_7, dods.fup_1st_8, dods.fup_1st_9, dods.fup_1st_10)
                                END)
                    
                            END)
                    
                        WHEN gl.grade_code = '2'
                          THEN (CASE
    --                        WHEN dm.measure_code like 'fnl'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.fnl_2nd_1, dods.fnl_2nd_2, dods.fnl_2nd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.fnl_2nd_4, dods.fnl_2nd_5, dods.fnl_2nd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.fnl_2nd_7, dods.fnl_2nd_8, dods.fnl_2nd_9, dods.fnl_2nd_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'fsi'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.fsi_2nd_1, dods.fsi_2nd_2, dods.fsi_2nd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.fsi_2nd_4, dods.fsi_2nd_5, dods.fsi_2nd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.fsi_2nd_7, dods.fsi_2nd_8, dods.fsi_2nd_9, dods.fsi_2nd_10)
    --                            END)
                    
                            WHEN dm.measure_code like 'fpstsl'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.fps_tsl_2nd_1, dods.fps_tsl_2nd_2, dods.fps_tsl_2nd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.fps_tsl_2nd_4, dods.fps_tsl_2nd_5, dods.fps_tsl_2nd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.fps_tsl_2nd_7, dods.fps_tsl_2nd_8, dods.fps_tsl_2nd_9, dods.fps_tsl_2nd_10)
                                END)
                    
    --                        WHEN dm.measure_code like 'fsftlp'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.fsf_tlp_2nd_1, dods.fsf_tlp_2nd_2, dods.fsf_tlp_2nd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.fsf_tlp_2nd_4, dods.fsf_tlp_2nd_5, dods.fsf_tlp_2nd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.fsf_tlp_2nd_7, dods.fsf_tlp_2nd_8, dods.fsf_tlp_2nd_9, dods.fsf_tlp_2nd_10)
    --                            END)
                    
                            WHEN dm.measure_code like 'flo'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.flo_2nd_1, dods.flo_2nd_2, dods.flo_2nd_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.flo_2nd_4, dods.flo_2nd_5, dods.flo_2nd_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.flo_2nd_7, dods.flo_2nd_8, dods.flo_2nd_9, dods.flo_2nd_10)
                                END)
                    
                            WHEN dm.measure_code like 'fro'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.fro_2nd_1, dods.fro_2nd_2, dods.fro_2nd_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.fro_2nd_4, dods.fro_2nd_5, dods.fro_2nd_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.fro_2nd_7, dods.fro_2nd_8, dods.fro_2nd_9, dods.fro_2nd_10)
                                END)
                    
                            WHEN dm.measure_code like 'fup'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.fup_2nd_1, dods.fup_2nd_2, dods.fup_2nd_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.fup_2nd_4, dods.fup_2nd_5, dods.fup_2nd_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.fup_2nd_7, dods.fup_2nd_8, dods.fup_2nd_9, dods.fup_2nd_10)
                                END)
                    
                            END)
                    
                        WHEN gl.grade_code = '3'
                          THEN (CASE
    --                        WHEN dm.measure_code like 'fnl'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.fnl_3rd_1, dods.fnl_3rd_2, dods.fnl_3rd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.fnl_3rd_4, dods.fnl_3rd_5, dods.fnl_3rd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.fnl_3rd_7, dods.fnl_3rd_8, dods.fnl_3rd_9, dods.fnl_3rd_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'fsi'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.fsi_3rd_1, dods.fsi_3rd_2, dods.fsi_3rd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.fsi_3rd_4, dods.fsi_3rd_5, dods.fsi_3rd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.fsi_3rd_7, dods.fsi_3rd_8, dods.fsi_3rd_9, dods.fsi_3rd_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'fpstsl'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.fps_tsl_3rd_1, dods.fps_tsl_3rd_2, dods.fps_tsl_3rd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.fps_tsl_3rd_4, dods.fps_tsl_3rd_5, dods.fps_tsl_3rd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.fps_tsl_3rd_7, dods.fps_tsl_3rd_8, dods.fps_tsl_3rd_9, dods.fps_tsl_3rd_10)
    --                            END)
                    
    --                        WHEN dm.measure_code like 'fsftlp'
    --                          THEN (CASE
    --                            WHEN dafp.period_code = '1'
    --                              THEN COALESCE(dods.fsf_tlp_3rd_1, dods.fsf_tlp_3rd_2, dods.fsf_tlp_3rd_3)
    --                            WHEN dafp.period_code = '2'
    --                              THEN COALESCE(dods.fsf_tlp_3rd_4, dods.fsf_tlp_3rd_5, dods.fsf_tlp_3rd_6)
    --                            WHEN dafp.period_code = '3'
    --                              THEN COALESCE(dods.fsf_tlp_3rd_7, dods.fsf_tlp_3rd_8, dods.fsf_tlp_3rd_9, dods.fsf_tlp_3rd_10)
    --                            END)
                    
                            WHEN dm.measure_code like 'flo'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.flo_3rd_1, dods.flo_3rd_2, dods.flo_3rd_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.flo_3rd_4, dods.flo_3rd_5, dods.flo_3rd_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.flo_3rd_7, dods.flo_3rd_8, dods.flo_3rd_9, dods.flo_3rd_10)
                                END)
                    
                            WHEN dm.measure_code like 'fro'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.fro_3rd_1, dods.fro_3rd_2, dods.fro_3rd_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.fro_3rd_4, dods.fro_3rd_5, dods.fro_3rd_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.fro_3rd_7, dods.fro_3rd_8, dods.fro_3rd_9, dods.fro_3rd_10)
                                END)
                    
                            WHEN dm.measure_code like 'fup'
                              THEN (CASE
                                WHEN dafp.period_code = '1'
                                  THEN COALESCE(dods.fup_3rd_1, dods.fup_3rd_2, dods.fup_3rd_3)
                                WHEN dafp.period_code = '2'
                                  THEN COALESCE(dods.fup_3rd_4, dods.fup_3rd_5, dods.fup_3rd_6)
                                WHEN dafp.period_code = '3'
                                  THEN COALESCE(dods.fup_3rd_7, dods.fup_3rd_8, dods.fup_3rd_9, dods.fup_3rd_10)
                                END)
                    
                            END)
                    
                        END) AS score
         FROM v_pmi_ods_dibels_idel AS dods
            JOIN tmp_etl_rpt_idel_scores_students AS st
                ON   st.student_code = dods.id
            JOIN tmp_student_grade_level_school_year AS sty
                ON   sty.student_id = st.student_id 
            JOIN c_grade_level AS gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN pm_idel_measure AS dm
            JOIN pm_idel_assess_freq_period AS dafp
                ON   dafp.freq_id = (select COALESCE(cast(value as SIGNED),0) from pmi_client_settings where client_setting_code = 'pmDibelAssessFreqId')
            JOIN pm_idel_measure_period AS xdmp
                ON   dm.measure_id  = xdmp.measure_id
                AND  dafp.period_id = xdmp.period_id
                AND  dafp.freq_id   = xdmp.freq_id
                AND  gl.grade_level_id = xdmp.grade_level_id
            WHERE sty.grade_level_id = gl.grade_level_id
        ) AS dt
       WHERE dt.score IS NOT NULL
        ON DUPLICATE key UPDATE last_user_id = 1234
            ,score = dt.score;


        -- Cleanup
        DROP TABLE IF EXISTS tmp_etl_rpt_idel_scores_students;
        DROP TABLE IF EXISTS tmp_student_grade_level_school_year;
        
        -- update score colors
        call etl_rpt_idel_scores_color_update();
        
      SET @sql_scan_log := '';
      SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_dibels_idel', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
  
      -- select @sql_scan_log;
  
      prepare sql_scan_log from @sql_scan_log;
      execute sql_scan_log;
      deallocate prepare sql_scan_log;

END PROC;
//
