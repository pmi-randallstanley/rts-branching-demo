DROP PROCEDURE IF EXISTS etl_imp_school_grades_predictive //

CREATE definer=`dbadmin`@`localhost` procedure etl_imp_school_grades_predictive( p_school_year_id int(10) )
COMMENT '$Rev: 9335 $ $Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $'
CONTAINS SQL
SQL SECURITY INVOKER

/*
$Rev: 9335 $ 
$Author: randall.stanley $ 
$Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_school_grades_predictive.sql $
$Id: etl_imp_school_grades_predictive.sql 9335 2010-10-03 18:10:23Z randall.stanley $ 
*/

BEGIN

    DECLARE v_school_year_id int(10);
    DECLARE v_standard_cutscore smallint(6);
    DECLARE v_lg_cutscore smallint(6);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend); 

    -- ---------------------------------------------------------------------------------------
    -- Check to ensure there is data to process 
    -- ---------------------------------------------------------------------------------------
    SELECT  count(*)
    INTO    @table_exists
    FROM    information_schema.views v
    WHERE   v.table_schema = @db_name AND
            v.table_name = 'v_pmi_ods_state_data_membership_file';

    IF @table_exists > 0 THEN  

        -- Determine current school year 
        IF p_school_year_id IS NULL THEN
            SELECT school_year_id 
            INTO v_school_year_id
            FROM c_school_year WHERE active_flag = 1;
        ELSE 
            -- Use parm override supplied for year
            SET v_school_year_id = p_school_year_id;
        END IF;

        
        -- ---------------------------------------------------------------------------------------
        -- Update predict flag to indicate predictive scores have been populated
        -- ---------------------------------------------------------------------------------------

        INSERT  l_school_grade_year (school_year_id, predict_flag, last_user_id, create_timestamp)
        VALUES (v_school_year_id, 1, 1234, now())
        ON DUPLICATE KEY UPDATE predict_flag = values(predict_flag)
            ,last_user_id = values(last_user_id)
        ;
        
        
        -- ---------------------------------------------------------------------------------------
        -- Create a temp hold table for processing 
        -- ---------------------------------------------------------------------------------------
        DROP TABLE IF EXISTS `tmp_school_grades_predictive`;
        DROP TABLE IF EXISTS `tmp_ayp_subject_grade_filter_list`;

        CREATE TABLE `tmp_school_grades_predictive` (
          `student_id` int(10) default NULL,
          `school_id` int(10) default NULL,
          `school_year_id` int(10) default NULL,
          `grade_level` int(10) default NULL,
          `ayp_subject_code` varchar(25) default NULL,
          `curr_yr_pmi_al` tinyint(1) default NULL,         # REMINDER: Must clean up and reference client al codes
          `prior_yr_pmi_al` tinyint(1) default NULL,        # REMINDER: Must clean up and reference client al codes
          `curr_yr_ayp_score` decimal(9,3) default NULL,
          `prior_yr_ayp_score` decimal(9,3) default NULL,
          `curr_yr_dev_score` decimal(9,3) default NULL,
          `prior_yr_dev_score` decimal(9,3) default NULL,
          `hp_flag` tinyint(1) default NULL,
          `writing_40_flag` tinyint(1) default NULL,  #NC
          `lg_flag` tinyint(1) default NULL,
          `rank` int(10) default NULL,
          `bott_lg_flag` tinyint(1) default NULL,
          KEY `ind_tmp_school_grades_predictive_stu` (`student_id`),
          KEY `ind_tmp_school_grades_predictive_sch` (`school_id`),
          KEY `ind_tmp_school_grades_predictive_sch_grd` (`school_id`,`grade_level`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_ayp_subject_grade_filter_list` (
          `ayp_subject_id` int(10) default NULL,
          `ayp_subject_code` varchar(25) NOT NULL,
          `min_grade_sequence` int(10) default NULL,
          `max_grade_sequence` int(10) default NULL,
          unique key `uq_tmp_ayp_subject_grade_filter_list_code` (`ayp_subject_code`),
          unique key `uq_tmp_ayp_subject_grade_filter_list_id` (`ayp_subject_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        insert tmp_ayp_subject_grade_filter_list (
            ayp_subject_code
            ,min_grade_sequence
            ,max_grade_sequence
        )
        
        values ('fcatMath', 0, 10)
                ,('fcatReading', 0, 10)
                ,('fcatScience', 0, 11)
                ,('fcatWriting', 0, 10)
        ;
        
        update  tmp_ayp_subject_grade_filter_list as upd
        join    c_ayp_subject as sub
                on      upd.ayp_subject_code = sub.ayp_subject_code
        set     upd.ayp_subject_id = sub.ayp_subject_id
        ;

        -- ---------------------------------------------------------------------------------------------
        -- Get all students in membership file and their curr yr ayp achievement levels per subject area
        -- ---------------------------------------------------------------------------------------------
        INSERT INTO tmp_school_grades_predictive (
            student_id
            ,school_id
            ,school_year_id
            ,grade_level
            ,ayp_subject_code
            ,curr_yr_pmi_al
            ,prior_yr_pmi_al
            ,curr_yr_ayp_score
            ,prior_yr_ayp_score
            ,curr_yr_dev_score
            ,prior_yr_dev_score
            ,hp_flag
            ,writing_40_flag   #NC
            ,lg_flag
            ,bott_lg_flag
        )
        SELECT  st.student_id               AS 'student_id', 
                sch.school_id               AS 'school_id', 
                cass.school_year_id         AS 'school_year_id', 
                state.processed_grade_level AS 'grade_level',
                cas.ayp_subject_code        AS 'ayp_subject_code', 
                al.pmi_al                   AS 'curr_yr_pmi_al',           -- REMINDER: Must clean up and reference client al codes
                NULL                        AS 'prior_yr_pmi_al',          -- REMINDER: Must clean up and reference client al codes
                cass.ayp_score              AS 'curr_yr_ayp_score',
                NULL                        AS 'prior_yr_ayp_score',        
                cass.alt_ayp_score          AS 'curr_yr_dev_score',
                NULL                        AS 'prior_yr_dev_score',
                NULL                        AS 'hp_flag',
                NULL                        AS 'writing_40_flag',  #NC
                NULL                        AS 'lg_flag',
                NULL                        AS 'bott_lg_flag' 
        FROM    v_pmi_ods_state_data_membership_file as state
        JOIN    c_student AS st  
                ON      state.sid = st.student_state_code -- AND st.active_flag = 1
        JOIN    c_school  AS sch 
                ON      state.school_of_enrollment = sch.school_state_code
        JOIN    c_ayp_subject_student as cass
                ON      st.student_id = cass.student_id 
                AND     cass.school_year_id = v_school_year_id
                AND     cass.score_record_flag = 1
        JOIN    c_ayp_subject as cas
                ON      cas.ayp_subject_id = cass.ayp_subject_id 
        JOIN    v_pmi_xref_grade_level as xgrd
                ON      state.processed_grade_level = xgrd.client_grade_code
        JOIN    tmp_ayp_subject_grade_filter_list as gf
                ON      cas.ayp_subject_id = gf.ayp_subject_id
        JOIN    c_grade_level as gl
                ON      gl.grade_code = xgrd.pmi_grade_code
                AND     gl.grade_sequence between gf.min_grade_sequence and gf.max_grade_sequence
        JOIN    c_ayp_achievement_level as al
                ON      cass.al_id = al.al_id
        ORDER BY st.student_id, ayp_subject_code
        ;


        -- ------------------------------------------------------------------
        -- Get prior yr ayp achievement levels per subject area
        -- ------------------------------------------------------------------
        UPDATE tmp_school_grades_predictive t
        JOIN c_ayp_subject_student cass
            ON t.student_id        = cass.student_id       
            AND cass.school_year_id = (v_school_year_id - 1)
            AND cass.score_record_flag = 1
        JOIN c_ayp_subject cas
            ON t.ayp_subject_code = cas.ayp_subject_code   AND
               cas.ayp_subject_id = cass.ayp_subject_id 
        JOIN c_ayp_achievement_level al
            ON cass.al_id = al.al_id
        SET prior_yr_pmi_al     = al.pmi_al,
            prior_yr_ayp_score  = cass.ayp_score, 
            prior_yr_dev_score  = cass.alt_ayp_score;


        -- ---------------------------------------------------------------------------------------
        -- Flag High Performance Students per Subject
        -- ---------------------------------------------------------------------------------------
        UPDATE  tmp_school_grades_predictive t
        JOIN c_student cs
            ON  t.student_id = cs.student_id 
        JOIN v_pmi_ods_state_data_membership_file state
            ON  cs.student_state_code = state.sid
            SET t.hp_flag = 
            
                     CASE
                         WHEN (state.processed_swd_code IS NULL OR 
                               SUBSTRING(state.processed_swd_code,1,2) in('LZ','MZ','FZ') OR 
                               SUBSTRING(state.processed_swd_code,1,4) in('LMFZ','LFMZ'))
                               AND (state.ell_change_date IS NULL OR 
                                    datediff(now(), state.ell_change_date)/365 > 2 ) THEN
                         
                            CASE 
                                WHEN t.ayp_subject_code = 'fcatMath'    AND t.curr_yr_pmi_al >= 3      THEN 1
                                WHEN t.ayp_subject_code = 'fcatMath'                                   THEN 0
                                
                                WHEN t.ayp_subject_code = 'fcatReading' AND t.curr_yr_pmi_al >= 3      THEN 1
                                WHEN t.ayp_subject_code = 'fcatReading'                                THEN 0
                                
                                WHEN t.ayp_subject_code = 'fcatScience' AND t.curr_yr_pmi_al >= 3      THEN 1
                                WHEN t.ayp_subject_code = 'fcatScience'                                THEN 0
                                
                                WHEN t.ayp_subject_code = 'fcatWriting' AND t.curr_yr_pmi_al >= 3      THEN 1     -- REWRITE: Writing scores seem more accurate using 3.5 now
                                WHEN t.ayp_subject_code = 'fcatWriting'                                THEN 0     -- REWRITE: Writing scores seem more accurate using 3.5 now
                                                           
                            END
                            
                         ELSE NULL  -- students should not be included in totals
                     END;       
                     
        #NC
        UPDATE  tmp_school_grades_predictive t
        JOIN c_student cs
            ON  t.student_id = cs.student_id 
        JOIN v_pmi_ods_state_data_membership_file state
            ON  cs.student_state_code = state.sid
            SET t.writing_40_flag = 
            
                     CASE
                         WHEN (state.processed_swd_code IS NULL OR 
                               SUBSTRING(state.processed_swd_code,1,2) in('LZ','MZ','FZ') OR 
                               SUBSTRING(state.processed_swd_code,1,4) in('LMFZ','LFMZ'))
                               AND (state.ell_change_date IS NULL OR 
                                    datediff(now(), state.ell_change_date)/365 > 2 ) THEN
                         
                            CASE 
                             
                                WHEN t.ayp_subject_code = 'fcatWriting' AND t.curr_yr_pmi_al >= 4      THEN 1     
                                WHEN t.ayp_subject_code = 'fcatWriting'                                THEN 0   
                                ELSE NULL
                                                           
                            END
                            
                         ELSE NULL  
                     END;                       
        
        #END NC

        -- ---------------------------------------------------------------------------------------
        -- Flag Learning Gain Students per Subject (Reading/Math only)
        -- ---------------------------------------------------------------------------------------
        UPDATE  tmp_school_grades_predictive t
        -- Determine if student's scores are considered a learning gain (lg)
        SET t.lg_flag = 
            CASE
                WHEN t.ayp_subject_code = 'fcatReading' THEN 
                    CASE 
                        -- Only count students who have curr and prior yr achievement levels as qualifying for learning gains
                        WHEN (t.curr_yr_pmi_al IS NULL OR t.prior_yr_pmi_al IS NULL) THEN NULL
                        -- A decrease in AL translates into no LG 
                        WHEN t.curr_yr_pmi_al < t.prior_yr_pmi_al  THEN 0 
                        -- Achievement level (al) of 3 or higher maintained from last year is a lg
                        WHEN (t.curr_yr_pmi_al >= 3 AND t.prior_yr_pmi_al >= 3) and (t.curr_yr_pmi_al >= t.prior_yr_pmi_al) THEN 1 
                        -- 1 or more al gain from prior year score is a lg 
                        WHEN (t.curr_yr_pmi_al - t.prior_yr_pmi_al >= 1) THEN 1 
                        -- 1 yr or more growth in dev score exceeded is a lg even though al = 1 or 2
                        WHEN t.curr_yr_pmi_al in(1,2) AND t.grade_level = 4  AND t.curr_yr_dev_score - t.prior_yr_dev_score > 230 THEN 1 
                        WHEN t.curr_yr_pmi_al in(1,2) AND t.grade_level = 5  AND t.curr_yr_dev_score - t.prior_yr_dev_score > 166 THEN 1
                        WHEN t.curr_yr_pmi_al in(1,2) AND t.grade_level = 6  AND t.curr_yr_dev_score - t.prior_yr_dev_score > 133 THEN 1
                        WHEN t.curr_yr_pmi_al in(1,2) AND t.grade_level = 7  AND t.curr_yr_dev_score - t.prior_yr_dev_score > 110 THEN 1
                        WHEN t.curr_yr_pmi_al in(1,2) AND t.grade_level = 8  AND t.curr_yr_dev_score - t.prior_yr_dev_score > 92  THEN 1
                        WHEN t.curr_yr_pmi_al in(1,2) AND t.grade_level = 9  AND t.curr_yr_dev_score - t.prior_yr_dev_score > 77  THEN 1
                        WHEN t.curr_yr_pmi_al in(1,2) AND t.grade_level = 10 AND t.curr_yr_dev_score - t.prior_yr_dev_score > 77  THEN 1
                        ELSE 0
                    END
                WHEN t.ayp_subject_code = 'fcatMath' THEN 
                    CASE 
                        -- Only count students who have curr and prior yr achievement levels as qualifying for learning gains
                        WHEN (t.curr_yr_pmi_al IS NULL OR t.prior_yr_pmi_al IS NULL) THEN NULL
                        -- A decrease in AL translates into no LG 
                        WHEN t.curr_yr_pmi_al < t.prior_yr_pmi_al  THEN 0 
                        -- Achievement level (al) of 3 or higher maintained from last year is a lg
                        WHEN (t.curr_yr_pmi_al >= 3 AND t.prior_yr_pmi_al >= 3) and (t.curr_yr_pmi_al >= t.prior_yr_pmi_al) THEN 1 
                        -- 1 or more al gain from prior year score is a lg 
                        WHEN (t.curr_yr_pmi_al - t.prior_yr_pmi_al >= 1) THEN 1 
                        -- 1 yr or more growth in dev score exceeded is a lg even though al = 1 or 2
                        WHEN t.curr_yr_pmi_al in(1,2) AND t.grade_level = 4  AND t.curr_yr_dev_score - t.prior_yr_dev_score > 162 THEN 1 
                        WHEN t.curr_yr_pmi_al in(1,2) AND t.grade_level = 5  AND t.curr_yr_dev_score - t.prior_yr_dev_score > 119 THEN 1
                        WHEN t.curr_yr_pmi_al in(1,2) AND t.grade_level = 6  AND t.curr_yr_dev_score - t.prior_yr_dev_score > 95  THEN 1
                        WHEN t.curr_yr_pmi_al in(1,2) AND t.grade_level = 7  AND t.curr_yr_dev_score - t.prior_yr_dev_score > 78  THEN 1
                        WHEN t.curr_yr_pmi_al in(1,2) AND t.grade_level = 8  AND t.curr_yr_dev_score - t.prior_yr_dev_score > 64  THEN 1
                        WHEN t.curr_yr_pmi_al in(1,2) AND t.grade_level = 9  AND t.curr_yr_dev_score - t.prior_yr_dev_score > 54  THEN 1
                        WHEN t.curr_yr_pmi_al in(1,2) AND t.grade_level = 10 AND t.curr_yr_dev_score - t.prior_yr_dev_score > 48  THEN 1
                        ELSE 0
                    END
                ELSE 0
            END     
        WHERE t.ayp_subject_code in('fcatReading','fcatMath');


        -- ------------------------------------------------------------------------------------------
        -- Flag Lower Quartile Learning Gain Students per School, Grade, Subject (Reading/Math only)
        -- ------------------------------------------------------------------------------------------

        UPDATE tmp_school_grades_predictive t
        JOIN (
               SELECT rcalc1.school_id, rcalc1.grade_level, rcalc1.ayp_subject_code,
                     rcalc1.student_id,              
                     CAST(rcalc1.prior_yr_dev_score as signed), 
                     SUM(CASE 
                             WHEN CAST(rcalc1.prior_yr_dev_score AS SIGNED) < CAST(rcalc2.prior_yr_dev_score AS SIGNED) THEN 1 
                             ELSE 0 
                         END) + 1 AS 'rank',
                     COUNT(*) AS 'total_count',  
                     ROUND(((SUM(CASE 
                                     WHEN CAST(rcalc1.prior_yr_dev_score AS SIGNED) < CAST(rcalc2.prior_yr_dev_score AS SIGNED) THEN 1 
                                     ELSE 0 
                                 END) + 1)/count(*))*100) AS 'percentile'
                FROM   tmp_school_grades_predictive rcalc1
                JOIN   tmp_school_grades_predictive rcalc2
                    ON  rcalc1.school_id        = rcalc2.school_id        AND 
                        rcalc1.grade_level      = rcalc2.grade_level      AND
                        rcalc1.ayp_subject_code = rcalc2.ayp_subject_code   
                WHERE 
                        rcalc1.ayp_subject_code = 'fcatReading'   AND
                        rcalc1.prior_yr_pmi_al is not null AND rcalc2.prior_yr_pmi_al is not null AND
                        rcalc1.curr_yr_pmi_al  is not null AND rcalc1.curr_yr_pmi_al is not null  AND
#                        rcalc1.prior_yr_pmi_al in(1,2,3)   AND rcalc2.prior_yr_pmi_al in(1,2,3)   AND
                        rcalc1.prior_yr_dev_score > 1    AND rcalc2.prior_yr_dev_score > 1    AND
                        rcalc1.curr_yr_dev_score  > 1    AND rcalc2.curr_yr_dev_score  > 1              
                GROUP BY rcalc1.school_id, rcalc1.grade_level, rcalc1.ayp_subject_code, rcalc1.student_id
                ORDER BY  rcalc1.school_id, rcalc1.grade_level, rank ) t2
                    ON  t.student_id       = t2.student_id  AND
                    t.ayp_subject_code = t2.ayp_subject_code 
            SET t.rank         = t2.percentile, 
                t.bott_lg_flag = CASE  WHEN t2.percentile >= 75 AND t.lg_flag = 1 AND t.prior_yr_pmi_al in(1,2,3) THEN 1  
                                       WHEN t2.percentile >= 75 AND t.lg_flag = 0 THEN 0 ELSE NULL END; 
                                   
        UPDATE tmp_school_grades_predictive t
        JOIN (
               SELECT rcalc1.school_id, rcalc1.grade_level, rcalc1.ayp_subject_code,
                     rcalc1.student_id,              
                     CAST(rcalc1.prior_yr_dev_score as signed), 
                     SUM(CASE 
                             WHEN CAST(rcalc1.prior_yr_dev_score AS SIGNED) < CAST(rcalc2.prior_yr_dev_score AS SIGNED) THEN 1 
                             ELSE 0 
                         END) + 1 AS 'rank',
                     COUNT(*) AS 'total_count',  
                     ROUND(((SUM(CASE 
                                     WHEN CAST(rcalc1.prior_yr_dev_score AS SIGNED) < CAST(rcalc2.prior_yr_dev_score AS SIGNED) THEN 1 
                                     ELSE 0 
                                 END) + 1)/count(*))*100) AS 'percentile'
                FROM   tmp_school_grades_predictive rcalc1
                JOIN   tmp_school_grades_predictive rcalc2
                    ON  rcalc1.school_id        = rcalc2.school_id        AND 
                        rcalc1.grade_level      = rcalc2.grade_level      AND
                        rcalc1.ayp_subject_code = rcalc2.ayp_subject_code   
                WHERE 
                        rcalc1.ayp_subject_code = 'fcatMath'   AND
                        rcalc1.prior_yr_pmi_al is not null AND rcalc2.prior_yr_pmi_al is not null AND
                        rcalc1.curr_yr_pmi_al  is not null AND rcalc1.curr_yr_pmi_al is not null  AND
#                        rcalc1.prior_yr_pmi_al in(1,2,3)   AND rcalc2.prior_yr_pmi_al in(1,2,3)   AND
                        rcalc1.prior_yr_dev_score > 1    AND rcalc2.prior_yr_dev_score > 1    AND
                        rcalc1.curr_yr_dev_score  > 1    AND rcalc2.curr_yr_dev_score  > 1              
                GROUP BY rcalc1.school_id, rcalc1.grade_level, rcalc1.ayp_subject_code, rcalc1.student_id
                ORDER BY  rcalc1.school_id, rcalc1.grade_level, rank ) t2
                    ON  t.student_id       = t2.student_id  AND
                    t.ayp_subject_code = t2.ayp_subject_code 
            SET t.rank         = t2.percentile, 
                t.bott_lg_flag = CASE  WHEN t2.percentile >= 75 AND t.lg_flag = 1 AND t.prior_yr_pmi_al in(1,2,3) THEN 1  
                                       WHEN t2.percentile >= 75 AND t.lg_flag = 0 THEN 0 ELSE NULL END; 

        -- ---------------------------------------------------------------------------------------
        -- Load l_school_grade_results     
        -- ---------------------------------------------------------------------------------------

        -- Start clean
        TRUNCATE TABLE l_school_grade_results;

        -- 
        -- REWRITE: This process will only handle predictive/actual grades for one year.
        --          It will need to be modified to handle multiple years.
        
        
        -- ---------------------------------------------------------------------------------------
        -- Get cut scores for determining proper coloring for year's scores
        -- ---------------------------------------------------------------------------------------
        SELECT gy.standard_cutscore, gy.lg_cutscore  
        INTO   v_standard_cutscore, v_lg_cutscore
        FROM   l_school_grade_year gy
        WHERE  gy.school_year_id = v_school_year_id 
        ;
        
        -- ---------------------------------------------------------------------------------------
        -- Populate high performance percentages and learning gains for each school 
        -- ---------------------------------------------------------------------------------------
        INSERT INTO l_school_grade_results 
                            (school_id, school_year_id, student_count, letter_grade, grade_points_total, math_points, reading_points, 
                             science_points, writing_points, math_lg_pct, reading_lg_pct, math_low_quartile_lg_pct, reading_low_quartile_lg_pct, 
                             letter_grade_color, math_color, reading_color, science_color, writing_color, math_lg_color, reading_lg_color, 
                             math_low_quartile_lg_color, reading_low_quartile_lg_color, create_timestamp, last_user_id)  
        SELECT sch_grd.school_id,
               sch_grd.school_year_id,
               sch_grd.student_count,
               NULL                           AS 'letter_grade',
               NULL                           AS 'grade_points_total',
               MAX(sch_grd.math_points)       AS 'math_points',
               MAX(sch_grd.reading_points)    AS 'reading_points',   
               MAX(sch_grd.science_points)    AS 'science_points', 
               MAX(sch_grd.writing_points)    AS 'writing_points',
               MAX(sch_grd.math_lg_pct)       AS 'math_lg_pct',
               MAX(sch_grd.reading_lg_pct)    AS 'reading_lg_pct',
               NULL                           AS 'math_low_quartile_lg_pct',
               NULL                           AS 'reading_low_quartile_lg_pct',
               NULL                           AS 'letter_grade_color',
               MIN(sch_grd.math_color)        AS 'math_color',
               MIN(sch_grd.reading_color)     AS 'reading_color',
               MIN(sch_grd.science_color)     AS 'science_color',
               MIN(sch_grd.writing_color)     AS 'writing_color',
               MIN(sch_grd.math_lg_color)     AS 'math_lg_color',
               MIN(sch_grd.reading_lg_color)  AS 'reading_lg_color',
               NULL                           AS 'math_low_quartile_lg_color',
               NULL                           AS 'reading_low_quartile_lg_color',
               NOW()                          AS 'create_timestamp',
               1234
        FROM (SELECT t.school_id          AS 'school_id',
                     t.school_year_id     AS 'school_year_id',
                     t.ayp_subject_code   AS 'ayp_subject_code',
                     COUNT(t.student_id)  AS 'student_count',   
                              
                     CASE WHEN t.ayp_subject_code = 'fcatMath'    THEN ROUND(SUM(t.hp_flag)/COUNT(t.hp_flag) * 100)    END AS 'math_points',
                     CASE WHEN t.ayp_subject_code = 'fcatReading' THEN ROUND(SUM(t.hp_flag)/COUNT(t.hp_flag) * 100)    END AS 'reading_points',
                     CASE WHEN t.ayp_subject_code = 'fcatScience' THEN ROUND(SUM(t.hp_flag)/COUNT(t.hp_flag) * 100)    END AS 'science_points',
                     #NC CASE WHEN t.ayp_subject_code = 'fcatWriting' THEN ROUND(SUM(t.hp_flag)/COUNT(t.hp_flag) * 100)    END AS 'writing_points',
                     #NC Writing Points in 2010 = Average of:  Students scoring 3.0 or higher AND Students scoring 4.0 or higher
                     CASE WHEN t.ayp_subject_code = 'fcatWriting' THEN ROUND( (SUM(t.hp_flag)/COUNT(t.hp_flag) + SUM(t.writing_40_flag)/COUNT(t.hp_flag)) / 2  * 100)    END AS 'writing_points',
                     CASE WHEN t.ayp_subject_code = 'fcatMath'    THEN ROUND(SUM(t.lg_flag)/COUNT(t.lg_flag) * 100)    END AS 'math_lg_pct',
                     CASE WHEN t.ayp_subject_code = 'fcatReading' THEN ROUND(SUM(t.lg_flag)/COUNT(t.lg_flag) * 100)    END AS 'reading_lg_pct',
                                                         
                     CASE WHEN t.ayp_subject_code = 'fcatMath' THEN   
                          CASE  WHEN ROUND(SUM(t.hp_flag)/COUNT(t.hp_flag) * 100) >= 55 THEN 'green' 
                                WHEN ROUND(SUM(t.hp_flag)/COUNT(t.hp_flag) * 100) <  55 THEN 'red' 
                                ELSE NULL 
                          END 
                     END AS 'math_color',
                     CASE WHEN t.ayp_subject_code = 'fcatReading' THEN   
                          CASE  WHEN ROUND(SUM(t.hp_flag)/COUNT(t.hp_flag) * 100) >= 55 THEN 'green'
                                WHEN ROUND(SUM(t.hp_flag)/COUNT(t.hp_flag) * 100) <  55 THEN 'red' 
                                ELSE NULL 
                          END 
                     END AS 'reading_color',
                     CASE WHEN t.ayp_subject_code = 'fcatScience' THEN   
                          CASE  WHEN ROUND(SUM(t.hp_flag)/COUNT(t.hp_flag) * 100) >= 55 THEN 'green' 
                                WHEN ROUND(SUM(t.hp_flag)/COUNT(t.hp_flag) * 100) <  55 THEN 'red' 
                                ELSE NULL 
                          END 
                     END AS 'science_color',
                     CASE WHEN t.ayp_subject_code = 'fcatWriting' THEN   
                          CASE  WHEN ROUND(SUM(t.hp_flag)/COUNT(t.hp_flag) * 100) >= 55 THEN 'green' 
                                WHEN ROUND(SUM(t.hp_flag)/COUNT(t.hp_flag) * 100) <  55 THEN 'red' 
                                ELSE NULL 
                          END 
                     END AS 'writing_color',
   
                     CASE WHEN t.ayp_subject_code = 'fcatMath' THEN   
                          CASE  WHEN ROUND(SUM(t.lg_flag)/COUNT(t.lg_flag) * 100) >= 50  THEN 'green' 
                                WHEN ROUND(SUM(t.lg_flag)/COUNT(t.lg_flag) * 100) <  50  THEN 'red' 
                                ELSE NULL 
                          END 
                     END AS 'math_lg_color',
                     CASE WHEN t.ayp_subject_code = 'fcatReading' THEN   
                          CASE  WHEN ROUND(SUM(t.lg_flag)/COUNT(t.lg_flag) * 100) >= 50  THEN 'green' 
                                WHEN ROUND(SUM(t.lg_flag)/COUNT(t.lg_flag) * 100) <  50  THEN 'red' 
                                ELSE NULL 
                          END 
                     END AS 'reading_lg_color'
              
              FROM   tmp_school_grades_predictive t
              GROUP BY t.school_id, t.ayp_subject_code) AS sch_grd
        GROUP BY sch_grd.school_id;


        -- ---------------------------------------------------------------------------------------
        -- Populate bottom quartile learning gain percentages for each school for Reading/Math only 
        -- ---------------------------------------------------------------------------------------
        UPDATE  l_school_grade_results r
        JOIN (SELECT school_id, t.school_year_id, ayp_subject_code, 
              COUNT(t.student_id), SUM(t.bott_lg_flag),
              ROUND((SUM(t.bott_lg_flag)/count(t.bott_lg_flag))* 100) AS 'reading_bott_lg_pct'
              FROM tmp_school_grades_predictive t                 
              WHERE t.ayp_subject_code = 'fcatReading'  AND t.rank >= 75
              GROUP by school_id, ayp_subject_code) AS lg_reading
            ON r.school_id = lg_reading.school_id AND r.school_year_id = lg_reading.school_year_id
        JOIN (SELECT school_id, t.school_year_id, ayp_subject_code, 
              COUNT(t.student_id), SUM(t.bott_lg_flag),
              ROUND((SUM(t.bott_lg_flag)/count(t.bott_lg_flag))* 100) AS 'math_bott_lg_pct'
              FROM tmp_school_grades_predictive t                 
              WHERE t.ayp_subject_code = 'fcatMath'  AND t.rank >= 75
              GROUP by school_id, ayp_subject_code) AS lg_math
            ON r.school_id = lg_math.school_id AND r.school_year_id = lg_math.school_year_id
        SET r.reading_low_quartile_lg_pct    = lg_reading.reading_bott_lg_pct, 
            r.math_low_quartile_lg_pct       = lg_math.math_bott_lg_pct,
            r.reading_low_quartile_lg_color  = CASE WHEN lg_reading.reading_bott_lg_pct >= v_lg_cutscore THEN 'green' ELSE 'red' END,        
            r.math_low_quartile_lg_color     = CASE WHEN lg_math.math_bott_lg_pct       >= v_lg_cutscore THEN 'green' ELSE 'red' END 
            ; 


        -- ---------------------------------------------------------------------------------------
        -- Populate total points for each school and 
        -- grade the school and assign proper color based upon total points
        -- ---------------------------------------------------------------------------------------
        UPDATE l_school_grade_results r
        JOIN l_color_school_grade sg
            ON  r.school_year_id     BETWEEN sg.begin_year AND sg.end_year 
            AND (ifnull(r.math_points,0)                +
                ifnull(r.reading_points,0)              +
                ifnull(r.writing_points,0)              +
                ifnull(r.science_points,0)              +
                ifnull(r.math_lg_pct,0)                 +
                ifnull(r.reading_lg_pct,0)              +
                ifnull(r.math_low_quartile_lg_pct,0)    +
                ifnull(r.reading_low_quartile_lg_pct,0)) BETWEEN sg.min_score AND sg.max_score 
        JOIN l_school_grade_letter_color glc
            ON  glc.color_id = sg.color_id
        JOIN pmi_color c
            ON  glc.color_id = c.color_id
        SET r.grade_points_total = ifnull(r.math_points,0)              +
                                   ifnull(r.reading_points,0)           +
                                   ifnull(r.writing_points,0)           +
                                   ifnull(r.science_points,0)           +
                                   ifnull(r.math_lg_pct,0)              +
                                   ifnull(r.reading_lg_pct,0)           +
                                   ifnull(r.math_low_quartile_lg_pct,0) +
                                   ifnull(r.reading_low_quartile_lg_pct,0),
            r.letter_grade_color = c.moniker,
            r.letter_grade       = glc.grade_letter
        WHERE r.school_year_id = v_school_year_id;

  
        -- ---------------------------------------------------------------------------------------
        -- Load l_school_grade_stu_results reporting table
        -- ---------------------------------------------------------------------------------------

        -- Start clean
        TRUNCATE TABLE l_school_grade_stu_results;

            
        -- ---------------------------------------------------------------------------------------
        -- Build l_school_grade_stu_results
        -- ---------------------------------------------------------------------------------------
        INSERT INTO l_school_grade_stu_results 
                (school_id, school_year_id, student_id, math_high_std_flag, reading_high_std_flag, science_high_std_flag, writing_high_std_flag, 
                 math_lg_flag, reading_lg_flag, math_low_quartile_flag, reading_low_quartile_flag, math_high_std_color, reading_high_std_color, 
                 science_high_std_color, writing_high_std_color, math_lg_color, reading_lg_color, math_low_quartile_color, 
                 reading_low_quartile_color, last_user_id)   

         SELECT sch_stu.school_id                             AS 'school_id',   
                sch_stu.school_year_id                        AS 'school_year_id',
                sch_stu.student_id                            AS 'student_id',

                MAX(sch_stu.math_high_std_flag)               AS 'math_high_std_flag',
                MAX(sch_stu.reading_high_std_flag)            AS 'reading_high_std_flag',
                MAX(sch_stu.science_high_std_flag)            AS 'science_high_std_flag',
                MAX(sch_stu.writing_high_std_flag)            AS 'writing_high_std_flag',
                MAX(sch_stu.math_lg_flag)                     AS 'math_lg_flag',
                MAX(sch_stu.reading_lg_flag)                  AS 'reading_lg_flag',
                MAX(sch_stu.math_low_quartile_flag)           AS 'math_low_quartile_flag',
                MAX(sch_stu.reading_low_quartile_flag)        AS 'reading_low_quartile_flag',
                
                MIN(sch_stu.math_high_std_color)              AS 'math_high_std_color',
                MIN(sch_stu.reading_high_std_color)           AS 'reading_high_std_color',
                MIN(sch_stu.science_high_std_color)           AS 'science_high_std_color',
                MIN(sch_stu.writing_high_std_color)           AS 'writing_high_std_color',
                MIN(sch_stu.math_lg_color)                    AS 'math_lg_color',
                MIN(sch_stu.reading_lg_color)                 AS 'reading_lg_color',
                MIN(sch_stu.math_low_quartile_color)          AS 'math_low_quartile_color',
                MIN(sch_stu.reading_low_quartile_color)       AS 'reading_low_quartile_color',
                1234                                              
        FROM   (SELECT t.school_id, 
                       t.school_year_id, 
                       t.student_id,
                       t.ayp_subject_code,
                       CASE WHEN t.ayp_subject_code = 'fcatMath'    THEN  t.hp_flag        END AS 'math_high_std_flag',
                       CASE WHEN t.ayp_subject_code = 'fcatReading' THEN  t.hp_flag        END AS 'reading_high_std_flag',
                       CASE WHEN t.ayp_subject_code = 'fcatScience' THEN  t.hp_flag        END AS 'science_high_std_flag',
                       CASE WHEN t.ayp_subject_code = 'fcatWriting' THEN  t.hp_flag        END AS 'writing_high_std_flag',
                       CASE WHEN t.ayp_subject_code = 'fcatMath'    THEN  t.lg_flag        END AS 'math_lg_flag',
                       CASE WHEN t.ayp_subject_code = 'fcatReading' THEN  t.lg_flag        END AS 'reading_lg_flag',
                       CASE WHEN t.ayp_subject_code = 'fcatMath'    THEN  t.bott_lg_flag   END AS 'math_low_quartile_flag',
                       CASE WHEN t.ayp_subject_code = 'fcatReading' THEN  t.bott_lg_flag   END AS 'reading_low_quartile_flag',
                       
                       CASE WHEN t.ayp_subject_code = 'fcatMath'    AND t.hp_flag  = 1     THEN 'green'                      
                            WHEN t.ayp_subject_code = 'fcatMath'    AND t.hp_flag  = 0     THEN 'red'   END AS 'math_high_std_color',
                       CASE WHEN t.ayp_subject_code = 'fcatReading' AND t.hp_flag  = 1     THEN 'green'                      
                            WHEN t.ayp_subject_code = 'fcatReading' AND t.hp_flag  = 0     THEN 'red'   END AS 'reading_high_std_color',                     
                       CASE WHEN t.ayp_subject_code = 'fcatScience' AND t.hp_flag  = 1     THEN 'green' 
                            WHEN t.ayp_subject_code = 'fcatScience' AND t.hp_flag  = 0     THEN 'red'   END AS 'science_high_std_color',                     
                       CASE WHEN t.ayp_subject_code = 'fcatWriting' AND t.hp_flag  = 1     THEN 'green'  
                            WHEN t.ayp_subject_code = 'fcatWriting' AND t.hp_flag  = 0     THEN 'red'   END AS 'writing_high_std_color',                     
                       CASE WHEN t.ayp_subject_code = 'fcatMath'    AND t.lg_flag  = 1     THEN 'green'
                            WHEN t.ayp_subject_code = 'fcatMath'    AND t.lg_flag  = 0     THEN 'red'   END AS 'math_lg_color',
                       CASE WHEN t.ayp_subject_code = 'fcatReading' AND t.lg_flag  = 1     THEN 'green'
                            WHEN t.ayp_subject_code = 'fcatReading' AND t.lg_flag  = 0     THEN 'red'   END AS 'reading_lg_color',                     
                       CASE WHEN t.ayp_subject_code = 'fcatMath'    AND t.bott_lg_flag = 1 THEN 'green' 
                            WHEN t.ayp_subject_code = 'fcatMath'    AND t.bott_lg_flag = 0 THEN 'red'   END AS 'math_low_quartile_color',
                       CASE WHEN t.ayp_subject_code = 'fcatReading' AND t.bott_lg_flag = 1 THEN 'green' 
                            WHEN t.ayp_subject_code = 'fcatReading' AND t.bott_lg_flag = 0 THEN 'red'   END AS 'reading_low_quartile_color'   
                FROM tmp_school_grades_predictive t
                GROUP BY t.student_id, t.school_id, t.school_year_id, t.ayp_subject_code) AS sch_stu
        GROUP BY sch_stu.student_id, sch_stu.school_id, sch_stu.school_year_id  
        ORDER BY sch_stu.student_id;        

        call etl_c_ayp_subject_student_update_lg_bq(v_school_year_id);
        
        DROP TABLE IF EXISTS `tmp_school_grades_predictive`;
        DROP TABLE IF EXISTS `tmp_ayp_subject_grade_filter_list`;

    END IF;        

END
//
