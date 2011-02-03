DROP PROCEDURE IF EXISTS etl_imp_school_grades_actual //

CREATE definer=`dbadmin`@`localhost` procedure etl_imp_school_grades_actual( p_school_year_id int(10) )
COMMENT '$Rev: 7380 $ $Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $'
CONTAINS SQL
SQL SECURITY INVOKER

/*
$Rev: 7380 $ 
$Author: randall.stanley $ 
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_school_grades_actual.sql $
$Id: etl_imp_school_grades_actual.sql 7380 2009-07-16 14:23:58Z randall.stanley $ 
*/

BEGIN

    DECLARE v_school_year_id int(10);
    DECLARE v_standard_cutscore smallint(6);
    DECLARE v_lg_cutscore smallint(6);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend); 

     -- Check to ensure there is data to process 
    SELECT  count(*)
    INTO    @table_exists
    FROM    information_schema.views v
    WHERE   v.table_schema = @db_name
    and     v.table_name = 'v_pmi_ods_fl_indv';

    IF @table_exists > 0 THEN

        select  count(*)
        into    @view_count
        from    v_pmi_ods_fl_indv
        ;
        
        if @view_count > 0 then
   
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
            -- Insert l_school_grade_year if doesn't exist yet; if does exist, flip predictive flag off
            -- ---------------------------------------------------------------------------------------
    
            INSERT  l_school_grade_year (school_year_id, predict_flag, last_user_id, create_timestamp)
            VALUES (v_school_year_id, 0, 1234, now())
            ON DUPLICATE KEY UPDATE predict_flag = values(predict_flag)
                ,last_user_id = values(last_user_id)
            ;
          
            ####################################
            ## Load l_school_grade_results    ##
            ####################################
        
            
            -- Start clean
            TRUNCATE TABLE l_school_grade_results;
            
        
            -- Sum key scoring indicators for each school
            # comment join to c_student ref SF case #7235
            INSERT INTO l_school_grade_results 
                (school_id, school_year_id, student_count, letter_grade, grade_points_total, reading_points, math_points, writing_points, science_points,  
                reading_lg_pct, math_lg_pct, reading_low_quartile_lg_pct, math_low_quartile_lg_pct, letter_grade_color, math_color, reading_color, writing_color, science_color, 
                math_lg_color, reading_lg_color, math_low_quartile_lg_color, reading_low_quartile_lg_color, create_timestamp, last_user_id)
            SELECT sch.school_id,   
               v_school_year_id AS 'student_year_id',
               COUNT(*) AS 'student_count',
               NULL AS 'letter_grade',
               round(sum(state.sch_gr_reading_hi_perform_numer)/sum(state.sch_gr_reading_hi_perform_denom) * 100)                    +
               round(sum(state.sch_gr_math_hi_perform_numer)/sum(state.sch_gr_math_hi_perform_denom) * 100)                          + 
               round(sum(state.sch_gr_writing_hi_perform_numer)/sum(state.sch_gr_writing_hi_perform_denom) * 100)                    +
               round(sum(state.sch_gr_sci_hi_perform_numer)/sum(state.sch_gr_sci_hi_perform_denom) * 100)                            +
               round(sum(state.sch_grades_reading_gains_numerator)/sum(state.sch_grades_reading_gains_denominator) * 100)            +
               round(sum(state.sch_grades_math_gains_numerator)/sum(state.sch_grades_math_gains_denominator) * 100)                  +
               round(sum(state.sch_grades_lo_25_pct_reading_gains_numer)/sum(state.sch_grades_lo_25_pct_reading_gains_denom) * 100)  +
               round(sum(state.sch_grades_lo_25_pct_math_gains_numer)/sum(state.sch_grades_lo_25_pct_math_gains_denom) * 100)        AS  'grades_point_total',
        
               round(sum(state.sch_gr_reading_hi_perform_numer)/sum(state.sch_gr_reading_hi_perform_denom) * 100)                    AS 'reading_points',
               round(sum(state.sch_gr_math_hi_perform_numer)/sum(state.sch_gr_math_hi_perform_denom) * 100)                          AS 'math_points',
               round(sum(state.sch_gr_writing_hi_perform_numer)/sum(state.sch_gr_writing_hi_perform_denom) * 100)                    AS 'writing_points',
               round(sum(state.sch_gr_sci_hi_perform_numer)/sum(state.sch_gr_sci_hi_perform_denom) * 100)                            AS 'science_points',
               round(sum(state.sch_grades_reading_gains_numerator)/sum(state.sch_grades_reading_gains_denominator) * 100)            AS 'reading_lg_pct',
               round(sum(state.sch_grades_math_gains_numerator)/sum(state.sch_grades_math_gains_denominator) * 100)                  AS 'math_lg_pct',
               round(sum(state.sch_grades_lo_25_pct_reading_gains_numer)/sum(state.sch_grades_lo_25_pct_reading_gains_denom) * 100)  AS 'reading_low_quartile_lg_pct',
               round(sum(state.sch_grades_lo_25_pct_math_gains_numer)/sum(state.sch_grades_lo_25_pct_math_gains_denom) * 100)        AS 'math_low_quartile_lg_pct',
               NULL  AS 'letter_grade_color', 
               NULL  AS 'math_color', 
               NULL  AS 'reading_color', 
               NULL  AS 'science_color', 
               NULL  AS 'writing_color', 
               NULL  AS 'math_lg_color', 
               NULL  AS 'reading_lg_color', 
               NULL  AS 'math_low_quartile_lg_color', 
               NULL  AS 'reading_low_quartile_lg_color', 
               NOW() AS 'create_timestamp',
               1234  AS 'last_user_id'
        
            -- REWRITE: This process will only handle predictive/actual grades for one year.
            --          It will need to be modified to handle multiple years.
             FROM v_pmi_ods_fl_indv state
    #            JOIN c_student as st ON state.student_id = st.student_state_code
                JOIN c_school as sch ON state.schl_nbr = sch.school_state_code
        -- where sch_gr_reading_hi_perform_numer = 1
        -- where sch_gr_reading_hi_perform_denom = 0
            GROUP BY sch.school_id;
        
        
            -- Grade the School and Assign Proper Color Based Upon Total Points
            UPDATE l_school_grade_results r
            JOIN l_color_school_grade sg
                ON r.school_year_id BETWEEN sg.begin_year AND sg.end_year 
                AND r.grade_points_total BETWEEN sg.min_score AND sg.max_score 
            JOIN l_school_grade_letter_color glc
                ON glc.color_id = sg.color_id
            JOIN pmi_color c
                ON glc.color_id = c.color_id
             SET r.letter_grade_color = c.moniker,
                    r.letter_grade = glc.grade_letter;
            
        
            -- Get cut scores for determining proper coloring for year's actual
            SELECT gy.standard_cutscore, gy.lg_cutscore  
            INTO   v_standard_cutscore, v_lg_cutscore
            FROM   l_school_grade_year gy
                WHERE  gy.school_year_id = v_school_year_id 
                      AND gy.predict_flag = 0;
        
        
            -- Assign proper colors to subject categories based upon cutoffs
            # comment setting of 6 color values re SF case #7235
            UPDATE l_school_grade_results r
                SET r.math_low_quartile_lg_color =    (CASE WHEN (r.math_low_quartile_lg_pct    >= v_lg_cutscore)       THEN 'green' ELSE 'red'  END),
                    r.reading_low_quartile_lg_color = (CASE WHEN (r.reading_low_quartile_lg_pct >= v_lg_cutscore)       THEN 'green' ELSE 'red'  END)
    #                r.math_color    =                 (CASE WHEN (r.math_points                 >= v_standard_cutscore) THEN 'green' ELSE 'red'  END),
    #                r.reading_color =                 (CASE WHEN (r.reading_points              >= v_standard_cutscore) THEN 'green' ELSE 'red'  END),
    #                r.science_color =                 (CASE WHEN (r.science_points              >= v_standard_cutscore) THEN 'green' ELSE 'red'  END),
    #                r.writing_color =                 (CASE WHEN (r.writing_points              >= v_standard_cutscore) THEN 'green' ELSE 'red'  END),
    #                r.math_lg_color =                 (CASE WHEN (r.math_lg_pct                 >= v_lg_cutscore)       THEN 'green' ELSE 'red'  END),
    #                r.reading_lg_color =              (CASE WHEN (r.reading_lg_pct              >= v_lg_cutscore)       THEN 'green' ELSE 'red'  END),
            ;
        
        
            #####################################
            ## Load l_school_grade_stu_results ##
            #####################################
        
           
            -- Start clean
            TRUNCATE TABLE l_school_grade_stu_results;
        
            -- Set flags (0 or 1) for students in key school grading areas
            INSERT INTO l_school_grade_stu_results 
                (school_id, school_year_id, student_id, math_high_std_flag, reading_high_std_flag, science_high_std_flag, writing_high_std_flag, math_lg_flag, reading_lg_flag, 
                 math_low_quartile_flag, reading_low_quartile_flag, math_high_std_color, reading_high_std_color, science_high_std_color, writing_high_std_color,math_lg_color,
                 reading_lg_color, math_low_quartile_color, reading_low_quartile_color, last_user_id)   
            SELECT sch.school_id,   
                v_school_year_id AS 'student_year_id',
                    st.student_id,
                CASE 
                    WHEN sch_gr_math_hi_perform_denom = 1 THEN state.sch_gr_math_hi_perform_numer ELSE Null
                END as 'math_high_std_flag',
                CASE 
                    WHEN state.sch_gr_reading_hi_perform_denom = 1 THEN state.sch_gr_reading_hi_perform_numer ELSE Null
                END as 'reading_high_std_flag',
                CASE 
                    WHEN state.sch_gr_sci_hi_perform_denom = 1 THEN state.sch_gr_sci_hi_perform_numer ELSE Null
                END as 'science_high_std_flag',
                CASE 
                    WHEN state.sch_gr_writing_hi_perform_denom = 1 THEN state.sch_gr_writing_hi_perform_numer ELSE Null
                END as 'writing_high_std_flag',
                CASE 
                    WHEN state.sch_grades_math_gains_denominator = 1 THEN state.sch_grades_math_gains_numerator ELSE Null
                END as 'math_lg_flag',
                CASE 
                    WHEN state.sch_grades_reading_gains_denominator = 1 THEN state.sch_grades_reading_gains_numerator ELSE Null
                END as 'reading_lg_flag',
                CASE
                    WHEN state.sch_grades_lo_25_pct_math_gains_denom = 1 THEN state.sch_grades_lo_25_pct_math_gains_numer ELSE NULL
                END as 'math_low_quartile_flag',
                CASE 
                    WHEN state.sch_grades_lo_25_pct_reading_gains_denom = 1 THEN state.sch_grades_lo_25_pct_reading_gains_numer ELSE NULL
                END as 'reading_low_quartile_flag',
                NULL as 'math_high_std_color',
                NULL as 'reading_high_std_color',
                NULL as 'science_high_std_color',
                NULL as 'writing_high_std_color',
                NULL as 'math_lg_color',
                NULL as 'reading_lg_color',
                NULL as 'math_low_quartile_color',
                NULL as 'reading_low_quartile_color',
                1234
            FROM   v_pmi_ods_fl_indv state
            JOIN c_student as st 
                ON state.student_id = st.student_state_code
            JOIN c_school as sch 
                ON state.schl_nbr = sch.school_state_code;
        
        
            -- Assign proper colors to subject categories based upon cutoffs
            UPDATE l_school_grade_stu_results r
            SET r.math_high_std_color =       
                CASE 
                WHEN r.math_high_std_flag    = 1 THEN 'green'
                WHEN r.math_high_std_flag        = 0 THEN 'red'  
                
                END,
            r.reading_high_std_color =     
                CASE 
                WHEN r.reading_high_std_flag     = 1 THEN 'green' 
                WHEN r.reading_high_std_flag     = 0 THEN 'red'  
                
                END,
            r.science_high_std_color =     
                CASE 
                WHEN r.science_high_std_flag     = 1 THEN 'green' 
                WHEN r.science_high_std_flag     = 0 THEN 'red'  
                
            END,
            r.writing_high_std_color =     
                CASE 
                WHEN r.writing_high_std_flag     = 1 THEN 'green' 
                WHEN r.writing_high_std_flag     = 0 THEN 'red'  
                  
            END,
            r.math_lg_color =     
                CASE 
                WHEN r.math_lg_flag          = 1 THEN 'green' 
                WHEN r.math_lg_flag              = 0 THEN 'red'  
                
            END,
                r.reading_lg_color =     
                CASE 
                WHEN r.reading_lg_flag           = 1 THEN 'green' 
                WHEN r.reading_lg_flag           = 0 THEN 'red'  
                
            END,
            r.math_low_quartile_color =     
                CASE 
                WHEN r.math_low_quartile_flag    = 1 THEN 'green' 
                WHEN r.math_low_quartile_flag    = 0 THEN 'red'  
                
            END,
            r.reading_low_quartile_color =    
                CASE 
                WHEN r.reading_low_quartile_flag = 1 THEN 'green'
                WHEN r.reading_low_quartile_flag = 0 THEN 'red'  
               
            END;
    
            call etl_c_ayp_subject_student_update_lg_bq(v_school_year_id);
    
            -- Update imp_upload_log
            set @sql_string := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_fl_indv', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
            
            prepare sql_string from @sql_string;
            execute sql_string;
            deallocate prepare sql_string;     
            
        end if;
        
    END IF;
    
END
//
