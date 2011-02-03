/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_c_ayp_strand_tx.sql $
$Id: etl_c_ayp_strand_tx.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
 */
DROP PROCEDURE IF EXISTS etl_c_ayp_strand_tx//

CREATE definer=`dbadmin`@`localhost` procedure `etl_c_ayp_strand_tx`()
 CONTAINS SQL
 SQL SECURITY INVOKER
 COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
BEGIN 

        DECLARE v_ayp_subject_id, v_ayp_strand_id, v_pp, v_begin_year, v_begin_grade_sequence, v_end_year, v_end_grade_sequence int;
        DECLARE v_strand_source, v_pe varchar(1000);
        DECLARE no_more_rows BOOLEAN;

        DECLARE cur_etl_strand CURSOR FOR
        SELECT  cs.ayp_subject_id
                                ,cs.ayp_strand_id
                                ,c.moniker
                                ,cs.pp
                                ,concat_ws(' ',c.moniker, '/', cs.pp)
                                ,cs.begin_year
                                ,cs.begin_grade_sequence
                                ,cs.end_year
                                ,cs.end_grade_sequence
                FROM v_imp_table_column_ayp_strand cs
                    JOIN    v_imp_etl_calc_column c 
                        ON      cs.table_id = c.table_id
                        AND     cs.column_id = c.column_id;

    DECLARE CONTINUE HANDLER FOR NOT FOUND 
      SET no_more_rows = TRUE;

                
   OPEN cur_etl_strand;

   loop_cur_etl_strand: LOOP
      FETCH   cur_etl_strand 
      INTO      v_ayp_subject_id,
                            v_ayp_strand_id,
                            v_pe,
                            v_pp,
                            v_strand_source,
                            v_begin_year,
                            v_begin_grade_sequence,
                            v_end_year,
                            v_end_grade_sequence;
   
      IF no_more_rows THEN
            CLOSE cur_etl_strand;
            LEAVE loop_cur_etl_strand;
      END IF;
            
            SET @sql_text := concat('INSERT c_ayp_strand_student (student_id, ayp_subject_id, ayp_strand_id, school_year_id, ayp_score, score_type_code, last_user_id, create_timestamp)');
            SET @sql_text := concat(@sql_text, ' SELECT  dt.student_id, dt.ayp_subject_id, dt.ayp_strand_id, dt.school_year_id, dt.ayp_score, \'n\', 1234, current_timestamp ');
            SET @sql_text := concat(@sql_text, ' FROM (');
            SET @sql_text := concat(@sql_text, '     SELECT  s.student_id, sub.ayp_subject_id, str.ayp_strand_id, sty.school_year_id');
            SET @sql_text := CONCAT(@sql_text, '         ,(CASE WHEN ', v_pe, ' = \'' '\' THEN NULL WHEN ', v_pe, ' IS NOT NULL THEN ', v_strand_source, ' ELSE  NULL END) AS ayp_score');
            SET @sql_text := CONCAT(@sql_text, '     FROM    v_pmi_ods_taks AS m ');
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_ayp_subject AS sub   ON  sub.ayp_subject_id = ', v_ayp_subject_id); 
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_ayp_strand AS str    ON  str.ayp_subject_id = sub.ayp_subject_id AND str.ayp_strand_id = ', v_ayp_strand_id);
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_student AS s         ON  s.student_code = m.student_id');
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_student_year AS sty  ON  sty.student_id = s.student_id AND sty.school_year_id = CAST(m.school_year_manual AS signed)');
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_grade_level AS g     ON  g.grade_level_id = sty.grade_level_id  AND g.grade_sequence BETWEEN ', v_begin_grade_sequence, ' AND ', v_end_grade_sequence);
            SET @sql_text := CONCAT(@sql_text, '     WHERE   ', v_pp, ' >= 1 ');
            SET @sql_text := CONCAT(@sql_text, '         AND ', v_pe, ' is not null');
            SET @sql_text := concat(@sql_text, '         AND  m.school_year_manual BETWEEN ', v_begin_year, ' AND ', v_end_year);
            SET @sql_text := CONCAT(@sql_text, '     GROUP BY s.student_id, sub.ayp_subject_id, str.ayp_strand_id, sty.school_year_id');
            SET @sql_text := CONCAT(@sql_text, '     HAVING  min(cast(concat(cast(right(test_date,2) as signed) + 2000, \'-\',left(test_date,2), \'-01\') as date)) ');
            SET @sql_text := concat(@sql_text, '     ) dt');
            SET @sql_text := CONCAT(@sql_text, ' ON DUPLICATE KEY UPDATE last_user_id = 1234, ayp_score = dt.ayp_score');

            prepare stmt from @sql_text;
            execute stmt;
            deallocate prepare stmt;
            -- select @sql_text;
                        
   END LOOP loop_cur_etl_strand;

END
//
