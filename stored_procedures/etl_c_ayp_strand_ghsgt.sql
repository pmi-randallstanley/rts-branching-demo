/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_c_ayp_strand_ghsgt.sql $
$Id: etl_c_ayp_strand_ghsgt.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
COMMENT '$Rev: 6928 $'
 */
DROP PROCEDURE IF EXISTS etl_c_ayp_strand_ghsgt//
CREATE definer=`dbadmin`@`localhost` procedure etl_c_ayp_strand_ghsgt()
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
CONTAINS SQL
SQL SECURITY INVOKER

BEGIN
        DECLARE v_ayp_subject_id, v_ayp_strand_id int;
        DECLARE v_strand_source, v_pe, v_pp, v_tbl varchar(1000);
        DECLARE no_more_rows BOOLEAN;
        DECLARE cur_etl_strand CURSOR FOR
        select st.ayp_subject_id, st.ayp_strand_id, dt1.moniker, COALESCE(dt2.moniker, 1), concat_ws(' ',dt1.moniker, dt2.operand, dt2.moniker) as strand_source, CASE it.table_id WHEN 1000038 THEN 'v_pmi_ghsgt' ELSE 'v_pmi_ods_crct' END AS tbl
                from v_imp_etl_calc_column etl_c
                    join (select etl1.table_id, etl1.column_id, c1.moniker
                                    from v_imp_table_column c1
                                        join v_imp_etl_calc_column_list etl1 on
                                            c1.table_id = etl1.table_id and
                                            c1.column_id = etl1.source_column_id and
                                            etl1.calc_order = 1) dt1 on
                        etl_c.table_id = dt1.table_id and
                        etl_c.column_id = dt1.column_id
                    left join (select etl2.table_id, etl2.column_id, c2.moniker, etl2.operand
                                    from v_imp_table_column c2
                                        join v_imp_etl_calc_column_list etl2 on
                                            c2.table_id = etl2.table_id and
                                            c2.column_id = etl2.source_column_id and
                                            etl2.calc_order = 2) dt2 on
                        etl_c.table_id = dt2.table_id and
                        etl_c.column_id = dt2.column_id
                join v_imp_table_column_ayp_strand st on
                        st.table_id = etl_c.table_id and
                        st.column_id = etl_c.column_id
                JOIN    v_imp_table AS it
                        ON      etl_c.table_id = it.table_id
                        AND     it.target_table_name IN ('pmi_ods_ga_ghsgt');
    DECLARE CONTINUE HANDLER FOR NOT FOUND
    SET no_more_rows = TRUE;

   OPEN cur_etl_strand;
   loop_cur_etl_strand: LOOP
      FETCH cur_etl_strand
      INTO v_ayp_subject_id,
           v_ayp_strand_id,
           v_pe,
           v_pp,
           v_strand_source,
           v_tbl;

      IF no_more_rows THEN
            CLOSE cur_etl_strand;
            LEAVE loop_cur_etl_strand;
      END IF;
            SET @sql_text := '';
            SET @sql_text := concat('INSERT c_ayp_strand_student (student_id, ayp_subject_id, ayp_strand_id, school_year_id, ayp_score, score_type_code, last_user_id, create_timestamp)');
            SET @sql_text := concat(@sql_text, ' SELECT  dt.student_id, dt.ayp_subject_id, dt.ayp_strand_id, dt.school_year_id, dt.student_ayp_score, \'n\', 1234, current_timestamp ');
            SET @sql_text := concat(@sql_text, ' FROM (');
            SET @sql_text := concat(@sql_text, '     SELECT  s.student_id, sub.ayp_subject_id, str.ayp_strand_id, sty.school_year_id');
            SET @sql_text := CONCAT(@sql_text, '         ,max(CASE WHEN ', v_pe, ' = \'' '\' THEN NULL WHEN ', v_pe, ' IS NOT NULL THEN ', v_strand_source, ' ELSE  NULL END) AS student_ayp_score');
            SET @sql_text := CONCAT(@sql_text, '     FROM    ', v_tbl ,' AS m ');
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_ayp_subject AS sub  ON  sub.ayp_subject_id = ', v_ayp_subject_id);
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_ayp_strand AS str   ON  str.ayp_subject_id = sub.ayp_subject_id AND str.ayp_strand_id = ', v_ayp_strand_id);
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_student AS s        ON  s.student_state_code = m.stuid');
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_student_year AS sty ON  sty.student_id = s.student_id ');
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_school_year sy      ON  cast(concat(case when length(m.testyr) = 2 then concat(20, m.testyr) when length(m.testyr) = 1 then concat(200, m.testyr) else m.testyr end,\'-\',case when length(m.testmo) = 1 then concat(0,m.testmo) else m.testmo end,\'-01\') as date) between sy.begin_date and end_date');
            SET @sql_text := CONCAT(@sql_text, '         and sy.school_year_id = sty.school_year_id');
            SET @sql_text := CONCAT(@sql_text, '     WHERE ', v_pp, ' >= 1 and s.student_state_code > 1');
            SET @sql_text := CONCAT(@sql_text, '     GROUP BY s.student_id, sub.ayp_subject_id, str.ayp_strand_id, sty.school_year_id');
--             SET @sql_text := CONCAT(@sql_text, '     HAVING  min(cast(concat(case when length(m.testyr) = 2 then concat(20, testyr) when length(m.testyr) = 1 then concat(200, testyr) else testyr end, \'-\', testmo, \'-01\') as date)) ');
            SET @sql_text := concat(@sql_text, '     ) dt');
            SET @sql_text := CONCAT(@sql_text, ' ON DUPLICATE KEY UPDATE last_user_id = 1234, ayp_score = student_ayp_score');

            prepare stmt from @sql_text;
            -- select @sql_text;
            execute stmt;
            deallocate prepare stmt;

            SET @sql_text := '';
            SET @sql_text := concat('INSERT c_ayp_strand_student (student_id, ayp_subject_id, ayp_strand_id, school_year_id, ayp_score, score_type_code, last_user_id, create_timestamp)');
            SET @sql_text := concat(@sql_text, ' SELECT  dt.student_id, dt.ayp_subject_id, dt.ayp_strand_id, dt.school_year_id, dt.student_ayp_score, \'n\', 1234, current_timestamp ');
            SET @sql_text := concat(@sql_text, ' FROM (');
            SET @sql_text := concat(@sql_text, '     SELECT  s.student_id, sub.ayp_subject_id, str.ayp_strand_id, sty.school_year_id');
            SET @sql_text := CONCAT(@sql_text, '         ,max(CASE WHEN ', v_pe, ' = \'' '\' THEN NULL WHEN ', v_pe, ' IS NOT NULL THEN ', v_strand_source, ' ELSE  NULL END) AS student_ayp_score');
            SET @sql_text := CONCAT(@sql_text, '     FROM    ', v_tbl ,' AS m ');
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_ayp_subject AS sub  ON  sub.ayp_subject_id = ', v_ayp_subject_id);
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_ayp_strand AS str   ON  str.ayp_subject_id = sub.ayp_subject_id AND str.ayp_strand_id = ', v_ayp_strand_id);
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_student AS s        ON  s.fid_code = m.stuid');
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_student_year AS sty ON  sty.student_id = s.student_id ');
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_school_year sy      ON  cast(concat(case when length(m.testyr) = 2 then concat(20, m.testyr) when length(m.testyr) = 1 then concat(200, m.testyr) else m.testyr end,\'-\',case when length(m.testmo) = 1 then concat(0,m.testmo) else m.testmo end,\'-01\') as date) between sy.begin_date and end_date');
            SET @sql_text := CONCAT(@sql_text, '         and sy.school_year_id = sty.school_year_id');
            SET @sql_text := CONCAT(@sql_text, '     WHERE ', v_pp, ' >= 1 and s.fid_code > 1' );
            SET @sql_text := CONCAT(@sql_text, '     GROUP BY s.student_id, sub.ayp_subject_id, str.ayp_strand_id, sty.school_year_id');
--             SET @sql_text := CONCAT(@sql_text, '     HAVING  min(cast(concat(case when length(m.testyr) = 2 then concat(20, testyr) when length(m.testyr) = 1 then concat(200, testyr) else testyr end, \'-\', testmo, \'-01\') as date)) ');
            SET @sql_text := concat(@sql_text, '     ) dt');
            SET @sql_text := CONCAT(@sql_text, ' ON DUPLICATE KEY UPDATE last_user_id = 1234, ayp_score = student_ayp_score');

            prepare stmt from @sql_text;
            -- select @sql_text;
            execute stmt;
            deallocate prepare stmt;
            
            SET @sql_text := '';
            SET @sql_text := concat('INSERT c_ayp_strand_student (student_id, ayp_subject_id, ayp_strand_id, school_year_id, ayp_score, score_type_code, last_user_id, create_timestamp)');
            SET @sql_text := concat(@sql_text, ' SELECT  dt.student_id, dt.ayp_subject_id, dt.ayp_strand_id, dt.school_year_id, dt.student_ayp_score, \'n\', 1234, current_timestamp ');
            SET @sql_text := concat(@sql_text, ' FROM (');
            SET @sql_text := concat(@sql_text, '     SELECT  s.student_id, sub.ayp_subject_id, str.ayp_strand_id, sty.school_year_id');
            SET @sql_text := CONCAT(@sql_text, '         ,max(CASE WHEN ', v_pe, ' = \'' '\' THEN NULL WHEN ', v_pe, ' IS NOT NULL THEN ', v_strand_source, ' ELSE  NULL END) AS student_ayp_score');
            SET @sql_text := CONCAT(@sql_text, '     FROM    ', v_tbl ,' AS m ');
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_ayp_subject AS sub  ON  sub.ayp_subject_id = ', v_ayp_subject_id);
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_ayp_strand AS str   ON  str.ayp_subject_id = sub.ayp_subject_id AND str.ayp_strand_id = ', v_ayp_strand_id);
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_student AS s        ON  s.student_code = m.stuid');
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_student_year AS sty ON  sty.student_id = s.student_id ');
            SET @sql_text := CONCAT(@sql_text, '     JOIN    c_school_year sy      ON  cast(concat(case when length(m.testyr) = 2 then concat(20, m.testyr) when length(m.testyr) = 1 then concat(200, m.testyr) else m.testyr end,\'-\',case when length(m.testmo) = 1 then concat(0,m.testmo) else m.testmo end,\'-01\') as date) between sy.begin_date and end_date');
            SET @sql_text := CONCAT(@sql_text, '         and sy.school_year_id = sty.school_year_id');
            SET @sql_text := CONCAT(@sql_text, '     WHERE ', v_pp, ' >= 1 and s.student_code > 1' );
            SET @sql_text := CONCAT(@sql_text, '     GROUP BY s.student_id, sub.ayp_subject_id, str.ayp_strand_id, sty.school_year_id');
--             SET @sql_text := CONCAT(@sql_text, '     HAVING  min(cast(concat(case when length(m.testyr) = 2 then concat(20, testyr) when length(m.testyr) = 1 then concat(200, testyr) else testyr end, \'-\', testmo, \'-01\') as date)) ');
            SET @sql_text := concat(@sql_text, '     ) dt');
            SET @sql_text := CONCAT(@sql_text, ' ON DUPLICATE KEY UPDATE last_user_id = 1234, ayp_score = student_ayp_score');

            prepare stmt from @sql_text;
            -- select @sql_text;
            execute stmt;
            deallocate prepare stmt;

   END LOOP loop_cur_etl_strand;
END
//
