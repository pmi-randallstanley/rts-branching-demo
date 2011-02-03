/*
$Rev: 7380 $ 
$Author: randall.stanley $ 
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_dibels_forf.sql $
$Id: etl_rpt_dibels_forf.sql 7380 2009-07-16 14:23:58Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_rpt_dibels_forf//

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_dibels_forf`()
CONTAINS SQL
SQL SECURITY INVOKER
comment '$Rev: 7380 $ $Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $'
BEGIN

call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend); 

select  COALESCE(cast(value as SIGNED),0) 
into    @dibels_assess_freq_id 
from    pmi_client_settings 
where   client_setting_code = 'pmDibelAssessFreqId'
;

INSERT INTO rpt_dibels_scores (
        measure_period_id
        ,student_id
        ,school_year_id
        ,score
        ,last_user_id
        ,create_timestamp
        )
SELECT *
FROM (SELECT  xdmp.measure_period_id,
                s.student_id,
                sy.school_year_id,
                MAX(CASE 
                        WHEN dm.measure_code = 'ISF' AND dafp.period_code = 1 THEN f.ISF_1
                        WHEN dm.measure_code = 'ISF' AND dafp.period_code = 2 THEN f.ISF_2
                        WHEN dm.measure_code = 'ISF' AND dafp.period_code = 3 THEN f.ISF_3
                        WHEN dm.measure_code = 'LNF' AND dafp.period_code = 1 THEN f.LNF_1
                        WHEN dm.measure_code = 'LNF' AND dafp.period_code = 2 THEN f.LNF_2
                        WHEN dm.measure_code = 'LNF' AND dafp.period_code = 3 THEN f.LNF_3
                        WHEN dm.measure_code = 'PSF' AND dafp.period_code = 1 THEN f.PSF_1
                        WHEN dm.measure_code = 'PSF' AND dafp.period_code = 2 THEN f.PSF_2
                        WHEN dm.measure_code = 'PSF' AND dafp.period_code = 3 THEN f.PSF_3
                        WHEN dm.measure_code = 'NWF' AND dafp.period_code = 1 THEN f.NWF_1
                        WHEN dm.measure_code = 'NWF' AND dafp.period_code = 2 THEN f.NWF_2
                        WHEN dm.measure_code = 'NWF' AND dafp.period_code = 3 THEN f.NWF_3
                        WHEN dm.measure_code = 'ORF' AND dafp.period_code = 1 THEN f.orf_1
                        WHEN dm.measure_code = 'ORF' AND dafp.period_code = 2 THEN f.orf_2
                        WHEN dm.measure_code = 'ORF' AND dafp.period_code = 3 THEN f.orf_3
                    END) AS score,
                1234 AS user_id,
                current_timestamp() AS create_timestamp
        FROM v_pmi_ods_forf f
            JOIN c_student s
                ON  s.student_state_code = f.student_id
            JOIN c_student_year sy
                ON  s.student_id = sy.student_id
                AND sy.school_year_id = f.current_year
            JOIN c_grade_level gl
                ON   sy.grade_level_id = gl.grade_level_id
            JOIN pm_dibels_measure AS dm
            JOIN pm_dibels_assess_freq_period AS dafp
                ON   dafp.freq_id = @dibels_assess_freq_id
            JOIN pm_dibels_measure_period AS xdmp
                ON   dm.measure_id  = xdmp.measure_id
                AND  dafp.period_id = xdmp.period_id
                AND  dafp.freq_id   = xdmp.freq_id
                AND  gl.grade_level_id = xdmp.grade_level_id
        GROUP BY  xdmp.measure_period_id,
                s.student_id,
                sy.school_year_id) dt
WHERE dt.score IS NOT null               
ON DUPLICATE key UPDATE last_user_id = 1234
        ,score = dt.score;
        
INSERT INTO rpt_dibels_scores (
        measure_period_id
        ,student_id
        ,school_year_id
        ,score
        ,last_user_id
        ,create_timestamp
        )
SELECT *
FROM (SELECT  xdmp.measure_period_id,
                s.student_id,
                sy.school_year_id,
                MAX(CASE 
                        WHEN dm.measure_code = 'ISF' AND dafp.period_code = 1 THEN f.ISF_1
                        WHEN dm.measure_code = 'ISF' AND dafp.period_code = 2 THEN f.ISF_2
                        WHEN dm.measure_code = 'ISF' AND dafp.period_code = 3 THEN f.ISF_3
                        WHEN dm.measure_code = 'LNF' AND dafp.period_code = 1 THEN f.LNF_1
                        WHEN dm.measure_code = 'LNF' AND dafp.period_code = 2 THEN f.LNF_2
                        WHEN dm.measure_code = 'LNF' AND dafp.period_code = 3 THEN f.LNF_3
                        WHEN dm.measure_code = 'PSF' AND dafp.period_code = 1 THEN f.PSF_1
                        WHEN dm.measure_code = 'PSF' AND dafp.period_code = 2 THEN f.PSF_2
                        WHEN dm.measure_code = 'PSF' AND dafp.period_code = 3 THEN f.PSF_3
                        WHEN dm.measure_code = 'NWF' AND dafp.period_code = 1 THEN f.NWF_1
                        WHEN dm.measure_code = 'NWF' AND dafp.period_code = 2 THEN f.NWF_2
                        WHEN dm.measure_code = 'NWF' AND dafp.period_code = 3 THEN f.NWF_3
                        WHEN dm.measure_code = 'ORF' AND dafp.period_code = 1 THEN f.orf_1
                        WHEN dm.measure_code = 'ORF' AND dafp.period_code = 2 THEN f.orf_2
                        WHEN dm.measure_code = 'ORF' AND dafp.period_code = 3 THEN f.orf_3
                    END) AS score,
                1234 AS user_id,
                current_timestamp() AS create_timestamp
        FROM v_pmi_ods_forf f
            JOIN c_student s
                ON  s.student_code = f.student_id
            JOIN c_student_year sy
                ON  s.student_id = sy.student_id
                AND sy.school_year_id = f.current_year
            JOIN c_grade_level gl
                ON   sy.grade_level_id = gl.grade_level_id
            JOIN pm_dibels_measure AS dm
            JOIN pm_dibels_assess_freq_period AS dafp
                ON   dafp.freq_id = @dibels_assess_freq_id
            JOIN pm_dibels_measure_period AS xdmp
                ON   dm.measure_id  = xdmp.measure_id
                AND  dafp.period_id = xdmp.period_id
                AND  dafp.freq_id   = xdmp.freq_id
                AND  gl.grade_level_id = xdmp.grade_level_id
        GROUP BY  xdmp.measure_period_id,
                s.student_id,
                sy.school_year_id) dt
WHERE dt.score IS NOT null               
ON DUPLICATE key UPDATE last_user_id = 1234
        ,score = dt.score;       
        
-- Update imp_upload_log
SET @sql_string := '';
SET @sql_string := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_forf', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');

prepare sql_string from @sql_string;
execute sql_string;
deallocate prepare sql_string;     

END;
//
