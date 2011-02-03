/*
$Rev: 7380 $ 
$Author: randall.stanley $ 
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_hst_ltdb_pentamation.sql $
$Id: etl_hst_ltdb_pentamation.sql 7380 2009-07-16 14:23:58Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_hst_ltdb_pentamation//

CREATE definer=`dbadmin`@`localhost` procedure `etl_hst_ltdb_pentamation`()
    SQL SECURITY INVOKER
    COMMENT '$Rev: 7380 $ $Date: 2008-09-02 13:16:24 -0400 (Tue, 02 Sep 2008)'
BEGIN 

declare v_date_format_mask varchar(15) default '%m/%d/%Y';

call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);  

# stage any new data - this is issued here
# becuase this process is currently executing
# outside of etl_imp()
set @sqltext := concat('call ', @db_name_ods, '.imp_process_upload_log()');
prepare sqltext from @sqltext;
execute sqltext;
deallocate prepare sqltext;

# check for new upload of HST Pentamation data
set @sqltext := concat('select max(ul.upload_id) into @new_upload_id from ', @db_name_ods, '.imp_upload_log ul ');
set @sqltext := concat(@sqltext, ' join ', @db_name_ods, '.imp_table t on ul.table_id = t.table_id and ul.upload_status_code = \'', 'c', '\'');
set @sqltext := concat(@sqltext, ' where t.target_table_name = \'', 'pmi_ods_ltdb_pentamation', '\'');

prepare sqltext from @sqltext;
execute sqltext;
deallocate prepare sqltext;


# load new data if received
if @new_upload_id > 1 then 
    
    SELECT  count(*) 
    INTO    @view_exists
    FROM    information_schema.tables t
    WHERE   t.table_schema = @db_name_core
    AND     t.table_name = 'v_pmi_ods_ltdb_pentamation';
    
    IF @view_exists > 0 THEN
    
        DROP TABLE IF EXISTS tmp_v_pmi_ods_ltdb_pentamation_pivot;
        
        CREATE TABLE tmp_v_pmi_ods_ltdb_pentamation_pivot
        (test_key char(10),
        subtest_name char(15),
        student_id char(10),
        test_date char(15),
        score_typ char(10),
        score char(10),
        KEY `ind_student_id` (`student_id`),
        KEY `ind_test_key` (`test_key`)); 
        
        INSERT INTO tmp_v_pmi_ods_ltdb_pentamation_pivot
        SELECT p.test_key, p.subtest_name, p.student_id, p.test_date, p.score01_typ, p.score01
        FROM v_pmi_ods_ltdb_pentamation p JOIN c_student s ON p.student_id = s.student_code WHERE test_key in (10042, 10041) AND score01_typ IS NOT NULL
        UNION ALL
        SELECT p.test_key, p.subtest_name, p.student_id, p.test_date, p.score02_typ, p.score02 
        FROM v_pmi_ods_ltdb_pentamation p JOIN c_student s ON p.student_id = s.student_code  WHERE test_key in (10042, 10041) AND score02_typ IS NOT NULL 
        UNION all
        SELECT p.test_key, p.subtest_name, p.student_id, p.test_date, p.score03_typ, p.score03 
        FROM v_pmi_ods_ltdb_pentamation p JOIN c_student s ON p.student_id = s.student_code  WHERE test_key in (10042, 10041) AND score03_typ IS NOT NULL 
        UNION all
        SELECT p.test_key, p.subtest_name, p.student_id, p.test_date, p.score04_typ, p.score04 
        FROM v_pmi_ods_ltdb_pentamation p JOIN c_student s ON p.student_id = s.student_code  WHERE test_key in (10042, 10041) AND score04_typ IS NOT NULL 
        UNION all
        SELECT p.test_key, p.subtest_name, p.student_id, p.test_date, p.score05_typ, p.score05 
        FROM v_pmi_ods_ltdb_pentamation p JOIN c_student s ON p.student_id = s.student_code  WHERE test_key in (10042, 10041) AND score05_typ IS NOT NULL 
        UNION all
        SELECT p.test_key, p.subtest_name, p.student_id, p.test_date, p.score06_typ, p.score06 
        FROM v_pmi_ods_ltdb_pentamation p JOIN c_student s ON p.student_id = s.student_code  WHERE test_key in (10042, 10041) AND score06_typ IS NOT NULL;
        
        DELETE FROM tmp_v_pmi_ods_ltdb_pentamation_pivot WHERE score BETWEEN 'a' AND 'z';
                  
        select max(s.school_id)
        into @school_id
        from c_school s
          inner join c_school_type st
            on s.school_type_id = st.school_type_id
        where st.school_type_code = 'hs';
            
        
        insert c_student_year (student_id, school_year_id, school_id, grade_level_id, active_flag, last_user_id, create_timestamp, last_edit_timestamp, client_id)
        select s.student_id
          ,sy.school_year_id
          ,MAX(COALESCE(sch.school_id, @school_id))
          ,gl.grade_level_id as grade_level_id
          ,1 as active_flag
          ,1234 as last_user_id
          ,now()
          ,now()
          ,@client_id as client_id
        FROM (SELECT m.student_id
                ,STR_TO_DATE(m.test_date,v_date_format_mask) test_date
                ,MAX(building) AS test_school
                ,scy.school_year_id
                ,coalesce(m.grade, gll.grade_code) grade_level
              FROM v_pmi_ods_ltdb_pentamation m
                INNER JOIN c_school_year scy
                  ON STR_TO_DATE(m.test_date,v_date_format_mask) between scy.begin_date AND scy.end_date
                INNER JOIN c_student s
                  ON s.student_code = m.student_id
                INNER JOIN c_student_year sy
                  ON sy.student_id = s.student_id
                INNER JOIN c_grade_level gl
                  ON sy.grade_level_id = gl.grade_level_id
                INNER JOIN c_grade_level gll
                  ON (gl.grade_code + (scy.school_year_id - sy.school_year_id)) = gll.grade_code
                GROUP BY m.student_id, scy.school_year_id, STR_TO_DATE(m.test_date,v_date_format_mask)) m
        JOIN c_student AS s
          ON s.student_code = m.student_id
        JOIN c_school_year sy
          ON m.test_date BETWEEN sy.begin_date AND sy.end_date
        LEFT JOIN c_school sch
          ON sch.school_code = m.test_school
        JOIN (SELECT client_grade_code, pmi_grade_code
              FROM md_calvertnet_ods.pmi_xref_grade_level
              union all
              SELECT pmi_grade_code as client_grade_code, pmi_grade_code
              FROM md_calvertnet_ods.pmi_xref_grade_level
              WHERE pmi_grade_code between 1 AND 12) g
          ON m.grade_level = g.client_grade_code        
        JOIN c_grade_level gl
          ON gl.grade_code = g.pmi_grade_code
        GROUP BY s.student_id
          ,sy.school_year_id
        ON DUPLICATE key UPDATE last_user_id = 1234, last_edit_timestamp = now();
        
        insert c_ayp_subject_student (student_id
        ,ayp_subject_id
        ,school_year_id
        ,ayp_score
        ,al_id
        ,score_type_code
        ,last_user_id
        ,create_timestamp)
        select dt.student_id
          ,dt.ayp_subject_id
          ,dt.school_year_id
          ,dt.score
          ,null
          ,'n' 
          ,1234
          ,current_timestamp
        from      
        (SELECT  s.student_id
            ,sub.ayp_subject_id
            ,sty.school_year_id
            ,max(m.score01) score
        FROM   v_pmi_ods_ltdb_pentamation AS m
          inner join c_ayp_subject sub
              on sub.ayp_subject_code = 
                case 
                  when m.test_key = 10041 AND subtest_name = 'BIOLOGY'    then 'hsaBiology'
                  when m.test_key = 10041 AND subtest_name = 'ALGEBRA'    then 'hsaAlgebra'
                  when m.test_key = 10041 AND subtest_name = 'GOVERNMENT' then 'hsaGovernment'
                  when m.test_key = 10041 AND subtest_name = 'GEOMETRY'   then 'hsaGeometry'
                  when m.test_key = 10041 AND subtest_name = 'ENGLISH'    then 'hsaEnglish'
                  when m.test_key = 10042 AND subtest_name = 'READING'    then 'msaReading'
                  when m.test_key = 10042 AND subtest_name = 'MATH'       then 'msaMath'
                  when m.test_key = 10042 AND subtest_name = 'SCIENCE'    then 'msaScience'
                end 
          inner join c_student AS s
            on s.student_code = m.student_id
          inner join c_school_year sy
            on STR_TO_DATE(test_date,v_date_format_mask) BETWEEN sy.begin_date AND sy.end_date        
          inner join c_student_year AS sty
            on sty.student_id = s.student_id
            and sty.school_year_id = sy.school_year_id
        where m.score01 > 1
        group by s.student_id
            ,sub.ayp_subject_id
            ,sty.school_year_id) dt
        ON DUPLICATE KEY UPDATE last_user_id = 1234,ayp_score = dt.score, last_edit_timestamp = now();
        
        insert c_ayp_strand_student (student_id
        ,ayp_subject_id
        ,ayp_strand_id
        ,school_year_id
        ,ayp_score
        ,score_type_code
        ,last_user_id
        ,create_timestamp)
        select dt.student_id
          ,dt.ayp_subject_id
          ,dt.ayp_strand_id
          ,dt.school_year_id 
          ,dt.score
          ,'n' 
          ,1234
          ,current_timestamp
        from     
        (SELECT s.student_id
            ,str.ayp_subject_id
            ,str.ayp_strand_id
            ,sty.school_year_id
            ,max(CAST(m.score AS signed)) score
        FROM    tmp_v_pmi_ods_ltdb_pentamation_pivot AS m
        JOIN        c_ayp_strand AS str
          ON str.ayp_strand_id = 
                        CASE 
                            WHEN m.subtest_name like 'READING %1' AND m.test_key = '10042' AND m.score_typ = 'GENL' THEN 1024039
                            WHEN m.subtest_name like 'READING %1' AND m.test_key = '10042' AND m.score_typ = 'INFO' THEN 1024040
                            WHEN m.subtest_name like 'READING %1' AND m.test_key = '10042' AND m.score_typ = 'LITR' THEN 1024041
                            WHEN m.subtest_name like 'MATH %1'    AND m.test_key = '10042' AND m.score_typ = 'ALGE' THEN 1024034
                            WHEN m.subtest_name like 'MATH %1'    AND m.test_key = '10042' AND m.score_typ = 'GEOM' THEN 1024035
                            WHEN m.subtest_name like 'MATH %1'    AND m.test_key = '10042' AND m.score_typ = 'NUMB' THEN 1024036
                            WHEN m.subtest_name like 'MATH %1'    AND m.test_key = '10042' AND m.score_typ = 'PROC' THEN 1024037
                            WHEN m.subtest_name like 'MATH %1'    AND m.test_key = '10042' AND m.score_typ = 'STAT' THEN 1024038
                            WHEN m.subtest_name like 'GEOMETR %1' AND m.test_key = '10041' AND m.score_typ = 'RAGF' THEN 1024026
                            WHEN m.subtest_name like 'GEOMETR %1' AND m.test_key = '10041' AND m.score_typ = 'SPCM' THEN 1024027
                            WHEN m.subtest_name like 'GEOMETR %1' AND m.test_key = '10041' AND m.score_typ = 'SPGP' THEN 1024028
                            WHEN m.subtest_name like 'ENGLISH %1' AND m.test_key = '10041' AND m.score_typ = 'CS' THEN 1024022
                            WHEN m.subtest_name like 'ENGLISH %1' AND m.test_key = '10041' AND m.score_typ = 'COMP' THEN 1024022
                            WHEN m.subtest_name like 'ENGLISH %1' AND m.test_key = '10041' AND m.score_typ = 'CWL' THEN 1024023
                            WHEN m.subtest_name like 'ENGLISH %1' AND m.test_key = '10041' AND m.score_typ = 'LANG' THEN 1024023
                            WHEN m.subtest_name like 'ENGLISH %1' AND m.test_key = '10041' AND m.score_typ = 'EVAL' THEN 1024024
                            WHEN m.subtest_name like 'ENGLISH %1' AND m.test_key = '10041' AND m.score_typ = 'RRL' THEN 1024025
                            WHEN m.subtest_name like 'ENGLISH %1' AND m.test_key = '10041' AND m.score_typ = 'C/I' THEN 1024025
                            WHEN m.subtest_name like 'BIOLOGY %1' AND m.test_key = '10041' AND m.score_typ = 'IOB' THEN 1024018
                            WHEN m.subtest_name like 'BIOLOGY %1' AND m.test_key = '10041' AND m.score_typ = 'IT' THEN 1024017
                            WHEN m.subtest_name like 'BIOLOGY %1' AND m.test_key = '10041' AND m.score_typ = 'MEC' THEN 1024019
                            WHEN m.subtest_name like 'BIOLOGY %1' AND m.test_key = '10041' AND m.score_typ = 'SFBM' THEN 1024021
                            WHEN m.subtest_name like 'BIOLOGY %1' AND m.test_key = '10041' AND m.score_typ = 'SFCO' THEN 1024042
                            WHEN m.subtest_name like 'BIOLOGY %1' AND m.test_key = '10041' AND m.score_typ = 'SPB' THEN 1024020
                            WHEN m.subtest_name like 'ALGEBRA %1' AND m.test_key = '10041' AND m.score_typ = 'APF' THEN 1024014
                            WHEN m.subtest_name like 'ALGEBRA %1' AND m.test_key = '10041' AND m.score_typ = 'COAD' THEN 1024013
                            WHEN m.subtest_name like 'ALGEBRA %1' AND m.test_key = '10041' AND m.score_typ = 'MRWS' THEN 1024015
                            WHEN m.subtest_name like 'ALGEBRA %1' AND m.test_key = '10041' AND m.score_typ = 'USMP' THEN 1024016
                            WHEN m.subtest_name like 'GOVERNM %1' AND m.test_key = '10041' AND m.score_typ = 'EPIP' THEN 1024029
                            WHEN m.subtest_name like 'GOVERNM %1' AND m.test_key = '10041' AND m.score_typ = 'GSFP' THEN 1024033
                            WHEN m.subtest_name like 'GOVERNM %1' AND m.test_key = '10041' AND m.score_typ = 'IGGP' THEN 1024030
                            WHEN m.subtest_name like 'GOVERNM %1' AND m.test_key = '10041' AND m.score_typ = 'PRMO' THEN 1024031
                            WHEN m.subtest_name like 'GOVERNM %1' AND m.test_key = '10041' AND m.score_typ = 'SGFP' THEN 1024032
                            WHEN m.subtest_name = 'SCIENCE' AND m.test_key = '10042' AND m.score_typ = 'SKPR' THEN 1626083
                            WHEN m.subtest_name = 'SCIENCE' AND m.test_key = '10042' AND m.score_typ = 'EASP' THEN 1626080
                            WHEN m.subtest_name = 'SCIENCE' AND m.test_key = '10042' AND m.score_typ = 'LFSC' THEN 1626081
                            WHEN m.subtest_name = 'SCIENCE' AND m.test_key = '10042' AND m.score_typ = 'CHEM' THEN 1626078
                            WHEN m.subtest_name = 'SCIENCE' AND m.test_key = '10042' AND m.score_typ = 'PHYS' THEN 1626082
                            WHEN m.subtest_name = 'SCIENCE' AND m.test_key = '10042' AND m.score_typ = 'ENVI' THEN 1626079                        
                        END
        JOIN    c_student AS s
                ON  s.student_code = m.student_id
        JOIN        c_school_year sy
                        ON            STR_TO_DATE(test_date,v_date_format_mask) BETWEEN sy.begin_date AND sy.end_date        
        JOIN    c_student_year AS sty
                ON      sty.student_id = s.student_id
                AND     sty.school_year_id = sy.school_year_id
        where     m.score rlike '[[:digit:]]' AND m.score > 1 -- AND subtest_name like '%1'
        group by s.student_id
            ,str.ayp_subject_id
            ,str.ayp_strand_id
            ,sty.school_year_id) dt
        ON DUPLICATE KEY UPDATE last_user_id = 1234,ayp_score = dt.score, last_edit_timestamp = now();
        
        # update score colors moved to the scores tables.
        update c_ayp_subject_student as ss
        join    c_student_year as sty
                on      sty.student_id = ss.student_id
                and     sty.school_year_id = ss.school_year_id
        join    c_grade_level as gl
                on      gl.grade_level_id = sty.grade_level_id
        left join    c_color_ayp_subject as csub
                on      csub.ayp_subject_id = ss.ayp_subject_id
                and     ss.school_year_id between csub.begin_year and csub.end_year
                and     gl.grade_sequence between csub.begin_grade_sequence and csub.end_grade_sequence
                and     round(ss.ayp_score,0) between csub.min_score and csub.max_score
        left join    pmi_color as clr
                on      clr.color_id = csub.color_id
        set     ss.ayp_score_color = coalesce(clr.moniker, 'white')
        ;
    
        update  c_ayp_strand_student as ss
        join    c_student_year as sty
                on      sty.student_id = ss.student_id
                and     sty.school_year_id = ss.school_year_id
        join    c_grade_level as gl
                on      gl.grade_level_id = sty.grade_level_id
        left join   c_color_ayp_strand as cstr
                on      cstr.ayp_subject_id = ss.ayp_subject_id
                and     cstr.ayp_strand_id = ss.ayp_strand_id
                and     ss.school_year_id between cstr.begin_year and cstr.end_year
                and     gl.grade_sequence between cstr.begin_grade_sequence and cstr.end_grade_sequence
                and     round(ss.ayp_score,1) between cstr.min_score and cstr.max_score
        left join    pmi_color as clr
                on      clr.color_id = cstr.color_id
        set     ss.ayp_score_color = coalesce(clr.moniker, 'white')
        ;
    
        call etl_c_ayp_subject_student_update_al();
        
        UPDATE c_student AS s
        JOIN    (SELECT ss.student_id
                      ,SUM(ss.ayp_score) AS comp_score
                      ,COUNT(DISTINCT ss.ayp_subject_id) AS sub_count
                      ,SUM(attl.pass_flag) AS pass_sum
              FROM c_ayp_subject AS sub
              INNER JOIN c_ayp_subject_student AS ss
                ON ss.ayp_subject_id = sub.ayp_subject_id
              INNER JOIN c_ayp_test_type_al AS attl
                ON attl.al_id = ss.al_id
                AND attl.ayp_test_type_id = sub.ayp_test_type_id
              WHERE sub.grad_report_flag = 1
                AND NOT EXISTS (SELECT * 
                                FROM c_ayp_subject_student AS ess
                                WHERE ess.student_id = ss.student_id
                                  AND ess.ayp_subject_id = ss.ayp_subject_id
                                  AND( ess.ayp_score > ss.ayp_score
                                  OR (ess.ayp_score = ss.ayp_score
                                  AND ess.school_year_id > ss.school_year_id)))
                GROUP BY ss.student_id
              ) AS dt
              ON  dt.student_id = s.student_id
        SET s.all_grad_subs_tested_flag = CASE WHEN dt.sub_count <> 4 THEN 0 ELSE 1 end
          ,s.grad_reqs_met_flag = CASE WHEN dt.comp_score >= 1602 AND dt.sub_count = 4 THEN 1 ELSE 0 END
          ,s.grad_comp_score = CASE WHEN dt.comp_score < 1602 THEN (dt.comp_score - 1602) ELSE dt.comp_score END
        WHERE   s.grad_exempt_flag = 0
        ; 
      
        # load new c_ayp_year_class table for classroom summary
        insert  c_ayp_year_class (class_id, school_year_id, last_user_id, create_timestamp)
        select  cle.class_id, ss.school_year_id, 1234, now()
        from    c_ayp_subject_student as ss
        join    c_class_enrollment as cle
                on      ss.student_id = cle.student_id
        group by cle.class_id, ss.school_year_id
        on duplicate key update last_user_id = 1234
        ;
    
        # load new c_student grad_eligible_flag
        update  c_student st 
        join    c_ayp_subject_student cass
                on    st.student_id = cass.student_id
        join    c_ayp_subject s
                on    cass.ayp_subject_id = s.ayp_subject_id
                and   s.grad_report_flag = 1
        set st.grad_eligible_flag = 1
        ;
    
        DROP TABLE IF EXISTS tmp_v_pmi_ods_ltdb_pentamation_pivot;

        -- Update imp_upload_log
        SET @sqltext := '';
        SET @sqltext := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_ltdb_pentamation', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
        
        prepare sqltext from @sqltext;
        execute sqltext;
        deallocate prepare sqltext;     
    
    end if;

end if;

END
//
