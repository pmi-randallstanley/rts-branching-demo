
DROP PROCEDURE IF EXISTS etl_pm_natl_test_scores_sat_md_calvertnet//

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_natl_test_scores_sat_md_calvertnet()
COMMENT '$Rev: 7380 $ $Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $'
CONTAINS SQL
SQL SECURITY INVOKER

/*
$Rev: 7380 $ 
$Author: randall.stanley $ 
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_natl_test_scores_sat_md_calvertnet.sql $
$Id: etl_pm_natl_test_scores_sat_md_calvertnet.sql 7380 2009-07-16 14:23:58Z randall.stanley $ 
*/

BEGIN

    ####################################################################
    #
    # Insert SAT data into pm_natl_test_scores for md_calvertnet
    # 
    ####################################################################


    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_ltdb_pentamation';

    if @view_exists > 0 then
    
        ##############################################################
        # Get md_calvertnet specific scores into standard sat view
        ##############################################################

        DROP TABLE IF EXISTS tmp_sat_md_calvertnet;
        
       CREATE TABLE tmp_sat_md_calvertnet
        SELECT   s.student_code AS studentid
                        ,STR_TO_DATE(test_date,  '%m/%d/%Y') AS test_date                            
                        ,p.score01 AS 'verbal_score'
                        ,p.score02 AS 'math_score'
                        ,p.score03 AS 'writing_score'                     
        FROM      v_pmi_ods_ltdb_pentamation p
            JOIN    c_student s
                ON     s.student_code = p.student_id
       WHERE p.test_name like 'SAT%';
            
        ##############################################################
        # Insert SAT scores
        ##############################################################
     
        INSERT INTO pm_natl_test_scores (
            test_type_id
            ,subject_id
            ,student_id
            ,test_year
            ,test_month
            ,score
            ,last_user_id
            ,create_timestamp
            )            
            SELECT 
                dt.test_type_id
                ,dt.subject_id
                ,dt.student_id
                ,dt.test_year
                ,dt.test_month
                ,dt.score
                ,1234
                ,now()
            FROM (
                    SELECT 
                        tty.test_type_id
                        ,tsub.subject_id
                        ,st.student_id
                        ,YEAR(odsv.test_date) AS test_year
                        ,MONTH(odsv.test_date) AS test_month
                        ,(CASE WHEN tsub.subject_code like '%Math'
                                    THEN odsv.math_score
                                WHEN tsub.subject_code like '%Verbal'
                                    THEN odsv.verbal_score
                                WHEN tsub.subject_code like '%Writing'
                                    THEN odsv.writing_score
                                WHEN tsub.subject_code like '%satMvsum'
                                    THEN (odsv.math_score + odsv.verbal_score)
                                WHEN tsub.subject_code like '%satMvwsum'
                                    THEN (odsv.math_score + odsv.verbal_score + odsv.writing_score)
                                ELSE NULL
                                END) AS score
                    FROM  tmp_sat_md_calvertnet AS odsv
                    JOIN c_student st
                        ON st.student_code = odsv.studentid
                    JOIN pm_natl_test_type AS tty
                        ON  tty.test_type_code = 'sat'
                    JOIN pm_natl_test_subject AS tsub
                        ON  tsub.test_type_id = tty.test_type_id
                        AND tsub.subject_code NOT LIKE '%Max%'                
                ) AS dt
            WHERE dt.score > 100
        ON DUPLICATE key UPDATE last_user_id = 1234
            ,score = dt.score
        ;


    ####################################################################
    # Insert MV/MVW Max Sums:
    ####################################################################

    INSERT INTO pm_natl_test_scores (
        test_type_id
        ,subject_id
        ,student_id
        ,test_year
        ,test_month
        ,score
        ,last_user_id
        ,create_timestamp
        )

        SELECT 
            dt.test_type_id
            ,dt.subject_id
            ,dt.student_id
            ,dt.test_year
            ,dt.test_month
            ,dt.score
            ,1234
            ,now()
        FROM (
                SELECT 
                    maxer.test_type_id
                    ,tsub2.subject_id
                    ,maxer.student_id
                    ,MAX(maxer.test_year) AS test_year
                    ,MAX(ts2.test_month) AS test_month
                    ,CASE WHEN tsub2.subject_code LIKE '%MvSum' THEN max(maxer.max_math) + MAX(maxer.max_verbal)
                            WHEN tsub2.subject_code LIKE '%MvwSum' THEN max(maxer.max_math) + MAX(maxer.max_verbal) + MAX(maxer.max_writing)
                            ELSE NULL END AS score
                FROM 
                    (SELECT 
                        ts.test_type_id
                        ,ts.student_id
                        ,CASE WHEN tsub.subject_code LIKE '%Math' THEN MAX(ts.score) ELSE NULL END AS max_math
                        ,CASE WHEN tsub.subject_code LIKE '%Verbal' THEN MAX(ts.score) ELSE NULL END AS max_verbal
                        ,CASE WHEN tsub.subject_code LIKE '%Writing' THEN MAX(ts.score) ELSE NULL END AS max_writing
                        ,MAX(ts.test_year) AS test_year
                        
                    FROM pm_natl_test_scores AS ts
                    JOIN pm_natl_test_subject AS tsub
                        ON   tsub.subject_id = ts.subject_id
                        AND  tsub.test_type_id = ts.test_type_id
                    JOIN pm_natl_test_type AS tty
                        ON   tty.test_type_id = ts.test_type_id
                        AND  tty.test_type_code IN ('sat')
                    GROUP BY ts.student_id
                            ,ts.test_type_id
                            ,ts.subject_id  ) AS maxer
                JOIN pm_natl_test_subject AS tsub2
                    ON   tsub2.test_type_id = maxer.test_type_id
                    AND  tsub2.subject_code LIKE '%Max%'
                JOIN pm_natl_test_scores AS ts2
                    ON   ts2.test_type_id = maxer.test_type_id
                    AND  ts2.student_id = maxer.student_id
                    AND  ts2.test_year = maxer.test_year
                GROUP BY maxer.student_id
                        ,maxer.test_type_id
                        ,tsub2.subject_code
            ) AS dt
        WHERE dt.score > 100

        ON DUPLICATE key UPDATE last_user_id = 1234
            ,score = dt.score
        ;
   
        ####################################################################
        # Cleanup
        #################################################################### 
        
        DROP TABLE IF EXISTS tmp_sat_md_calvertnet;
        
    end if;
        
END;
//
