/*
$Rev: 7405 $ 
$Author: randall.stanley $ 
$Date: 2009-07-20 08:27:39 -0400 (Mon, 20 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_baseball_detail_leading.sql $
$Id: etl_rpt_baseball_detail_leading.sql 7405 2009-07-20 12:27:39Z randall.stanley $ 
 */

####################################################################
# Insert leading data into rpt tables for baseball report.
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_rpt_baseball_detail_leading //

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_baseball_detail_leading()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7405 $ $Date: 2009-07-20 08:27:39 -0400 (Mon, 20 Jul 2009) $'

PROC: BEGIN 

    select  school_year_id
    into    @curr_yr_id
    from    c_school_year
    where   active_flag = 1
    ;

    #=========
    DELETE FROM `rpt_baseball_detail_leading`
    WHERE bb_group_id IN (SELECT bb_group_id
                          FROM pm_baseball_group
                          WHERE bb_group_code IN ('lead'));
    #=========


    ##############################################################
    # Insert Leading data
    ##############################################################

    INSERT INTO rpt_baseball_detail_leading
        (
        bb_group_id
        , bb_measure_id
        , bb_measure_item_id
        , student_id
        , school_year_id
        , score
        , score_color
        , last_user_id
        , create_timestamp
        ) 
    # Leading - bm
    SELECT   bbg.bb_group_id
            ,bbm.bb_measure_id
            ,rptlead.ayp_subject_id
            ,rptlead.student_id
            ,@curr_yr_id
            ,CASE WHEN bbm.bb_measure_code = 'BM01' THEN round(rptlead.bm_01_pe/ rptlead.bm_01_pp * 100, 0)
                    WHEN bbm.bb_measure_code = 'BM02' THEN round(rptlead.bm_02_pe/ rptlead.bm_02_pp * 100, 0)
                    WHEN bbm.bb_measure_code = 'BM03' THEN round(rptlead.bm_03_pe/ rptlead.bm_03_pp * 100, 0)
                    WHEN bbm.bb_measure_code = 'BM04' THEN round(rptlead.bm_04_pe/ rptlead.bm_04_pp * 100, 0)
                    WHEN bbm.bb_measure_code = 'BM05' THEN round(rptlead.bm_05_pe/ rptlead.bm_05_pp * 100, 0)
                    WHEN bbm.bb_measure_code = 'BM06' THEN round(rptlead.bm_06_pe/ rptlead.bm_06_pp * 100, 0)
                    WHEN bbm.bb_measure_code = 'BM07' THEN round(rptlead.bm_07_pe/ rptlead.bm_07_pp * 100, 0)
                    WHEN bbm.bb_measure_code = 'BM08' THEN round(rptlead.bm_08_pe/ rptlead.bm_08_pp * 100, 0)
                    WHEN bbm.bb_measure_code = 'BMAve' THEN round(rptlead.total_pe/ rptlead.total_pp * 100, 0)
                    ELSE NULL END AS score
            ,CASE WHEN bbm.bb_measure_code = 'BM01' THEN rptlead.bm_01_color
                    WHEN bbm.bb_measure_code = 'BM02' THEN rptlead.bm_02_color
                    WHEN bbm.bb_measure_code = 'BM03' THEN rptlead.bm_03_color
                    WHEN bbm.bb_measure_code = 'BM04' THEN rptlead.bm_04_color
                    WHEN bbm.bb_measure_code = 'BM05' THEN rptlead.bm_05_color
                    WHEN bbm.bb_measure_code = 'BM06' THEN rptlead.bm_06_color
                    WHEN bbm.bb_measure_code = 'BM07' THEN rptlead.bm_07_color
                    WHEN bbm.bb_measure_code = 'BM08' THEN rptlead.bm_08_color
                    WHEN bbm.bb_measure_code = 'BMAve' THEN rptlead.wavg_color
                    ELSE NULL END AS color
            ,1234
            ,CURRENT_TIMESTAMP
    FROM    rpt_profile_leading_subject_stu AS rptlead
    CROSS JOIN  pm_baseball_group AS bbg
            ON      bbg.bb_group_code = 'lead'
    JOIN    pm_baseball_measure AS bbm
            ON      bbm.bb_group_id = bbg.bb_group_id
    ;
                    

END PROC;
//
