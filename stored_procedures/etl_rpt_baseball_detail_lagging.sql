
/*
$Rev: 7410 $ 
$Author: randall.stanley $ 
$Date: 2009-07-20 11:04:59 -0400 (Mon, 20 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_baseball_detail_lagging.sql $
$Id: etl_rpt_baseball_detail_lagging.sql 7410 2009-07-20 15:04:59Z randall.stanley $ 
 */

####################################################################
# Insert lagging data into rpt tables for baseball report.
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_rpt_baseball_detail_lagging //

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_baseball_detail_lagging()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7410 $ $Date: 2009-07-20 11:04:59 -0400 (Mon, 20 Jul 2009) $'

PROC: BEGIN 

    #=========
    DELETE FROM `rpt_baseball_detail_lagging`
    WHERE bb_group_id IN (SELECT bb_group_id
                          FROM pm_baseball_group
                          WHERE bb_group_code IN ('lag'));
    #=========


    ##############################################################
    # Insert lagging data
    ##############################################################

        INSERT INTO rpt_baseball_detail_lagging
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
    SELECT * FROM (
        # Lagging - Scale Score
        SELECT bbg.bb_group_id
                ,bbm.bb_measure_id
                ,asubst.ayp_subject_id
                ,asubst.student_id
                ,asubst.school_year_id
                ,asubst.ayp_score
                ,cl.moniker
                ,1234
                ,CURRENT_TIMESTAMP
        FROM c_ayp_subject_student AS asubst
        JOIN c_student_year AS sty
            ON   sty.student_id = asubst.student_id
            AND  sty.school_year_id = asubst.school_year_id
        JOIN c_grade_level AS gl
            ON gl.grade_level_id = sty.grade_level_id
        JOIN pm_baseball_group AS bbg
            ON bbg.bb_group_code = 'lag'
        JOIN pm_baseball_measure AS bbm
            ON   bbm.bb_measure_code = 'ss'
            AND  bbm.bb_group_id = bbg.bb_group_id
        LEFT JOIN c_color_ayp_subject AS csub
            ON   csub.ayp_subject_id = asubst.ayp_subject_id
            AND  sty.school_year_id BETWEEN csub.begin_year AND csub.end_year
            AND  gl.grade_sequence BETWEEN csub.begin_grade_sequence AND csub.end_grade_sequence
            AND  COALESCE(asubst.alt_ayp_score, asubst.ayp_score) BETWEEN csub.min_score AND csub.max_score
        LEFT JOIN pmi_color AS cl
            ON cl.color_id = csub.color_id

        WHERE   asubst.ayp_score IS NOT NULL
        AND     asubst.score_record_flag = 1
    
        UNION 
        # Lagging - Achievement Level
        SELECT bbg.bb_group_id
                ,bbm.bb_measure_id
                ,asubst.ayp_subject_id
                ,asubst.student_id
                ,asubst.school_year_id
                ,aal.pmi_al
                ,cl.moniker
                ,1234
                ,CURRENT_TIMESTAMP
        FROM c_ayp_subject_student AS asubst
        JOIN c_student_year AS sty
            ON   sty.student_id = asubst.student_id
            AND  sty.school_year_id = asubst.school_year_id
        JOIN    c_ayp_subject as sub
            ON  sub.ayp_subject_id = asubst.ayp_subject_id
        JOIN    c_ayp_test_type_al as atta
            ON  atta.ayp_test_type_id = sub.ayp_test_type_id
        JOIN    c_ayp_achievement_level as aal
            ON  aal.al_id = atta.al_id
                AND   aal.al_id = asubst.al_id
        JOIN pm_baseball_group AS bbg
            ON bbg.bb_group_code = 'lag'
        JOIN pm_baseball_measure AS bbm
            ON  bbm.bb_measure_code = 'al'
            AND bbm.bb_group_id = bbg.bb_group_id
        LEFT JOIN pmi_color AS cl
            ON  cl.color_id = atta.color_id
        WHERE   coalesce(asubst.ayp_score,asubst.alt_ayp_score) IS NOT NULL   #asubst.ayp_score IS NOT NULL
        AND     asubst.score_record_flag = 1
    
        UNION 
        # Lagging - Dev Score
        SELECT bbg.bb_group_id
                ,bbm.bb_measure_id
                ,asubst.ayp_subject_id
                ,asubst.student_id
                ,asubst.school_year_id
                ,asubst.alt_ayp_score
                ,cl.moniker
                ,1234
                ,CURRENT_TIMESTAMP
        FROM c_ayp_subject_student AS asubst
        JOIN c_student_year AS sty
            ON   sty.student_id = asubst.student_id
            AND  sty.school_year_id = asubst.school_year_id
        JOIN c_grade_level AS gl
            ON gl.grade_level_id = sty.grade_level_id
        JOIN pm_baseball_group AS bbg
            ON bbg.bb_group_code = 'lag'
        JOIN pm_baseball_measure AS bbm
            ON   bbm.bb_measure_code = 'dev'
            AND  bbm.bb_group_id = bbg.bb_group_id            
        LEFT JOIN c_color_ayp_subject AS csub
            ON   csub.ayp_subject_id = asubst.ayp_subject_id
            AND  sty.school_year_id BETWEEN csub.begin_year AND csub.end_year
            AND  gl.grade_sequence BETWEEN csub.begin_grade_sequence AND csub.end_grade_sequence
            AND  asubst.alt_ayp_score BETWEEN csub.min_score AND csub.max_score
        LEFT JOIN pmi_color AS cl
            ON cl.color_id = csub.color_id
        WHERE   asubst.alt_ayp_score IS NOT NULL
        AND     asubst.score_record_flag = 1
            ) dt
        ON DUPLICATE key UPDATE last_user_id = 1234;



END PROC;
//
