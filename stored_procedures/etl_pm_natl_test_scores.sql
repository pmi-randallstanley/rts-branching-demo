/*
$Rev: 7435 $ 
$Author: randall.stanley $ 
$Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_natl_test_scores.sql $
$Id: etl_pm_natl_test_scores.sql 7435 2009-07-24 13:58:49Z randall.stanley $ 
 */

####################################################################
# Insert scores into pm_natl_test_scores.
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_pm_natl_test_scores//

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_natl_test_scores()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7435 $ $Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $'

PROC: BEGIN 
    
#    CALL etl_pm_natl_test_scores_act();
#    CALL etl_pm_natl_test_scores_sat();
#    CALL etl_pm_natl_test_scores_psat();


    SELECT COUNT(*)
        ,SUM(CASE WHEN v.TABLE_NAME = 'v_pmi_ods_itbs'    THEN 1 ELSE 0 END)
        ,SUM(CASE WHEN v.TABLE_NAME = 'v_pmi_ods_plan'    THEN 1 ELSE 0 END)
        ,SUM(CASE WHEN v.TABLE_NAME = 'v_pmi_ods_ga_explorer'    THEN 1 ELSE 0 END)
        ,SUM(CASE WHEN v.TABLE_NAME = 'v_pmi_ods_stanford_10'    THEN 1 ELSE 0 END)
        ,SUM(CASE WHEN v.TABLE_NAME = 'v_pmi_ods_olsat'          THEN 1 ELSE 0 END)
        ,SUM(CASE WHEN v.TABLE_NAME = 'v_pmi_ods_fl_manatee_lctsp'    THEN 1 ELSE 0 END)
    INTO
        @tot_count
        ,@v_pmi_ods_itbs
        ,@v_pmi_ods_plan
        ,@v_pmi_ods_ga_explorer
        ,@v_pmi_ods_stanford_10
        ,@v_pmi_ods_olsat
        ,@v_pmi_ods_fl_manatee_lctsp
    FROM information_schema.VIEWS v
    WHERE v.TABLE_SCHEMA = database();


# Calls below moved to etl_imp() since these are direct loads from an ODS table.
# They were made imp_upload_log "queue" driven.
    ## ITBS =====================================
#        IF @v_pmi_ods_itbs = 1          
#            THEN    CALL etl_pm_natl_test_scores_itbs();
#        END IF;

    ## Plan =====================================
#        IF @v_pmi_ods_plan = 1          
#            THEN    CALL etl_pm_natl_test_scores_plan();
#        END IF;

    ## Explore ==================================
#        IF @v_pmi_ods_ga_explorer = 1          
#            THEN    CALL etl_pm_natl_test_scores_explore();
#        END IF;

    ## Stanford =================================
#        IF @v_pmi_ods_stanford_10 = 1
#            THEN    CALL etl_pm_natl_test_scores_stanford_10();
#        END IF;
#        IF @v_pmi_ods_olsat = 1
#            THEN    CALL etl_pm_natl_test_scores_stanford_olsat();
#        END IF;
#        IF @v_pmi_ods_fl_manatee_lctsp = 1
#            THEN    CALL etl_pm_natl_test_scores_stanford_fl_manatee_lctsp();
#        END IF;

END PROC;
//
