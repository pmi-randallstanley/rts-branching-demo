/*
$Rev: 7435 $ 
$Author: randall.stanley $ 
$Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_state_test_scores.sql $
$Id: etl_pm_state_test_scores.sql 7435 2009-07-24 13:58:49Z randall.stanley $ 
 */

####################################################################
# Insert scores into pm_state_test_scores.
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_pm_state_test_scores//

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_state_test_scores()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7435 $ $Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $'

PROC: BEGIN 

    SELECT COUNT(*)
        ,SUM(CASE WHEN v.TABLE_NAME = 'v_pmi_ods_ga_eoct' THEN 1 ELSE 0 END)
        ,SUM(CASE WHEN v.TABLE_NAME = 'v_pmi_ods_nwea'    THEN 1 ELSE 0 END)
    INTO
        @tot_count
        ,@v_pmi_ods_ga_eoct
        ,@v_pmi_ods_nwea
    FROM information_schema.VIEWS v
    WHERE v.TABLE_SCHEMA = database();



    ## EOCT =====================================
   /* The EOCT proc needs to be written 
   IF @v_pmi_ods_ga_eoct = 1           
        THEN CALL etl_pm_state_test_scores_eoct();
    END IF;
   */
    ## NWEA =====================================
#    IF @v_pmi_ods_nwea = 1           
#        THEN CALL etl_pm_state_test_scores_nwea();
#    END IF;


END PROC;
//
