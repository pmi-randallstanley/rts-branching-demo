/*
$Rev: 7435 $ 
$Author: randall.stanley $ 
$Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm.sql $
$Id: etl_pm.sql 7435 2009-07-24 13:58:49Z randall.stanley $
 */

DROP PROCEDURE IF EXISTS etl_pm//

CREATE definer=`dbadmin`@`localhost` procedure `etl_pm`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7435 $ $Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $'

BEGIN

#    call etl_pm_baseball_measure_rebuild();
#    call etl_pm_natl_test_scores();
#    call etl_pm_state_test_scores();
#    call etl_rpt_baseball_detail_assessment();
#    call etl_rpt_baseball_detail_iri();

    ## Lexile ===================================
    SELECT COUNT(*) INTO @v_pmi_ods_lexile
        FROM information_schema.VIEWS v
        WHERE v.TABLE_SCHEMA = database()
        AND v.TABLE_name = 'v_pmi_ods_lexile';
        
    IF @v_pmi_ods_lexile = 1 THEN 
        CALL etl_rpt_baseball_detail_lexile();
    END IF;
    
#    call etl_pm_baseball_measure_select();
    
END;
//
