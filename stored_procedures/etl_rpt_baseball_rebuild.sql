/*
$Rev: 7435 $ 
$Author: randall.stanley $ 
$Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_baseball_rebuild.sql $
$Id: etl_rpt_baseball_rebuild.sql 7435 2009-07-24 13:58:49Z randall.stanley $ 
 */

####################################################################
# Insert aggregate data into rpt tables for baseball report.
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_rpt_baseball_rebuild //

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_baseball_rebuild()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7435 $ $Date: 2009-07-24 09:58:49 -0400 (Fri, 24 Jul 2009) $'

PROC: BEGIN 

    flush tables `rpt_baseball_detail_college_prep`, `rpt_baseball_detail_leading`, `rpt_baseball_detail_lagging`
        , `rpt_baseball_detail_assessment`, `rpt_baseball_detail_lexile`,`rpt_baseball_detail`;

    truncate table `rpt_baseball_detail_lagging`;
    truncate table `rpt_baseball_detail_leading`;
    truncate table `rpt_baseball_detail_college_prep`;
    truncate table `rpt_baseball_detail_assessment`;
    truncate table `rpt_baseball_detail_lexile`;

    
    # Insert scores
    call etl_rpt_baseball_detail_college_prep();
    call etl_rpt_baseball_detail_leading();
    call etl_rpt_baseball_detail_lagging();
    call etl_rpt_baseball_detail_assessment();
    call etl_rpt_baseball_detail_lexile();

    # Rebuild BB Card Filitering data
    call etl_pm_baseball_measure_select();
    
END PROC;
//
