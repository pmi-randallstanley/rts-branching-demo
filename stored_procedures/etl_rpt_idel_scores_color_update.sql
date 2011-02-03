/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_idel_scores_color_update.sql $
$Id: etl_rpt_idel_scores_color_update.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
 */

####################################################################
#  Updates colors in the rpt_idel_score
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_rpt_idel_scores_color_update//

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_idel_scores_color_update()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'

PROC: BEGIN 

      update rpt_idel_scores as rpt
        left join pm_color_idel as ci
        on rpt.measure_period_id = ci.measure_period_id
        and rpt.score between ci.min_score and ci.max_score
        left join pmi_color as pc 
        on pc.color_id = ci.color_id
        set rpt.score_color = pc.moniker;


END PROC;
//
