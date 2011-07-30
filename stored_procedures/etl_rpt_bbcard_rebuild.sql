/*
$Rev: 9907 $ 
$Author: ryan.riordan $ 
$Date: 2011-01-19 14:19:09 -0500 (Wed, 19 Jan 2011) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bbcard_rebuild.sql $
$Id: etl_rpt_bbcard_rebuild.sql 9907 2011-01-19 19:19:09Z ryan.riordan $ 
*/

drop procedure if exists etl_rpt_bbcard_rebuild//

create definer=`dbadmin`@`localhost` procedure etl_rpt_bbcard_rebuild()
contains sql
sql security invoker
comment '$Rev: 9907 $ $Date: 2011-01-19 14:19:09 -0500 (Wed, 19 Jan 2011) $'


proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    flush tables `rpt_bbcard_detail_college_prep`, `rpt_bbcard_detail_lag_lead_hst_subject`, `rpt_bbcard_detail_lag_lead_hst_strand`
        , `rpt_bbcard_detail_assessment`, `rpt_bbcard_detail_lexile`,`rpt_bbcard_detail_grades`,`rpt_bbcard_detail_smi_quantile`,`rpt_bbcard_detail`;

    truncate table `rpt_bbcard_detail_assessment`;
    truncate table `rpt_bbcard_detail_college_prep`;
    truncate table `rpt_bbcard_detail_lag_lead_hst_subject`;
    truncate table `rpt_bbcard_detail_lag_lead_hst_strand`;
    truncate table `rpt_bbcard_detail_grades`;
    truncate table `rpt_bbcard_detail_lexile`;
    truncate table `rpt_bbcard_detail_smi_quartile`;

    
    # Insert scores
    call etl_rpt_bbcard_detail_college_prep();
    call etl_rpt_bbcard_detail_lag_lead_hst_subject();
    call etl_rpt_bbcard_detail_lag_lead_hst_strand();
    call etl_rpt_bbcard_detail_assessment();
    call etl_rpt_bbcard_detail_grades();
    call etl_rpt_bbcard_detail_lexile();
    call etl_rpt_bbcard_detail_snap();
    call etl_rpt_bbcard_detail_smi_quantile();

    # Rebuild BB Card Filitering data
    call etl_pm_bbcard_measure_select();


end proc;
//
