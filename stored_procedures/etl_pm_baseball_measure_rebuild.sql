/*
$Rev: 9435 $ 
$Author: randall.stanley $ 
$Date: 2010-10-11 16:04:34 -0400 (Mon, 11 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_baseball_measure_rebuild.sql $
$Id: etl_pm_baseball_measure_rebuild.sql 9435 2010-10-11 20:04:34Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_pm_baseball_measure_rebuild//

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_baseball_measure_rebuild()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 9435 $ $Date: 2010-10-11 16:04:34 -0400 (Mon, 11 Oct 2010) $'

PROC: BEGIN 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    drop table if exists `tmp_dyn_meas_with_scores`;
    create table `tmp_dyn_meas_with_scores` (
        bb_group_id int(11) not null,
        bb_measure_id int(11) not null,
        primary key  (`bb_group_id`,`bb_measure_id`)
    );

    select  bb_group_id
    into    @bbg_lead_id
    from    pm_baseball_group
    where   bb_group_code = 'lead'
    ;

    #################################################
    # Update leading monikers so they match
    #  sam_test_admin_period.report_column_text
    #################################################

    UPDATE  pm_baseball_measure AS bbm
    JOIN    sam_test_admin_period AS tap
            ON      tap.admin_code = bbm.bb_measure_code
    LEFT JOIN   sam_test_admin_period_client as tapc
            ON      tap.admin_period_id = tapc.admin_period_id
    SET     bbm.moniker = coalesce(tapc.report_column_text, tap.report_column_text)
    WHERE   bbm.bb_group_id = @bbg_lead_id
    ;

    # Truncate filtering tables #
    
    truncate table pm_baseball_measure_select;
    truncate table pm_baseball_measure_select_teacher;
    
    # Get list of "dynamic" measures with linked scores
    insert tmp_dyn_meas_with_scores (
        bb_group_id
        ,bb_measure_id
    )

    select  bm.bb_group_id
        ,bm.bb_measure_id            
    from    pm_baseball_measure as bm
    where   bm.dynamic_creation_flag = 1
    and exists (    select  *
                    from    rpt_baseball_detail as bd
                    where   bm.bb_group_id = bd.bb_group_id
                    and     bm.bb_measure_id = bd.bb_measure_id
                )
    ;

    # Remove "dynamic" measures that have no assoc scores
    delete  del.*
    from    pm_baseball_measure as del
    left join   tmp_dyn_meas_with_scores as tmp1
            on      tmp1.bb_group_id = del.bb_group_id
            and     tmp1.bb_measure_id = del.bb_measure_id
    where   del.dynamic_creation_flag = 1
    and     tmp1.bb_measure_id is null
    ;
    
    drop table if exists `tmp_dyn_meas_with_scores`;
    
END PROC;
//


