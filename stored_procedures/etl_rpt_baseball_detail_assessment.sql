/*
$Rev: 8490 $ 
$Author: randall.stanley $ 
$Date: 2010-04-30 11:16:38 -0400 (Fri, 30 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_baseball_detail_assessment.sql $
$Id: etl_rpt_baseball_detail_assessment.sql 8490 2010-04-30 15:16:38Z randall.stanley $ 
 */

####################################################################
# Insert assessment data into rpt tables for baseball report.
# 
####################################################################

drop procedure if exists etl_rpt_baseball_detail_assessment//

create definer=`dbadmin`@`localhost` procedure etl_rpt_baseball_detail_assessment()
contains sql
sql security invoker
comment '$Rev: 8490 $ $Date: 2010-04-30 11:16:38 -0400 (Fri, 30 Apr 2010) $'

proc: begin 
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    ##############################################################
    # Insert Assessment data
    ##############################################################
    drop table if exists `tmp_bbcard_tests`;
    create table `tmp_bbcard_tests` (
      bb_group_id int(11) not null,
      bb_measure_id int(11) not null,
      test_id int(11) not null,
      purge_flag tinyint(1) not null,
      primary key  (`bb_group_id`,`bb_measure_id`),
      key `tmp_bbcard_tests_test_id` (`test_id`)
    );

    # New ID's table for Assessment measures
    drop table if exists `tmp_id_assign`;
    create table `tmp_id_assign` (
        new_id int(11) not null,
        base_code varchar(50) not null,
        moniker varchar(50) default null,
        primary key  (`new_id`),
        unique key `uq_tmp_id_assign` (`base_code`)
    );


    select  school_year_id
    into    @curr_sy_id
    from    c_school_year
    where   active_flag = 1
    ;
    
    select  bb_group_id
            ,swatch_id
    into    @bb_group_id
            ,@swatch_id
    from    pm_baseball_group
    where   bb_group_code = 'assessments'
    ;

    # Get id's for new measures (new Tests)
    insert  tmp_id_assign (new_id, base_code, moniker)
    select  pmi_f_get_next_sequence_app_db('pm_baseball_measure', 1), src.test_id, min(src.test_name)
    from    rpt_test_scores as src
    left join   pm_baseball_measure as tar
            on      tar.bb_group_id = @bb_group_id
            and     tar.bb_measure_code = cast(src.test_id as char(15))
    where   tar.bb_measure_id is null
    group by src.test_id
    ;      

    insert  pm_baseball_measure ( 
        bb_group_id
        , bb_measure_id
        , bb_measure_code
        , moniker
        , active_flag
        , dynamic_creation_flag
        , last_user_id
        , create_timestamp
    )
    select  @bb_group_id
        ,tmpid.new_id
        ,tmpid.base_code
        ,tmpid.moniker
        ,1
        ,1
        ,1234
        ,now()
    from    tmp_id_assign as tmpid
    ;
    
    # load tmp table used for deleting purged tests
    # and adding valid test results
    insert  tmp_bbcard_tests ( 
        bb_group_id
        ,bb_measure_id
        ,test_id
        ,purge_flag
    )
    
    select  bm.bb_group_id
        ,bm.bb_measure_id
        ,t.test_id
        ,t.purge_flag
        
    from    pm_baseball_measure as bm
    join    sam_test as t
            on      cast(bm.bb_measure_code as signed) = t.test_id
            and     t.owner_id = t.client_id
    where   bm.bb_group_id = @bb_group_id
    ;

    # Remove BBCard Assess data linked to purged Tests
    delete  rpt.*
    from    tmp_bbcard_tests as tmp1
    join    rpt_baseball_detail_assessment as rpt
            on      tmp1.bb_group_id = rpt.bb_group_id
            and     tmp1.bb_measure_id = rpt.bb_measure_id
    where   tmp1.purge_flag = 1
    ;

    
    insert rpt_baseball_detail_assessment (
        bb_group_id
        ,bb_measure_id
        ,bb_measure_item_id
        ,student_id
        ,school_year_id
        ,score
        ,score_color
        ,last_user_id
        ,create_timestamp
    )

    select tmp1.bb_group_id
        ,tmp1.bb_measure_id
        ,0
        ,rts.student_id
        ,@curr_sy_id
        ,round(rts.points_earned / rts.points_possible * 100, 0)
        ,clr.moniker
        ,1234
        ,now()
    
    from    tmp_bbcard_tests as tmp1
    join    rpt_test_scores as rts
            on      tmp1.test_id = rts.test_id
    join    sam_test_mt_color_sequence_list as tmcsl
            on      rts.test_id = tmcsl.test_id
            and     round(rts.points_earned / rts.points_possible * 100, 0) between tmcsl.min_score and tmcsl.max_score
    join    c_color_swatch_list as csl
            on      csl.swatch_id = @swatch_id
            and     csl.sort_order = tmcsl.color_sequence
    join    pmi_color as clr
            on      csl.color_id = clr.color_id
    where   tmp1.purge_flag = 0
    group by tmp1.bb_group_id, tmp1.bb_measure_id, rts.student_id
    on duplicate key update score = values(score)
        ,score_color = values(score_color)
    ;

    drop table if exists `tmp_bbcard_tests`;
    drop table if exists `tmp_id_assign`;

end proc;
//
