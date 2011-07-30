/*
$Rev: 9893 $ 
$Author: randall.stanley $ 
$Date: 2011-01-18 09:10:39 -0500 (Tue, 18 Jan 2011) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bbcard_detail_assessment.sql $
$Id: etl_rpt_bbcard_detail_assessment.sql 9893 2011-01-18 14:10:39Z randall.stanley $ 
*/

drop procedure if exists etl_rpt_bbcard_detail_assessment//

create definer=`dbadmin`@`localhost` procedure etl_rpt_bbcard_detail_assessment()
contains sql
sql security invoker
comment '$Rev: 9893 $ $Date: 2011-01-18 09:10:39 -0500 (Tue, 18 Jan 2011) $'


proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    drop table if exists `tmp_id_assign_bb_meas`;
    drop table if exists `tmp_id_assign_bb_meas_item`;
    drop table if exists `tmp_pm_bbcard_measure`;
    drop table if exists `tmp_pm_bbcard_measure_item`;
    drop table if exists `tmp_test_list`;

    create table `tmp_pm_bbcard_measure` (
      `bb_measure_code` varchar(40) not null,
      `moniker` varchar(75) not null,
      `sort_order` smallint(6) not null default '0',
      `swatch_id` int(11) default null,
      `active_flag` tinyint(1) not null default '1',
      `dynamic_creation_flag` tinyint(1) not null default '0',
      `last_user_id` int(11) not null,
      `create_timestamp` datetime not null default '1980-12-31 00:00:00',
      `last_edit_timestamp` timestamp not null default current_timestamp on update current_timestamp,
      unique key `uq_tmp_pm_bbcard_measure` (`bb_measure_code`)
    ) engine=innodb default charset=latin1
    ;
    create table `tmp_pm_bbcard_measure_item` (
      `bb_measure_code` varchar(40) not null,
      `bb_measure_item_code` varchar(40) not null,
      `moniker` varchar(120) not null,
      `sort_order` smallint(6) not null default '0',
      `swatch_id` int(11) default null,
      `score_sort_type_code` enum('a','m','n') not null,
      `active_flag` tinyint(1) not null default '1',
      `dynamic_creation_flag` tinyint(1) not null default '0',
      `last_user_id` int(11) not null,
      `create_timestamp` datetime not null default '1980-12-31 00:00:00',
      `last_edit_timestamp` timestamp not null default current_timestamp on update current_timestamp,
      unique key `uq_tmp_pm_bbcard_measure_item` (`bb_measure_code`,`bb_measure_item_code`)
    ) engine=innodb default charset=latin1
    ;
    create table `tmp_id_assign_bb_meas` (
      `bb_group_id` int(11) not null,
      `new_id` int(11) not null,
      `base_code` varchar(50) not null,
      `moniker` varchar(50) default null,
      primary key  (`bb_group_id`,`new_id`),
      unique key `uq_tmp_id_assign_bb_meas` (`bb_group_id`,`base_code`)
    ) engine=innodb default charset=latin1
    ;
    create table `tmp_id_assign_bb_meas_item` (
      `bb_group_id` int(11) NOT NULL,
      `bb_measure_id` int(11) NOT NULL,
      `new_id` int(11) not null,
      `base_code` varchar(50) not null,
      `moniker` varchar(120) default null,
      `sort_order` smallint(6) default null,
      primary key  (`bb_group_id`,`bb_measure_id`,`new_id`),
      unique key `uq_tmp_id_assign_bb_meas_item` (`bb_group_id`,`bb_measure_id`,`base_code`)
    );

    create table `tmp_test_list` (
      `test_id` int(11) not null,
      `test_name` varchar(120) not null,
      `test_id_as_char` varchar(15) not null,
      `purge_flag` tinyint(1) not null,
      `course_type_id_as_char` varchar(15) default null,
      `course_type_name` varchar(50) default null,
      `district_test_flag` tinyint(1) not null,
      `bb_group_id` int(11) default NULL,
      `bb_measure_id` int(11) default NULL,
      `bb_measure_item_id` int(11) default NULL,
      primary key  (`test_id`),
      unique key `uq_tmp_test_list` (`test_id_as_char`),
      key `ind_tmp_test_list_ctid` (`course_type_id_as_char`),
      key `ind_tmp_test_list_bb` (`bb_group_id`,`bb_measure_id`,`bb_measure_item_id`)
    ) engine=innodb default charset=latin1
    ;

    select  school_year_id
    into    @curr_sy_id
    from    c_school_year
    where   active_flag = 1
    ;
    
    select  bb_group_id
    into    @bb_group_id
    from    pm_bbcard_group
    where   bb_group_code = 'assessments'
    ;

    select  swatch_id
    into    @swatch_id
    from    c_color_swatch
    where   swatch_code = 'testMT'
    ;


    insert tmp_test_list (
        test_id
        ,test_name
        ,test_id_as_char
        ,purge_flag
        ,course_type_id_as_char
        ,course_type_name
        ,district_test_flag
        ,bb_group_id
    )
    
    select  t.test_id
        ,min(t.moniker)
        ,cast(min(t.test_id) as char(15))
        ,min(t.purge_flag)
        ,cast(min(t.course_type_id) as char(15))
        ,min(ct.moniker)
        ,max(case when t.owner_id = t.client_id then 1 else 0 end)
        ,@bb_group_id
    from    sam_test as t
    join    c_course_type as ct
            on      t.course_type_id = ct.course_type_id
    join    rpt_test_scores as src
            on      src.test_id = t.test_id
    group by t.test_id
    ;

    insert  tmp_pm_bbcard_measure (
        bb_measure_code
        ,moniker
        ,sort_order
        ,swatch_id
        ,active_flag
        ,dynamic_creation_flag
        ,last_user_id
        ,create_timestamp
    )

    select  src.course_type_id_as_char
        ,min(src.course_type_name) as course_type_name
        ,0
        ,@swatch_id
        ,1
        ,1
        ,1234
        ,now()
        
    from    tmp_test_list as src
    where   src.purge_flag = 0
    and     src.district_test_flag = 1
    group by src.course_type_id_as_char
    ;

    insert tmp_pm_bbcard_measure_item (
        bb_measure_code
        ,bb_measure_item_code
        ,moniker
        ,sort_order
        ,swatch_id
        ,score_sort_type_code
        ,active_flag
        ,dynamic_creation_flag
        ,last_user_id
        ,create_timestamp
    )

    select  src.course_type_id_as_char
        ,src.test_id_as_char
        ,src.test_name
        ,0
        ,@swatch_id
        ,'n'
        ,1
        ,1
        ,1234
        ,now()

    from    tmp_test_list as src
    where   src.purge_flag = 0
    and     src.district_test_flag = 1
    ;

    insert  tmp_id_assign_bb_meas (
        bb_group_id
        ,new_id
        ,base_code
        ,moniker
    )

    select  @bb_group_id
        ,pmi_admin.pmi_f_get_next_sequence('pm_bbcard_measure', 1)
        ,src.bb_measure_code
        ,src.moniker

    from    tmp_pm_bbcard_measure as src
    left join   pm_bbcard_measure as tar
            on      tar.bb_group_id = @bb_group_id
            and     tar.bb_measure_code = src.bb_measure_code
    where   tar.bb_measure_id is null
    ;

    insert  pm_bbcard_measure ( 
        bb_group_id
        ,bb_measure_id
        ,bb_measure_code
        ,moniker
        ,sort_order
        ,swatch_id
        ,active_flag
        ,dynamic_creation_flag
        ,last_user_id
        ,create_timestamp
    )
    select  @bb_group_id
        ,coalesce(tmpid.new_id, tar.bb_measure_id)
        ,src.bb_measure_code
        ,src.moniker
        ,0
        ,@swatch_id
        ,1
        ,1
        ,1234
        ,now()

    from    tmp_pm_bbcard_measure as src
    left join   tmp_id_assign_bb_meas as tmpid
            on      tmpid.bb_group_id = @bb_group_id
            and     src.bb_measure_code = tmpid.base_code
    left join   pm_bbcard_measure as tar
            on      tar.bb_group_id = @bb_group_id
            and     tar.bb_measure_code = src.bb_measure_code
    on duplicate key update moniker = values(moniker)
        ,last_user_id = values(last_user_id)
    ;


    # Get id's for new measures items (new tests)
    insert tmp_id_assign_bb_meas_item (
        bb_group_id
        ,bb_measure_id
        ,new_id
        ,base_code
        ,moniker
    )

    select  bm.bb_group_id
        ,bm.bb_measure_id
        ,pmi_admin.pmi_f_get_next_sequence('pm_bbcard_measure_item', 1)
        ,src.bb_measure_item_code
        ,src.moniker

    from    tmp_pm_bbcard_measure_item as src
    join    pm_bbcard_measure as bm
            on      bm.bb_group_id = @bb_group_id
            and     bm.bb_measure_code = src.bb_measure_code
    left join   pm_bbcard_measure_item as tar
            on      tar.bb_group_id = bm.bb_group_id
            and     tar.bb_measure_id = bm.bb_measure_id
            and     tar.bb_measure_item_code = src.bb_measure_item_code
    where   tar.bb_measure_item_id is null
    ;


    # need to discuss handling sort order with incremental course adds
    insert  pm_bbcard_measure_item ( 
        bb_group_id
        ,bb_measure_id
        ,bb_measure_item_id
        ,bb_measure_item_code
        ,moniker
        ,sort_order
        ,swatch_id
        ,score_sort_type_code
        ,active_flag
        ,dynamic_creation_flag
        ,last_user_id
        ,create_timestamp
    )

    select bm.bb_group_id
        ,bm.bb_measure_id
        ,coalesce(tmpid.new_id, tar.bb_measure_item_id)
        ,src.bb_measure_item_code
        ,src.moniker
        ,src.sort_order
        ,src.swatch_id
        ,src.score_sort_type_code
        ,src.active_flag
        ,src.dynamic_creation_flag
        ,src.last_user_id
        ,src.create_timestamp


    from    tmp_pm_bbcard_measure_item as src
    join    pm_bbcard_measure as bm
            on      bm.bb_group_id = @bb_group_id
            and     bm.bb_measure_code = src.bb_measure_code
    left join   tmp_id_assign_bb_meas_item as tmpid
            on      tmpid.bb_group_id = bm.bb_group_id
            and     tmpid.bb_measure_id = bm.bb_measure_id
            and     tmpid.base_code = src.bb_measure_item_code
    left join   pm_bbcard_measure_item as tar
            on      tar.bb_group_id = @bb_group_id
            and     tar.bb_measure_id = bm.bb_measure_id
            and     tar.bb_measure_item_code = src.bb_measure_item_code
    on duplicate key update moniker = values(moniker)
        ,last_user_id = values(last_user_id)
    ;


    update  tmp_test_list as upd
    join    pm_bbcard_measure as bm
            on      upd.bb_group_id = bm.bb_group_id
            and     upd.course_type_id_as_char = bm.bb_measure_code
    join    pm_bbcard_measure_item as bmi
            on      bm.bb_group_id = bmi.bb_group_id
            and     bm.bb_measure_id = bmi.bb_measure_id
            and     upd.test_id_as_char = bmi.bb_measure_item_code
    set     upd.bb_measure_id = bmi.bb_measure_id
            ,upd.bb_measure_item_id = bmi.bb_measure_item_id
    ;

    # Remove BBCard Assess data linked to purged Tests
    delete  rpt.*
    from    tmp_test_list as tmp1
    join    rpt_bbcard_detail_assessment as rpt
            on      tmp1.bb_group_id = rpt.bb_group_id
            and     tmp1.bb_measure_id = rpt.bb_measure_id
            and     tmp1.bb_measure_item_id = rpt.bb_measure_item_id
    where   tmp1.purge_flag = 1
    ;

    insert rpt_bbcard_detail_assessment (
        bb_group_id
        ,bb_measure_id
        ,bb_measure_item_id
        ,student_id
        ,school_year_id
        ,score
        ,score_type
        ,score_color
        ,last_user_id
        ,create_timestamp
    )

    select tmp1.bb_group_id
        ,tmp1.bb_measure_id
        ,tmp1.bb_measure_item_id
        ,rts.student_id
        ,@curr_sy_id
        ,round(rts.points_earned / rts.points_possible * 100, 0)
        ,'n'
        ,clr.moniker
        ,1234
        ,now()
    
    from    tmp_test_list as tmp1
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
    and     tmp1.district_test_flag = 1    
    group by tmp1.bb_group_id, tmp1.bb_measure_id, tmp1.bb_measure_item_id, rts.student_id
    on duplicate key update score = values(score)
        ,score_color = values(score_color)
    ;

    drop table if exists `tmp_id_assign_bb_meas`;
    drop table if exists `tmp_id_assign_bb_meas_item`;
    drop table if exists `tmp_pm_bbcard_measure`;
    drop table if exists `tmp_pm_bbcard_measure_item`;
    drop table if exists `tmp_test_list`;

end proc;
//
