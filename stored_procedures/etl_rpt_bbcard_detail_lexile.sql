/*
$Rev: 9764 $ 
$Author: randall.stanley $ 
$Date: 2010-12-08 09:50:45 -0500 (Wed, 08 Dec 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bbcard_detail_lexile.sql $
$Id: etl_rpt_bbcard_detail_lexile.sql 9764 2010-12-08 14:50:45Z randall.stanley $ 
*/

drop procedure if exists etl_rpt_bbcard_detail_lexile//

create definer=`dbadmin`@`localhost` procedure etl_rpt_bbcard_detail_lexile()
contains sql
sql security invoker
comment '$Rev: 9764 $ $Date: 2010-12-08 09:50:45 -0500 (Wed, 08 Dec 2010) $'


proc: begin 

    declare v_table_name varchar(64);
    declare v_table_exists tinyint(1);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_table_name = 'pm_lexile_scores';

    select  count(*)
    into    v_table_exists
    from    information_schema.`tables` as t
    where   t.table_schema = database()
    and     t.table_name = v_table_name;
    

    if v_table_exists > 0 then

        drop table if exists `tmp_id_assign_bb_meas`;
        drop table if exists `tmp_id_assign_bb_meas_item`;
        drop table if exists `tmp_test_list`;
    
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
          `moniker` varchar(50) default null,
          `sort_order` smallint(6) default null,
          primary key  (`bb_group_id`,`bb_measure_id`,`new_id`),
          unique key `uq_tmp_id_assign_bb_meas_item` (`bb_group_id`,`bb_measure_id`,`base_code`)
        );
        
        create table `tmp_test_list` (
          `test_name` varchar(75) not null,
          `bb_group_id` int(11) default NULL,
          `bb_measure_id` int(11) default NULL,
          `bb_measure_item_id` int(11) default NULL,
          unique key `uq_tmp_test_list` (`test_name`),
          key `ind_tmp_test_list_bb` (`bb_group_id`,`bb_measure_id`,`bb_measure_item_id`)
        ) engine=innodb default charset=latin1
        ;

        select  bb_group_id
        into    @bb_group_id
        from    pm_bbcard_group
        where   bb_group_code = 'lexile'
        ;

        select  swatch_id
        into    @swatch_id
        from    c_color_swatch
        where   swatch_code = 'lexile'
        ;

        insert tmp_test_list ( test_name, bb_group_id )
        select  test_moniker, @bb_group_id
        from    pm_lexile_scores
        group by test_moniker
        ;
        

        # Get id's for new measures
        insert  tmp_id_assign_bb_meas (bb_group_id, new_id, base_code, moniker)
        select  @bb_group_id, pmi_admin.pmi_f_get_next_sequence('pm_bbcard_measure', 1), src.test_name, src.test_name
        from    tmp_test_list as src
        left join   pm_bbcard_measure as tar
                on      tar.bb_group_id = @bb_group_id
                and     tar.bb_measure_code = src.test_name
        where   tar.bb_measure_id is null
        ;      

        # Add any new measures
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
        select  tmpid.bb_group_id
            ,tmpid.new_id
            ,tmpid.base_code
            ,tmpid.moniker
            ,0
            ,@swatch_id
            ,1
            ,1
            ,1234
            ,now()
        from    tmp_id_assign_bb_meas as tmpid
        ;


        # add new measure items (dummy records)
        insert pm_bbcard_measure_item (
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

        select  bm.bb_group_id
            ,bm.bb_measure_id
            ,0
            ,'ignore'
            ,'Ignore Item'
            ,0
            ,@swatch_id
            ,'n'
            ,0
            ,1
            ,1234
            ,now()


        from    tmp_test_list as src
        join    pm_bbcard_measure as bm
                on      bm.bb_group_id = @bb_group_id
                and     bm.bb_measure_code = src.test_name
        left join   pm_bbcard_measure_item as tar
                on      tar.bb_group_id = bm.bb_group_id
                and     tar.bb_measure_id = bm.bb_measure_id
                and     tar.bb_measure_item_id = 0
        where   tar.bb_measure_item_id is null
        on duplicate key update moniker = values(moniker)
            ,sort_order = values(sort_order)
            ,swatch_id = values(swatch_id)
            ,score_sort_type_code = values(score_sort_type_code)
            ,last_user_id = values(last_user_id)
        ;

        update  tmp_test_list as upd
        join    pm_bbcard_measure as bm
                on      upd.bb_group_id = bm.bb_group_id
                and     upd.test_name = bm.bb_measure_code
        set     upd.bb_measure_id = bm.bb_measure_id
                ,upd.bb_measure_item_id = 0
        ;


        insert rpt_bbcard_detail_lexile (
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
            ,src.student_id
            ,src.school_year_id
            ,src.lexile_score
            ,'n'
            ,pc.moniker
            ,1234
            ,now()
    
        from    pm_lexile_scores as src
        join    tmp_test_list as tmp1
                on      src.test_moniker = tmp1.test_name
        join    pm_bbcard_measure_item as bmi
                on      bmi.bb_group_id = tmp1.bb_group_id
                and     bmi.bb_measure_id = tmp1.bb_measure_id
                and     bmi.bb_measure_item_id = tmp1.bb_measure_item_id
        join    c_student_year as sy
                on      src.student_id = sy.student_id
                and     src.school_year_id = sy.school_year_id
        join    c_grade_level as gl
                on      sy.grade_level_id = gl.grade_level_id
        left join   pm_color_lexile as pcl
                on      gl.grade_sequence between pcl.begin_grade_sequence and pcl.end_grade_sequence
                and     sy.school_year_id  between pcl.begin_year and pcl.end_year
                and     src.lexile_score between pcl.min_score and pcl.max_score
        left join   pmi_color as pc
                on      pc.color_id = pcl.color_id
        on duplicate key update score = values(score)
            ,score_type = values(score_type)
            ,score_color = values(score_color)
            ,last_user_id = values(last_user_id)
        ;

        drop table if exists `tmp_id_assign_bb_meas`;
        drop table if exists `tmp_id_assign_bb_meas_item`;
        drop table if exists `tmp_test_list`;

    end if;

end proc;
//
