/*
$Rev: 9929 $ 
$Author: randall.stanley $ 
$Date: 2011-01-26 08:00:36 -0500 (Wed, 26 Jan 2011) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bbcard_detail_iri.sql $
$Id: etl_rpt_bbcard_detail_iri.sql 9929 2011-01-26 13:00:36Z randall.stanley $ 
*/

drop procedure if exists etl_rpt_bbcard_detail_iri//

create definer=`dbadmin`@`localhost` procedure etl_rpt_bbcard_detail_iri()
contains sql
sql security invoker
comment '$Rev: 9929 $ $Date: 2011-01-26 08:00:36 -0500 (Wed, 26 Jan 2011) $'


proc: begin 

    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_view_exists tinyint(1);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_iri';
    set v_ods_view = concat('v_', v_ods_table);

    select  count(*)
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;
    

    if v_view_exists > 0 then

        # New ID's table for adding new baseball measures for IRI
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
        where   bb_group_code = 'iri'
        ;

        # currently no coloring for IRI
        set @swatch_id := null;

        select  school_year_id
        into    @curr_sy_id
        from    c_school_year
        where   active_flag = 1
        ;

        insert tmp_test_list ( test_name, bb_group_id )
        select  moniker, @bb_group_id
        from    v_pmi_ods_iri
        group by moniker
        ;
        

        # Get id's for new measures (new IRI tests)
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


        # add new measure items (dummy for iri)
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
            ,'a'
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


        insert rpt_bbcard_detail_iri (
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
            ,st.student_id
            ,@curr_sy_id
            ,ods.iri_score
            ,'a'
            ,ods.color
            ,1234
            ,now()
    
        from    v_pmi_ods_iri as ods
        join    tmp_test_list as tmp1
                on      ods.moniker = tmp1.test_name
        join    c_student as st
                on      ods.student_id = st.student_code
                and     st.active_flag = 1
        join    c_student_year as sty
                on      st.student_id = sty.student_id
                and     sty.school_year_id = @curr_sy_id
        join    c_grade_level as gl
                on      sty.grade_level_id = gl.grade_level_id
        join    pm_bbcard_measure_item as bmi
                on      bmi.bb_group_id = tmp1.bb_group_id
                and     bmi.bb_measure_id = tmp1.bb_measure_id
                and     bmi.bb_measure_item_id = tmp1.bb_measure_item_id
        on duplicate key update score = values(score)
            ,score_type = values(score_type)
            ,score_color = values(score_color)
            ,last_user_id = values(last_user_id)
        ;

        drop table if exists `tmp_id_assign_bb_meas`;
        drop table if exists `tmp_id_assign_bb_meas_item`;
        drop table if exists `tmp_test_list`;

        #################
        ## Update Log
        #################
        set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', v_ods_table, '\', \'P\', \'ETL Load Successful\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;

    end if;

end proc;
//
