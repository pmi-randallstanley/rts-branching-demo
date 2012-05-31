drop procedure if exists etl_rpt_bbcard_detail_nwea  //
create definer=`dbadmin`@`localhost` procedure etl_rpt_bbcard_detail_nwea ()

contains sql
sql security invoker

comment 'Date: 2012-01-24 etl_rpt_bbcard_detail_nwea '


proc: begin

    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_view_exists tinyint(1);
    declare v_bb_group_id int(11);
    declare v_backfill_needed smallint(6);
    declare v_date_format_mask varchar(15) default '%m/%d/%Y';  -- Dates will come in as mm/dd/yyyy
    declare v_grade_unassigned_id  int(10);
    declare v_school_unassigned_id  int(10);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_nwea';
    set v_ods_view = concat('v_', v_ods_table);

    select  count(*)
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;

    if v_view_exists > 0 then

        select  bb_group_id
        into    v_bb_group_id
        from    pm_bbcard_group
        where   bb_group_code = 'nwea'
        ;
        
        select  grade_level_id
        into    v_grade_unassigned_id
        from    c_grade_level
        where   grade_code = 'unassigned'
        ;
        
        select school_id
        into    v_school_unassigned_id 
        from    c_school
        where   school_code = 'unassigned'
        ;
        
        set @nwea_date_format_mask := pmi_f_get_etl_setting('nweaDateFormatMask');
    
        if @nwea_date_format_mask is not null then
            set v_date_format_mask = @nwea_date_format_mask;
        end if;
        
        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_date_conversion`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_id_assign_bb_meas`;
        drop table if exists `tmp_id_assign_bb_meas_item`;
        drop table if exists `tmp_source_test_goal_list`;
        drop table if exists `tmp_pm_bbcard_measure_item`;
        drop table if exists `tmp_pm_bbcard_measure_item_base_explode`;
        
          create table `tmp_stu_admin` (
          `student_code` varchar(15) NOT NULL,
          `row_num` int(10) NOT NULL,
          `test_name` varchar(75) default null,
          `test_name_chksum` varchar(40) default null,
          `student_id` int(10) NOT NULL,
          `school_year_id` smallint(4) NOT NULL,
          `grade_code` varchar(15) default null,
          `grade_id` int(10) default null,
          `school_code` varchar(15) default null,
          `backfill_needed_flag` tinyint(1),
          primary key (`student_id`, `test_name`, `grade_code`, `school_year_id`)
        ) engine=innodb default charset=latin1
        ;
        
        create table `tmp_date_conversion` (
           `test_start_date`      date NOT NULL
          ,`test_start_date_str`  varchar(20) NOT NULL
          ,`school_year_id`       int unsigned,
         primary key (`test_start_date`),
          key (`school_year_id`)
        ) engine=innodb default charset=latin1
        ;
        
        create table `tmp_student_year_backfill` (
           `ods_row_num` int(10) not null,
           `student_id` int(10) not null,
           `school_year_id` smallint(4) not null,
           `grade_level_id` int(10) null,
           `school_id` int(10) null,
           primary key  (`ods_row_num`),
           unique key `uq_tmp_student_year_backfill` (`student_id`, `school_year_id`)
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
          `moniker` varchar(50) default null,
          `sort_order` smallint(6) default null,
          primary key  (`bb_group_id`,`bb_measure_id`,`new_id`),
          unique key `uq_tmp_id_assign_bb_meas_item` (`bb_group_id`,`bb_measure_id`,`base_code`)
        );
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
        create table `tmp_source_test_goal_list` (
          `test_name` varchar(150) not null,
          `test_name_cksum` varchar(40) not null,
          `test_type_name` varchar(50) not null,
          `discipline` varchar(50) not null default 'all',
          `goal_name_1` varchar(50) default null,
          `goal_adj_1` varchar(40) default null,
          `goal_name_1_cksum` varchar(40) default null,
          `goal_adj_1_cksum` varchar(40) default null,
          `goal_name_2` varchar(50) default null,
          `goal_adj_2` varchar(40) default null,
          `goal_name_2_cksum` varchar(40) default null,
          `goal_adj_2_cksum` varchar(40) default null,
          `goal_name_3` varchar(50) default null,
          `goal_adj_3` varchar(40) default null,
          `goal_name_3_cksum` varchar(40) default null,
          `goal_adj_3_cksum` varchar(40) default null,
          `goal_name_4` varchar(50) default null,
          `goal_adj_4` varchar(40) default null,
          `goal_name_4_cksum` varchar(40) default null,
          `goal_adj_4_cksum` varchar(40) default null,
          `goal_name_5` varchar(50) default null,
          `goal_adj_5` varchar(40) default null,
          `goal_name_5_cksum` varchar(40) default null,
          `goal_adj_5_cksum` varchar(40) default null,
          `goal_name_6` varchar(50) default null,
          `goal_adj_6` varchar(40) default null,
          `goal_name_6_cksum` varchar(40) default null,
          `goal_adj_6_cksum` varchar(40) default null,
          `goal_name_7` varchar(50) default null,
          `goal_adj_7` varchar(40) default null,
          `goal_name_7_cksum` varchar(40) default null,
          `goal_adj_7_cksum` varchar(40) default null,
          `goal_name_8` varchar(50) default null,
          `goal_adj_8` varchar(40) default null,
          `goal_name_8_cksum` varchar(40) default null,
          `goal_adj_8_cksum` varchar(40) default null,
          `goal_name_9` varchar(50) default null,
          `goal_adj_9` varchar(40) default null,
          `goal_name_9_cksum` varchar(40) default null,
          `goal_adj_9_cksum` varchar(40) default null
        );
        create table `tmp_pm_bbcard_measure_item_base_explode` (
          `bb_measure_item_code` varchar(40) not null,
          `moniker` varchar(75) not null,
          `sort_order` smallint(6) not null default '0',
          `score_sort_type_code` enum('a','m','n') not null,
          `discipline` varchar(25) not null,
          unique key `uq_tmp_pm_bbcard_measure_item_base_explode` (`bb_measure_item_code`)
        ) engine=innodb default charset=latin1
        ;


        --
        --  To expedite processing, we will determine the unique dates the tests were taken 
        --  and the current school year for those dates.
        --
        
        insert tmp_date_conversion (
            test_start_date 
           ,test_start_date_str
        )
        select distinct
            str_to_date(test_start_date, v_date_format_mask)
           ,test_start_date
        from v_pmi_ods_nwea ods;

        update tmp_date_conversion tdc
        join c_school_year sy
           on tdc.test_start_date between sy.begin_date and sy.end_date
        set tdc.school_year_id = sy.school_year_id;

        --  Get the student data and determine if backloading of Student Year information is needed.
        --  We will also get the current school year information for the date the test was
        --  administered. This year infomration will be used to generate the new report data.

        insert  tmp_stu_admin (
                row_num
               ,student_id
               ,test_name
               ,test_name_chksum
               ,student_code
               ,school_year_id
               ,grade_code
               ,grade_id
               ,school_code
               ,backfill_needed_flag
        )
        select  ods.row_num
               ,s.student_id
               ,ods.test_name
               ,md5(ods.test_name) as test_name_chksum
               ,ods.student_id
               ,tdc.school_year_id
               ,coalesce(gl.grade_code, 'unassigned') 
               ,coalesce(gl.grade_level_id, v_grade_unassigned_id)
               ,NULL -- Dont' know school code just yet
               ,case when sty.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_nwea as ods
        join    tmp_date_conversion tdc
                on ods.test_start_date = tdc.test_start_date_str
        join    c_student as s
                on    s.student_state_code = ods.student_id
        left join c_student_year as sty
                on    sty.student_id = s.student_id
                and   sty.school_year_id = tdc.school_year_id
        left join c_grade_level as gl
                on sty.grade_level_id  = gl.grade_level_id
        where   ods.student_id is not null
        union all
        select  ods.row_num
               ,s.student_id
               ,ods.test_name
               ,md5(ods.test_name) as test_name_chksum
               ,ods.student_id
               ,tdc.school_year_id
               ,coalesce(gl.grade_code, 'unassigned')
               ,coalesce(gl.grade_level_id, v_grade_unassigned_id)
               ,NULL -- Dont' know school code just yet
               ,case when sty.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_nwea as ods
        join    tmp_date_conversion tdc
                on ods.test_start_date = tdc.test_start_date_str
        join    c_student as s
                on    s.fid_code = ods.student_id
        left join c_student_year as sty
                on    sty.student_id = s.student_id
                and   sty.school_year_id = tdc.school_year_id
        left join c_grade_level as gl
                on sty.grade_level_id  = gl.grade_level_id
        where   ods.student_id is not null
        union all
        select  ods.row_num
               ,s.student_id
               ,ods.test_name
               ,md5(ods.test_name) as test_name_chksum
               ,ods.student_id
               ,tdc.school_year_id
               ,coalesce(gl.grade_code, 'unassigned')
               ,coalesce(gl.grade_level_id, v_grade_unassigned_id)
               ,NULL -- Dont' know school code just yet
               ,case when sty.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_nwea as ods
        join    tmp_date_conversion tdc
                on ods.test_start_date = tdc.test_start_date_str
        join    c_student as s
                on    s.student_code = ods.student_id
        left join c_student_year as sty
                on    sty.student_id = s.student_id
                and   sty.school_year_id = tdc.school_year_id
        left join c_grade_level as gl
                on sty.grade_level_id  = gl.grade_level_id
        where   ods.student_id is not null
        on duplicate key update row_num = values(row_num)
        ;        
        -- Now ascertain our internal grade_id based on customers grade code and if possible the school's code
        -- Note: Because school code would be very difficult to ascertain based on the incomming data file, 
        --       we have opted to flag the new student year row with an unassigned school id.
        
        update tmp_stu_admin sadmin
        left join v_pmi_xref_grade_level as gxref
                on sadmin.grade_code = gxref.client_grade_code
        left join c_grade_level as grd
                on gxref.pmi_grade_code = grd.grade_code
        set sadmin.grade_id = coalesce(grd.grade_level_id, v_grade_unassigned_id)
        ;

        -- #########################################
        -- Backfill for c_student_year 
        -- Need to detect and load c_student_year 
        -- records when supporting ones that don't exist
        -- ##############################################

        select count(*)
        into v_backfill_needed
        from tmp_stu_admin
        where backfill_needed_flag = 1
        ;

        if v_backfill_needed > 0 then

            insert tmp_student_year_backfill (
                   ods_row_num
                  ,student_id
                  ,school_year_id
                  ,grade_level_id
                  ,school_id
            )
            select   sadmin.row_num
                    ,sadmin.student_id
                    ,sadmin.school_year_id
                    ,sadmin.grade_id
                    ,v_school_unassigned_id
            from tmp_stu_admin as sadmin
            where sadmin.backfill_needed_flag = 1
            on duplicate key update grade_level_id = values(grade_level_id)
                ,school_id = values(school_id)
            ;

            -- ##########################################
            -- proc developed to standardize loading c_student_year. 
            -- This proc reads the tmp_student_year_backfill table and loads
            -- c_student_year with these rows.
            -- ############################################

            call etl_hst_load_backfill_stu_year();

        end if;

        insert tmp_source_test_goal_list (
            test_name
            ,test_name_cksum
            ,test_type_name
            ,discipline
            ,goal_name_1
            ,goal_name_1_cksum
            ,goal_name_2
            ,goal_name_2_cksum
            ,goal_name_3
            ,goal_name_3_cksum
            ,goal_name_4
            ,goal_name_4_cksum
            ,goal_name_5
            ,goal_name_5_cksum
            ,goal_name_6
            ,goal_name_6_cksum
            ,goal_name_7
            ,goal_name_7_cksum
            ,goal_name_8
            ,goal_name_8_cksum
            ,goal_name_9
            ,goal_name_9_cksum
        )

        select  distinct test_name
            ,md5(test_name)
            ,test_type_name
            ,discipline
            ,goal_name_1
            ,md5(goal_name_1)
            ,goal_name_2
            ,md5(goal_name_2)
            ,goal_name_3
            ,md5(goal_name_3)
            ,goal_name_4
            ,md5(goal_name_4)
            ,goal_name_5
            ,md5(goal_name_5)
            ,goal_name_6
            ,md5(goal_name_6)
            ,goal_name_7
            ,md5(goal_name_7)
            ,goal_name_8
            ,md5(goal_name_8)
            ,goal_name_9
            ,md5(goal_name_9)
        from    v_pmi_ods_nwea 
        ;



        insert  tmp_id_assign_bb_meas (bb_group_id, new_id, base_code, moniker)
        select  v_bb_group_id, pmi_admin.pmi_f_get_next_sequence('pm_bbcard_measure', 1), src.test_name_cksum, src.test_name
        from    tmp_source_test_goal_list as src
        left join   pm_bbcard_measure as tar
                on      tar.bb_group_id = v_bb_group_id
                and     tar.bb_measure_code = src.test_name_cksum
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
        select  tmpid.bb_group_id
            ,tmpid.new_id
            ,tmpid.base_code
            ,tmpid.moniker
            ,0
            ,null
            ,1
            ,1
            ,1234
            ,now()
        from    tmp_id_assign_bb_meas as tmpid
        ;


    insert tmp_pm_bbcard_measure_item_base_explode (
        bb_measure_item_code
        ,moniker
        ,sort_order
        ,score_sort_type_code
        ,discipline
    )
    
    values ('termName', 'Term', -9, 'a', 'all')
            ,('testDate', 'Test Date', -8, 'a', 'all')
            ,('growthMeasure', 'Growth Measure', -7, 'a', 'all')
            ,('testRITScore', 'Test RIT Score', -6, 'n', 'all')
            ,('testStdError', 'Test Std Error', -5, 'n', 'all')
            ,('testRITPct', 'Test Percentile', -4, 'n', 'all')
            ,('testRITtoReadScore', 'RIT to Reading Score', -3, 'a', 'Reading')
            ,('testRITtoReadMin', 'RIT to Reading Min', -2, 'a', 'Reading')
            ,('testRITtoReadMax', 'RIT to Reading Max', -1, 'a', 'Reading')
            ,('PctCorrect', 'Percent Correct', 10, 'n', 'all')
            ,('ProjProf', 'Projected Proficiency', 11, 'a', 'all')
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

    select  dt.test_name_cksum
        ,exp.bb_measure_item_code
        ,exp.moniker
        ,exp.sort_order
        ,null
        ,exp.score_sort_type_code
        ,1
        ,1
        ,1234
        ,now()
    
    from    (   
                select  test_name_cksum, discipline
                from    tmp_source_test_goal_list
                group by test_name_cksum, discipline
            ) as dt

    cross join    tmp_pm_bbcard_measure_item_base_explode as exp
            on  exp.discipline = 'all'
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

    select  dt.test_name_cksum
        ,exp.bb_measure_item_code
        ,exp.moniker
        ,exp.sort_order
        ,null
        ,exp.score_sort_type_code
        ,1
        ,1
        ,1234
        ,now()
    
    from    (   
                select  test_name_cksum, discipline
                from    tmp_source_test_goal_list
                where   discipline = 'Reading'
                group by test_name_cksum, discipline
            ) as dt

    join    tmp_pm_bbcard_measure_item_base_explode as exp
            on  exp.discipline = dt.discipline
    on duplicate key update last_user_id = values(last_user_id)
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

    select  test_name_cksum as bb_measure_code
        ,goal_name_1_cksum as bb_measure_item_code
        ,goal_name_1 as moniker
        ,1 as sort_order
        ,null as swatch_id
        ,'n' as score_sort_type_code
        ,1 as active_flag
        ,1 as dynamic_creation_flag
        ,1234 as last_user_id
        ,now() as create_timestamp

    from    tmp_source_test_goal_list as src1
    where   src1.goal_name_1_cksum is not null
    union
    select  test_name_cksum as bb_measure_code
        ,goal_name_2_cksum as bb_measure_item_code
        ,goal_name_2 as moniker
        ,2 as sort_order
        ,null as swatch_id
        ,'n' as score_sort_type_code
        ,1 as active_flag
        ,1 as dynamic_creation_flag
        ,1234 as last_user_id
        ,now() as create_timestamp

    from    tmp_source_test_goal_list as src2
    where   src2.goal_name_2_cksum is not null
    union
    select  test_name_cksum as bb_measure_code
        ,goal_name_3_cksum as bb_measure_item_code
        ,goal_name_3 as moniker
        ,3 as sort_order
        ,null as swatch_id
        ,'n' as score_sort_type_code
        ,1 as active_flag
        ,1 as dynamic_creation_flag
        ,1234 as last_user_id
        ,now() as create_timestamp

    from    tmp_source_test_goal_list as src3
    where   src3.goal_name_3_cksum is not null
    union
    select  test_name_cksum as bb_measure_code
        ,goal_name_4_cksum as bb_measure_item_code
        ,goal_name_4 as moniker
        ,4 as sort_order
        ,null as swatch_id
        ,'n' as score_sort_type_code
        ,1 as active_flag
        ,1 as dynamic_creation_flag
        ,1234 as last_user_id
        ,now() as create_timestamp

    from    tmp_source_test_goal_list as src4
    where   src4.goal_name_4_cksum is not null
    union
    select  test_name_cksum as bb_measure_code
        ,goal_name_5_cksum as bb_measure_item_code
        ,goal_name_5 as moniker
        ,5 as sort_order
        ,null as swatch_id
        ,'n' as score_sort_type_code
        ,1 as active_flag
        ,1 as dynamic_creation_flag
        ,1234 as last_user_id
        ,now() as create_timestamp

    from    tmp_source_test_goal_list as src5
    where   src5.goal_name_5_cksum is not null
    union
    select  test_name_cksum as bb_measure_code
        ,goal_name_6_cksum as bb_measure_item_code
        ,goal_name_6 as moniker
        ,6 as sort_order
        ,null as swatch_id
        ,'n' as score_sort_type_code
        ,1 as active_flag
        ,1 as dynamic_creation_flag
        ,1234 as last_user_id
        ,now() as create_timestamp

    from    tmp_source_test_goal_list as src6
    where   src6.goal_name_6_cksum is not null
    union
    select  test_name_cksum as bb_measure_code
        ,goal_name_7_cksum as bb_measure_item_code
        ,goal_name_7 as moniker
        ,7 as sort_order
        ,null as swatch_id
        ,'n' as score_sort_type_code
        ,1 as active_flag
        ,1 as dynamic_creation_flag
        ,1234 as last_user_id
        ,now() as create_timestamp

    from    tmp_source_test_goal_list as src7
    where   src7.goal_name_7_cksum is not null
    union
    select  test_name_cksum as bb_measure_code
        ,goal_name_8_cksum as bb_measure_item_code
        ,goal_name_8 as moniker
        ,8 as sort_order
        ,null as swatch_id
        ,'n' as score_sort_type_code
        ,1 as active_flag
        ,1 as dynamic_creation_flag
        ,1234 as last_user_id
        ,now() as create_timestamp

    from    tmp_source_test_goal_list as src8
    where   src8.goal_name_8_cksum is not null
    union
    select  test_name_cksum as bb_measure_code
        ,goal_name_9_cksum as bb_measure_item_code
        ,goal_name_9 as moniker
        ,9 as sort_order
        ,null as swatch_id
        ,'n' as score_sort_type_code
        ,1 as active_flag
        ,1 as dynamic_creation_flag
        ,1234 as last_user_id
        ,now() as create_timestamp

    from    tmp_source_test_goal_list as src9
    where   src9.goal_name_9_cksum is not null
    ;


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
            on      bm.bb_group_id = v_bb_group_id
            and     bm.bb_measure_code = src.bb_measure_code
    left join   pm_bbcard_measure_item as tar
            on      tar.bb_group_id = bm.bb_group_id
            and     tar.bb_measure_id = bm.bb_measure_id
            and     tar.bb_measure_item_code = src.bb_measure_item_code
    where   tar.bb_measure_item_id is null
    ;


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
            on      bm.bb_group_id = v_bb_group_id
            and     bm.bb_measure_code = src.bb_measure_code
    left join   tmp_id_assign_bb_meas_item as tmpid
            on      tmpid.bb_group_id = bm.bb_group_id
            and     tmpid.bb_measure_id = bm.bb_measure_id
            and     tmpid.base_code = src.bb_measure_item_code
    left join   pm_bbcard_measure_item as tar
            on      tar.bb_group_id = v_bb_group_id
            and     tar.bb_measure_id = bm.bb_measure_id
            and     tar.bb_measure_item_code = src.bb_measure_item_code
    on duplicate key update moniker = values(moniker)
        ,sort_order = values(sort_order)
        ,last_user_id = values(last_user_id)
    ;

        -- Incoming report data will be incremental in nature so we only want to add/update report records

        insert rpt_bbcard_detail_nwea (
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

        select  v_bb_group_id
            ,m.bb_measure_id
            ,mi.bb_measure_item_id
            ,s.student_id
            ,tdc.school_year_id
            ,max(case when mi.sort_order = -9 then substring_index(ods.term_name, ' ', 1)
                    when mi.sort_order = -8 then ods.test_start_date
                    when mi.sort_order = -7 then ods.growth_measure_flag
                    when mi.sort_order = -6 then ods.test_rit_score
                    when mi.sort_order = -5 then ods.test_std_err
                    when mi.sort_order = -4 then ods.test_percentile
                    when mi.sort_order = -3 then ods.rit_reading_score
                    when mi.sort_order = -2 then ods.rit_reading_min
                    when mi.sort_order = -1 then ods.rit_reading_max
                    when mi.sort_order = 1 then ods.goal_rit_score_1
                    when mi.sort_order = 2 then ods.goal_rit_score_2
                    when mi.sort_order = 3 then ods.goal_rit_score_3
                    when mi.sort_order = 4 then ods.goal_rit_score_4
                    when mi.sort_order = 5 then ods.goal_rit_score_5
                    when mi.sort_order = 6 then ods.goal_rit_score_6
                    when mi.sort_order = 7 then ods.goal_rit_score_7
                    when mi.sort_order = 8 then ods.goal_rit_score_8
                    when mi.sort_order = 9 then ods.goal_rit_score_9
                    when mi.sort_order = 10 then ods.percent_correct
                    when mi.sort_order = 11 then ods.projected_proficiency
                end ) as score
            ,max(case when mi.sort_order = -9 then mi.score_sort_type_code
                  when mi.sort_order = -8 then mi.score_sort_type_code
                  when mi.sort_order = -7 then mi.score_sort_type_code
                  when mi.sort_order = -6 then mi.score_sort_type_code
                  when mi.sort_order = -5 then mi.score_sort_type_code
                  when mi.sort_order = -4 then mi.score_sort_type_code
                  when mi.sort_order = -3 then mi.score_sort_type_code
                  when mi.sort_order = -2 then mi.score_sort_type_code
                  when mi.sort_order = -1 then mi.score_sort_type_code
                  when mi.sort_order = 1 then mi.score_sort_type_code
                  when mi.sort_order = 2 then mi.score_sort_type_code
                  when mi.sort_order = 3 then mi.score_sort_type_code
                  when mi.sort_order = 4 then mi.score_sort_type_code
                  when mi.sort_order = 5 then mi.score_sort_type_code
                  when mi.sort_order = 6 then mi.score_sort_type_code
                  when mi.sort_order = 7 then mi.score_sort_type_code
                  when mi.sort_order = 8 then mi.score_sort_type_code
                  when mi.sort_order = 9 then mi.score_sort_type_code
                  when mi.sort_order = 10 then mi.score_sort_type_code
                  when mi.sort_order = 11 then mi.score_sort_type_code
                end ) as score_type
            , NULL  -- Don't know color yet
            ,1234
            ,now()
        from    v_pmi_ods_nwea as ods
        join    tmp_date_conversion as tdc
                on ods.test_start_date = tdc.test_start_date_str
        join    tmp_stu_admin as sadmin
                on ods.row_num = sadmin.row_num
        join    c_student as s
                on      s.student_code = ods.student_id
        join    pm_bbcard_measure as m
                on      m.bb_group_id = v_bb_group_id
                and     m.bb_measure_code = sadmin.test_name_chksum
        join    pm_bbcard_measure_item as mi
                on      m.bb_group_id = mi.bb_group_id
                   and  m.bb_measure_id = mi.bb_measure_id
        group by m.bb_group_id
            ,m.bb_measure_id
            ,mi.bb_measure_item_id
            ,s.student_id
            ,tdc.school_year_id
        having score is not null
        on duplicate key update score = values(score)
            ,score_type = values(score_type)
            ,score_color = values(score_color)
            ,last_user_id = values(last_user_id)
        ;

        update rpt_bbcard_detail_nwea as rpt
        join pm_bbcard_measure_item mi
                on   rpt.bb_group_id = mi.bb_group_id
                 and rpt.bb_measure_id = mi.bb_measure_id
                 and rpt.bb_measure_item_id = mi.bb_measure_item_id
                 and mi.bb_measure_item_code = 'growthMeasure'
        set rpt.score_color = CASE rpt.score
                 when 'True'  then 'Green'
                 when 'False' then 'Red'
        end;

        ##########################
        ## working table cleanup
        ##########################

        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_date_conversion`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_id_assign_bb_meas`;
        drop table if exists `tmp_id_assign_bb_meas_item`;
        drop table if exists `tmp_source_test_goal_list`;
        drop table if exists `tmp_pm_bbcard_measure_item`;
        drop table if exists `tmp_pm_bbcard_measure_item_base_explode`;

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
