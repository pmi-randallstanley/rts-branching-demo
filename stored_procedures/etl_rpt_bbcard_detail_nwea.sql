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
        drop table if exists `tmp_stu_admin_dups`;
        drop table if exists `tmp_date_conversion`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_id_assign_bb_meas`;
        drop table if exists `tmp_id_assign_bb_meas_item`;
        drop table if exists `tmp_source_test_list`;
        drop table if exists `tmp_source_test_term_goal_list`;
        drop table if exists `tmp_pm_bbcard_measure_item`;
        drop table if exists `tmp_pm_bbcard_measure_item_base_explode`;
        
        create table `tmp_stu_admin` (
          `student_code` varchar(15) NOT NULL,
          `row_num` int(10) NOT NULL,
          `test_name` varchar(75) default null,
          `test_name_chksum` varchar(40) default null,
          `term_name_season` varchar(20) default null,
          `term_name_full` varchar(25) default null,
          `student_id` int(10) NOT NULL,
          `school_year_id` smallint(4) NOT NULL,
          `grade_code` varchar(15) default null,
          `grade_id` int(10) default null,
          `school_code` varchar(15) default null,
          `backfill_needed_flag` tinyint(1),
          primary key (`student_id`, `test_name`,`term_name_season`,`school_year_id`),
          unique key `uq_tmp_stu_admin` (`student_code`,`test_name_chksum`,`term_name_season`,`school_year_id`)
        ) engine=innodb default charset=latin1
        ;

        create table `tmp_stu_admin_dups` (
          `student_code` varchar(15) NOT NULL,
          `test_name` varchar(75) default null,
          `term_name_season` varchar(20) default null,
          `school_year_id` smallint(4) NOT NULL,
          `test_start_date` date default null,
          `test_start_date_str`  varchar(20) NOT NULL,
          `row_num` int(10) default NULL,
          unique key `uq_tmp_stu_admin_dups` (`student_code`, `test_name`,`term_name_season`,`school_year_id`)
        ) engine=innodb default charset=latin1
        ;
        
        create table `tmp_date_conversion` (
          `test_start_date`      date NOT NULL,
          `test_start_date_str`  varchar(20) NOT NULL,
          `school_year_id`       int unsigned,
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
        create table `tmp_source_test_term_goal_list` (
          `test_name` varchar(150) not null,
          `term_name` varchar(20) not null,
          `test_name_cksum` varchar(40) not null,
          `test_type_name` varchar(50) not null,
          `discipline` varchar(50) not null default 'all',
          `goal_name_1` varchar(50) default null,
          `goal_bbmeas_item_code_score_1` varchar(40) default null,
          `goal_bbmeas_item_code_adj_1` varchar(40) default null,
          `goal_name_2` varchar(50) default null,
          `goal_bbmeas_item_code_score_2` varchar(40) default null,
          `goal_bbmeas_item_code_adj_2` varchar(40) default null,
          `goal_name_3` varchar(50) default null,
          `goal_bbmeas_item_code_score_3` varchar(40) default null,
          `goal_bbmeas_item_code_adj_3` varchar(40) default null,
          `goal_name_4` varchar(50) default null,
          `goal_bbmeas_item_code_score_4` varchar(40) default null,
          `goal_bbmeas_item_code_adj_4` varchar(40) default null,
          `goal_name_5` varchar(50) default null,
          `goal_bbmeas_item_code_score_5` varchar(40) default null,
          `goal_bbmeas_item_code_adj_5` varchar(40) default null,
          `goal_name_6` varchar(50) default null,
          `goal_bbmeas_item_code_score_6` varchar(40) default null,
          `goal_bbmeas_item_code_adj_6` varchar(40) default null,
          `goal_name_7` varchar(50) default null,
          `goal_bbmeas_item_code_score_7` varchar(40) default null,
          `goal_bbmeas_item_code_adj_7` varchar(40) default null,
          `goal_name_8` varchar(50) default null,
          `goal_bbmeas_item_code_score_8` varchar(40) default null,
          `goal_bbmeas_item_code_adj_8` varchar(40) default null,
          `goal_name_9` varchar(50) default null,
          `goal_bbmeas_item_code_score_9` varchar(40) default null,
          `goal_bbmeas_item_code_adj_9` varchar(40) default null
        );
        create table `tmp_source_test_list` (
          `test_name` varchar(150) not null,
          `test_name_cksum` varchar(40) not null,
          `test_type_name` varchar(50) not null,
          `discipline` varchar(50) not null default 'all'
        );

        create table `tmp_pm_bbcard_measure_item_base_explode` (
          `bb_measure_item_code` varchar(40) not null,
          `moniker` varchar(75) not null,
          `sort_order` smallint(6) not null default '0',
          `score_sort_type_code` enum('a','m','n') not null,
          `discipline` varchar(25) not null,
          `term` varchar(20) not null,
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


        # manage dups
        insert  tmp_stu_admin_dups (
            student_code
            ,test_name
            ,term_name_season
            ,school_year_id
            ,test_start_date
            ,test_start_date_str
        )

        select  ods.student_id
            ,ods.test_name
            ,substring_index(ods.term_name, ' ', 1) as term_name_season
            ,tdc.school_year_id
            ,max(tdc.test_start_date) as max_test_start_date
            ,date_format(max(tdc.test_start_date), v_date_format_mask) as test_start_date_str
        
        from    v_pmi_ods_nwea as ods
        join    tmp_date_conversion as tdc
                on ods.test_start_date = tdc.test_start_date_str
        group by student_id, test_name, term_name_season, school_year_id
        having count(*) > 1
        ;

        update  tmp_stu_admin_dups as upd
        join    v_pmi_ods_nwea as ods
                on      upd.student_code = ods.student_id
                and     upd.test_name = ods.test_name
                and     upd.test_start_date_str = ods.test_start_date
        set     upd.row_num = ods.row_num
        ;


        insert  tmp_stu_admin (
            row_num
            ,student_id
            ,test_name
            ,test_name_chksum
            ,term_name_season
            ,term_name_full
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
            ,dups.term_name_season
            ,ods.term_name
            ,ods.student_id
            ,tdc.school_year_id
            ,coalesce(gl.grade_code, 'unassigned')
            ,coalesce(gl.grade_level_id, v_grade_unassigned_id)
            ,NULL -- Dont' know school code just yet
            ,case when sty.school_year_id is null then 1 end as backfill_needed_flag

        from    v_pmi_ods_nwea as ods
        join    tmp_stu_admin_dups as dups
                on      ods.row_num = dups.row_num
        join    tmp_date_conversion as tdc
                on dups.test_start_date = tdc.test_start_date
        join    c_student as s
                on    s.student_code = dups.student_code
        left join c_student_year as sty
                on    sty.student_id = s.student_id
                and   sty.school_year_id = tdc.school_year_id
        left join c_grade_level as gl
                on sty.grade_level_id  = gl.grade_level_id
        where   ods.student_id is not null
        ;


        insert  tmp_stu_admin (
            row_num
            ,student_id
            ,test_name
            ,test_name_chksum
            ,term_name_season
            ,term_name_full
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
            ,dups.term_name_season
            ,ods.term_name
            ,ods.student_id
            ,tdc.school_year_id
            ,coalesce(gl.grade_code, 'unassigned') 
            ,coalesce(gl.grade_level_id, v_grade_unassigned_id)
            ,NULL -- Dont' know school code just yet
            ,case when sty.school_year_id is null then 1 end as backfill_needed_flag

        from    v_pmi_ods_nwea as ods
        join    tmp_stu_admin_dups as dups
                on      ods.row_num = dups.row_num
        join    tmp_date_conversion as tdc
                on dups.test_start_date = tdc.test_start_date
        join    c_student as s
                on    s.student_state_code = dups.student_code
        left join   tmp_stu_admin as tar
                on      tar.student_code = ods.student_id
                and     tar.term_name_full = ods.term_name
                and     tar.school_year_id = tdc.school_year_id
        left join   c_student_year as sty
                on    sty.student_id = s.student_id
                and   sty.school_year_id = tdc.school_year_id
        left join c_grade_level as gl
                on sty.grade_level_id  = gl.grade_level_id
        where   ods.student_id is not null
        and     tar.student_id is null
        ;

        insert  tmp_stu_admin (
            row_num
            ,student_id
            ,test_name
            ,test_name_chksum
            ,term_name_season
            ,term_name_full
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
            ,dups.term_name_season
            ,ods.term_name
            ,ods.student_id
            ,tdc.school_year_id
            ,coalesce(gl.grade_code, 'unassigned')
            ,coalesce(gl.grade_level_id, v_grade_unassigned_id)
            ,NULL -- Dont' know school code just yet
            ,case when sty.school_year_id is null then 1 end as backfill_needed_flag

        from    v_pmi_ods_nwea as ods
        join    tmp_stu_admin_dups as dups
                on      ods.row_num = dups.row_num
        join    tmp_date_conversion as tdc
                on dups.test_start_date = tdc.test_start_date
        join    c_student as s
                on    s.fid_code = dups.student_code
        left join   tmp_stu_admin as tar
                on      tar.student_code = ods.student_id
                and     tar.term_name_full = ods.term_name
                and     tar.school_year_id = tdc.school_year_id
        left join c_student_year as sty
                on    sty.student_id = s.student_id
                and   sty.school_year_id = tdc.school_year_id
        left join c_grade_level as gl
                on sty.grade_level_id  = gl.grade_level_id
        where   ods.student_id is not null
        and     tar.student_id is null
        ;        


        # Non-dups
        insert  tmp_stu_admin (
            row_num
           ,student_id
           ,test_name
           ,test_name_chksum
           ,term_name_season
           ,term_name_full
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
            ,substring_index(ods.term_name, ' ', 1) as term_name_season
            ,ods.term_name
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
        left join   tmp_stu_admin as tar
                on      tar.student_code = ods.student_id
                and     tar.test_name = ods.test_name
                and     tar.term_name_full = ods.term_name
                and     tar.school_year_id = tdc.school_year_id
        left join c_student_year as sty
                on    sty.student_id = s.student_id
                and   sty.school_year_id = tdc.school_year_id
        left join c_grade_level as gl
                on sty.grade_level_id  = gl.grade_level_id
        where   ods.student_id is not null
        and     tar.student_id is null
        on duplicate key update row_num = values(row_num)
        ;


        insert  tmp_stu_admin (
            row_num
            ,student_id
            ,test_name
            ,test_name_chksum
            ,term_name_season
            ,term_name_full
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
            ,substring_index(ods.term_name, ' ', 1) as term_name_season
            ,ods.term_name
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
        left join   tmp_stu_admin as tar
                on      tar.student_code = ods.student_id
                and     tar.test_name = ods.test_name
                and     tar.term_name_full = ods.term_name
                and     tar.school_year_id = tdc.school_year_id
        left join   c_student_year as sty
                on    sty.student_id = s.student_id
                and   sty.school_year_id = tdc.school_year_id
        left join c_grade_level as gl
                on sty.grade_level_id  = gl.grade_level_id
        where   ods.student_id is not null
        and     tar.student_id is null
        on duplicate key update row_num = values(row_num)
        ;

        insert  tmp_stu_admin (
            row_num
            ,student_id
            ,test_name
            ,test_name_chksum
            ,term_name_season
            ,term_name_full
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
            ,substring_index(ods.term_name, ' ', 1) as term_name_season
            ,ods.term_name
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
        left join   tmp_stu_admin as tar
                on      tar.student_code = ods.student_id
                and     tar.test_name = ods.test_name
                and     tar.term_name_full = ods.term_name
                and     tar.school_year_id = tdc.school_year_id
        left join c_student_year as sty
                on    sty.student_id = s.student_id
                and   sty.school_year_id = tdc.school_year_id
        left join c_grade_level as gl
                on sty.grade_level_id  = gl.grade_level_id
        where   ods.student_id is not null
        and     tar.student_id is null
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

        insert tmp_source_test_term_goal_list (
            test_name
            ,term_name
            ,test_name_cksum
            ,test_type_name
            ,discipline
            ,goal_name_1
            ,goal_bbmeas_item_code_score_1
            ,goal_bbmeas_item_code_adj_1
            ,goal_name_2
            ,goal_bbmeas_item_code_score_2
            ,goal_bbmeas_item_code_adj_2
            ,goal_name_3
            ,goal_bbmeas_item_code_score_3
            ,goal_bbmeas_item_code_adj_3
            ,goal_name_4
            ,goal_bbmeas_item_code_score_4
            ,goal_bbmeas_item_code_adj_4
            ,goal_name_5
            ,goal_bbmeas_item_code_score_5
            ,goal_bbmeas_item_code_adj_5
            ,goal_name_6
            ,goal_bbmeas_item_code_score_6
            ,goal_bbmeas_item_code_adj_6
            ,goal_name_7
            ,goal_bbmeas_item_code_score_7
            ,goal_bbmeas_item_code_adj_7
            ,goal_name_8
            ,goal_bbmeas_item_code_score_8
            ,goal_bbmeas_item_code_adj_8
            ,goal_name_9
            ,goal_bbmeas_item_code_score_9
            ,goal_bbmeas_item_code_adj_9
        )

        select  distinct test_name
            ,substring_index(term_name, ' ', 1)
            ,md5(test_name)
            ,test_type_name
            ,discipline
            ,goal_name_1
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_1, 'Score'))
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_1, 'Adj'))
            ,goal_name_2
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_2, 'Score'))
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_2, 'Adj'))
            ,goal_name_3
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_3, 'Score'))
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_3, 'Adj'))
            ,goal_name_4
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_4, 'Score'))
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_4, 'Adj'))
            ,goal_name_5
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_5, 'Score'))
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_5, 'Adj'))
            ,goal_name_6
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_6, 'Score'))
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_6, 'Adj'))
            ,goal_name_7
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_7, 'Score'))
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_7, 'Adj'))
            ,goal_name_8
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_8, 'Score'))
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_8, 'Adj'))
            ,goal_name_9
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_9, 'Score'))
            ,md5(concat(substring_index(term_name, ' ', 1), goal_name_9, 'Adj'))
        from    v_pmi_ods_nwea 
        ;

        insert tmp_source_test_list (
            test_name
            ,test_name_cksum
            ,test_type_name
            ,discipline
        )
        

        select  distinct test_name
            ,test_name_cksum
            ,test_type_name
            ,discipline

        from    tmp_source_test_term_goal_list
        ;

        insert  tmp_id_assign_bb_meas (bb_group_id, new_id, base_code, moniker)
        select  v_bb_group_id, pmi_admin.pmi_f_get_next_sequence('pm_bbcard_measure', 1), src.test_name_cksum, src.test_name
        from    tmp_source_test_list as src
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
            ,term
        )
        
        values ('testDateFall', 'Test Date - Fall', 101, 'a', 'all', 'Fall')
                ,('testDateWtr', 'Test Date - Winter', 102, 'a', 'all', 'Winter')
                ,('testDateSpr', 'Test Date - Spring', 103, 'a', 'all', 'Spring')
                ,('growthMeasureFall', 'Growth Measure - Fall', 201, 'a', 'all', 'Fall')
                ,('growthMeasureWtr', 'Growth Measure - Winter', 202, 'a', 'all', 'Winter')
                ,('growthMeasureSpr', 'Growth Measure - Spring', 203, 'a', 'all', 'Spring')
                ,('testRITScoreFall', 'Test RIT Score - Fall', 301, 'n', 'all', 'Fall')
                ,('testRITScoreWtr', 'Test RIT Score - Winter', 302, 'n', 'all', 'Winter')
                ,('testRITScoreSpr', 'Test RIT Score - Spring', 303, 'n', 'all', 'Spring')
                ,('testStdErrorFall', 'Test Std Error - Fall', 401, 'n', 'all', 'Fall')
                ,('testStdErrorWtr', 'Test Std Error - Winter', 402, 'n', 'all', 'Winter')
                ,('testStdErrorSpr', 'Test Std Error - Spring', 403, 'n', 'all', 'Spring')
                ,('testRITPctFall', 'Test Percentile - Fall', 501, 'n', 'all', 'Fall')
                ,('testRITPctWtr', 'Test Percentile - Winter', 502, 'n', 'all', 'Winter')
                ,('testRITPctSpr', 'Test Percentile - Spring', 503, 'n', 'all', 'Spring')
                ,('testRITtoReadScoreFall', 'RIT to Reading Score - Fall', 601, 'a', 'Reading', 'Fall')
                ,('testRITtoReadScoreWtr', 'RIT to Reading Score - Winter', 701, 'a', 'Reading', 'Winter')
                ,('testRITtoReadScoreSpr', 'RIT to Reading Score - Spring', 801, 'a', 'Reading', 'Spring')
                ,('testRITtoReadMinFall', 'RIT to Reading Min - Fall', 602, 'a', 'Reading', 'Fall')
                ,('testRITtoReadMinWtr', 'RIT to Reading Min - Winter', 702, 'a', 'Reading', 'Winter')
                ,('testRITtoReadMinSpr', 'RIT to Reading Min - Spring', 802, 'a', 'Reading', 'Spring')
                ,('testRITtoReadMaxFall', 'RIT to Reading Max - Fall', 603, 'a', 'Reading', 'Fall')
                ,('testRITtoReadMaxWtr', 'RIT to Reading Max - Winter', 703, 'a', 'Reading', 'Winter')
                ,('testRITtoReadMaxSpr', 'RIT to Reading Max - Spring', 803, 'a', 'Reading', 'Spring')
                ,('PctCorrectFall', 'Percent Correct - Fall', 1801, 'n', 'all', 'Fall')
                ,('PctCorrectWtr', 'Percent Correct - Winter', 1802, 'n', 'all', 'Winter')
                ,('PctCorrectSpr', 'Percent Correct - Spring', 1803, 'n', 'all', 'Spring')
                ,('ProjProfFall', 'Projected Proficiency - Fall', 1901, 'a', 'all', 'Fall')
                ,('ProjProfWtr', 'Projected Proficiency - Winter', 1902, 'a', 'all', 'Winter')
                ,('ProjProfSpr', 'Projected Proficiency - Spring', 1903, 'a', 'all', 'Spring')
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
    
        select  gl.test_name_cksum
            ,exp.bb_measure_item_code
            ,exp.moniker
            ,exp.sort_order
            ,null
            ,exp.score_sort_type_code
            ,1
            ,1
            ,1234
            ,now()
        
        from    tmp_source_test_term_goal_list as gl
        join    tmp_pm_bbcard_measure_item_base_explode as exp
                on      gl.term_name = exp.term
                and     exp.discipline = 'all'
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
    
        select  gl.test_name_cksum
            ,exp.bb_measure_item_code
            ,exp.moniker
            ,exp.sort_order
            ,null
            ,exp.score_sort_type_code
            ,1
            ,1
            ,1234
            ,now()
        
        from    tmp_source_test_term_goal_list as gl
        join    tmp_pm_bbcard_measure_item_base_explode as exp
                on      gl.term_name = exp.term
                and     exp.discipline = gl.discipline
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
    
        select  src1.test_name_cksum as bb_measure_code
            ,src1.goal_bbmeas_item_code_score_1 as bb_measure_item_code
            ,concat(src1.goal_name_1, ' - Score - ', src1.term_name)  as moniker
            ,case src1.term_name 
                    when 'Fall' then 901
                    when 'Winter' then 903
                    when 'Spring' then 905
             end as sort_order
            ,null as swatch_id
            ,'n' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src1
        where   src1.goal_bbmeas_item_code_score_1 is not null
        union
        select  src2.test_name_cksum as bb_measure_code
            ,src2.goal_bbmeas_item_code_adj_1 as bb_measure_item_code
            ,concat(src2.goal_name_1, ' - Adj - ', src2.term_name)  as moniker
            ,case src2.term_name 
                    when 'Fall' then 902
                    when 'Winter' then 904
                    when 'Spring' then 906
             end as sort_order
            ,null as swatch_id
            ,'a' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src2
        where   src2.goal_bbmeas_item_code_adj_1 is not null
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

        select  src3.test_name_cksum as bb_measure_code
            ,src3.goal_bbmeas_item_code_score_2 as bb_measure_item_code
            ,concat(src3.goal_name_2, ' - Score - ', src3.term_name) as moniker
            ,case src3.term_name 
                    when 'Fall' then 1001
                    when 'Winter' then 1003
                    when 'Spring' then 1005
             end as sort_order
            ,null as swatch_id
            ,'n' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src3
        where   src3.goal_bbmeas_item_code_score_2 is not null
        union
        select  src4.test_name_cksum as bb_measure_code
            ,src4.goal_bbmeas_item_code_adj_2 as bb_measure_item_code
            ,concat(src4.goal_name_2, ' - Adj - ', src4.term_name) as moniker
            ,case src4.term_name 
                    when 'Fall' then 1002
                    when 'Winter' then 1004
                    when 'Spring' then 1006
             end as sort_order
            ,null as swatch_id
            ,'a' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src4
        where   src4.goal_bbmeas_item_code_adj_2 is not null
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

        select  src5.test_name_cksum as bb_measure_code
            ,src5.goal_bbmeas_item_code_score_3 as bb_measure_item_code
            ,concat(src5.goal_name_3, ' - Score - ', src5.term_name) as moniker
            ,case src5.term_name 
                    when 'Fall' then 1101
                    when 'Winter' then 1103
                    when 'Spring' then 1105
             end as sort_order
            ,null as swatch_id
            ,'n' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src5
        where   src5.goal_bbmeas_item_code_score_3 is not null
        union
        select  src6.test_name_cksum as bb_measure_code
            ,src6.goal_bbmeas_item_code_adj_3 as bb_measure_item_code
            ,concat(src6.goal_name_3, ' - Adj - ', src6.term_name) as moniker
            ,case src6.term_name 
                    when 'Fall' then 1102
                    when 'Winter' then 1104
                    when 'Spring' then 1106
             end as sort_order
            ,null as swatch_id
            ,'a' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src6
        where   src6.goal_bbmeas_item_code_adj_3 is not null
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

        select  src7.test_name_cksum as bb_measure_code
            ,src7.goal_bbmeas_item_code_score_4 as bb_measure_item_code
            ,concat(src7.goal_name_4, ' - Score - ', src7.term_name) as moniker
            ,case src7.term_name 
                    when 'Fall' then 1201
                    when 'Winter' then 1203
                    when 'Spring' then 1205
             end as sort_order
            ,null as swatch_id
            ,'n' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src7
        where   src7.goal_bbmeas_item_code_score_4 is not null
        union
        select  src8.test_name_cksum as bb_measure_code
            ,src8.goal_bbmeas_item_code_adj_4 as bb_measure_item_code
            ,concat(src8.goal_name_4, ' - Adj - ', src8.term_name) as moniker
            ,case src8.term_name 
                    when 'Fall' then 1202
                    when 'Winter' then 1204
                    when 'Spring' then 1206
             end as sort_order
            ,null as swatch_id
            ,'a' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src8
        where   src8.goal_bbmeas_item_code_adj_4 is not null
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

        select  src9.test_name_cksum as bb_measure_code
            ,src9.goal_bbmeas_item_code_score_5 as bb_measure_item_code
            ,concat(src9.goal_name_5, ' - Score - ', src9.term_name) as moniker
            ,case src9.term_name 
                    when 'Fall' then 1301
                    when 'Winter' then 1303
                    when 'Spring' then 1305
             end as sort_order
            ,null as swatch_id
            ,'n' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src9
        where   src9.goal_bbmeas_item_code_score_5 is not null
        union
        select  src10.test_name_cksum as bb_measure_code
            ,src10.goal_bbmeas_item_code_adj_5 as bb_measure_item_code
            ,concat(src10.goal_name_5, ' - Adj - ', src10.term_name) as moniker
            ,case src10.term_name 
                    when 'Fall' then 1302
                    when 'Winter' then 1304
                    when 'Spring' then 1306
             end as sort_order
            ,null as swatch_id
            ,'a' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src10
        where   src10.goal_bbmeas_item_code_adj_5 is not null
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

        select  src11.test_name_cksum as bb_measure_code
            ,src11.goal_bbmeas_item_code_score_6 as bb_measure_item_code
            ,concat(src11.goal_name_6, ' - Score - ', src11.term_name) as moniker
            ,case src11.term_name 
                    when 'Fall' then 1401
                    when 'Winter' then 1403
                    when 'Spring' then 1405
             end as sort_order
            ,null as swatch_id
            ,'n' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src11
        where   src11.goal_bbmeas_item_code_score_6 is not null
        union
        select  src12.test_name_cksum as bb_measure_code
            ,src12.goal_bbmeas_item_code_adj_6 as bb_measure_item_code
            ,concat(src12.goal_name_6, ' - Adj - ', src12.term_name) as moniker
            ,case src12.term_name 
                    when 'Fall' then 1402
                    when 'Winter' then 1404
                    when 'Spring' then 1406
             end as sort_order
            ,null as swatch_id
            ,'a' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src12
        where   src12.goal_bbmeas_item_code_adj_6 is not null
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

        select  src13.test_name_cksum as bb_measure_code
            ,src13.goal_bbmeas_item_code_score_7 as bb_measure_item_code
            ,concat(src13.goal_name_7, ' - Score - ', src13.term_name) as moniker
            ,case src13.term_name 
                    when 'Fall' then 1501
                    when 'Winter' then 1503
                    when 'Spring' then 1505
             end as sort_order
            ,null as swatch_id
            ,'n' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src13
        where   src13.goal_bbmeas_item_code_score_7 is not null
        union
        select  src14.test_name_cksum as bb_measure_code
            ,src14.goal_bbmeas_item_code_adj_7 as bb_measure_item_code
            ,concat(src14.goal_name_7, ' - Adj - ', src14.term_name) as moniker
            ,case src14.term_name 
                    when 'Fall' then 1502
                    when 'Winter' then 1504
                    when 'Spring' then 1506
             end as sort_order
            ,null as swatch_id
            ,'a' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src14
        where   src14.goal_bbmeas_item_code_adj_7 is not null
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

        select  src15.test_name_cksum as bb_measure_code
            ,src15.goal_bbmeas_item_code_score_8 as bb_measure_item_code
            ,concat(src15.goal_name_8, ' - Score - ', src15.term_name) as moniker
            ,case src15.term_name 
                    when 'Fall' then 1601
                    when 'Winter' then 1603
                    when 'Spring' then 1605
             end as sort_order
            ,null as swatch_id
            ,'n' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src15
        where   src15.goal_bbmeas_item_code_score_8 is not null
        union
        select  src16.test_name_cksum as bb_measure_code
            ,src16.goal_bbmeas_item_code_adj_8 as bb_measure_item_code
            ,concat(src16.goal_name_8, ' - Adj - ', src16.term_name) as moniker
            ,case src16.term_name 
                    when 'Fall' then 1602
                    when 'Winter' then 1604
                    when 'Spring' then 1606
             end as sort_order
            ,null as swatch_id
            ,'a' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src16
        where   src16.goal_bbmeas_item_code_adj_8 is not null
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

        select  src17.test_name_cksum as bb_measure_code
            ,src17.goal_bbmeas_item_code_score_9 as bb_measure_item_code
            ,concat(src17.goal_name_9, ' - Score - ', src17.term_name) as moniker
            ,case src17.term_name 
                    when 'Fall' then 1701
                    when 'Winter' then 1703
                    when 'Spring' then 1705
             end as sort_order
            ,null as swatch_id
            ,'n' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src17
        where   src17.goal_bbmeas_item_code_score_9 is not null
        union
        select  src18.test_name_cksum as bb_measure_code
            ,src18.goal_bbmeas_item_code_adj_9 as bb_measure_item_code
            ,concat(src18.goal_name_9, ' - Adj - ', src18.term_name) as moniker
            ,case src18.term_name 
                    when 'Fall' then 1702
                    when 'Winter' then 1704
                    when 'Spring' then 1706
             end as sort_order
            ,null as swatch_id
            ,'a' as score_sort_type_code
            ,1 as active_flag
            ,1 as dynamic_creation_flag
            ,1234 as last_user_id
            ,now() as create_timestamp
    
        from    tmp_source_test_term_goal_list as src18
        where   src18.goal_bbmeas_item_code_adj_9 is not null
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
            ,sadmin.student_id
            ,tdc.school_year_id
            ,max(case when sadmin.term_name_season = 'Fall' and mi.sort_order = 101 then ods.test_start_date
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 102 then ods.test_start_date
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 103 then ods.test_start_date
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 201 then ods.growth_measure_flag
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 202 then ods.growth_measure_flag
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 203 then ods.growth_measure_flag
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 301 then ods.test_rit_score
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 302 then ods.test_rit_score
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 303 then ods.test_rit_score
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 401 then ods.test_std_err
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 402 then ods.test_std_err
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 403 then ods.test_std_err
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 501 then ods.test_percentile
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 502 then ods.test_percentile
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 503 then ods.test_percentile
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 601 then ods.rit_reading_score
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 701 then ods.rit_reading_score
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 801 then ods.rit_reading_score
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 602 then ods.rit_reading_min
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 702 then ods.rit_reading_min
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 802 then ods.rit_reading_min
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 603 then ods.rit_reading_max
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 703 then ods.rit_reading_max
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 803 then ods.rit_reading_max
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 901 then ods.goal_rit_score_1
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 902 then ods.goal_adjective_1
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 903 then ods.goal_rit_score_1
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 904 then ods.goal_adjective_1
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 905 then ods.goal_rit_score_1
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 906 then ods.goal_adjective_1
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1001 then ods.goal_rit_score_2
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1002 then ods.goal_adjective_2
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1003 then ods.goal_rit_score_2
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1004 then ods.goal_adjective_2
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1005 then ods.goal_rit_score_2
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1006 then ods.goal_adjective_2
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1101 then ods.goal_rit_score_3
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1102 then ods.goal_adjective_3
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1103 then ods.goal_rit_score_3
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1104 then ods.goal_adjective_3
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1105 then ods.goal_rit_score_3
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1106 then ods.goal_adjective_3
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1201 then ods.goal_rit_score_4
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1202 then ods.goal_adjective_4
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1203 then ods.goal_rit_score_4
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1204 then ods.goal_adjective_4
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1205 then ods.goal_rit_score_4
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1206 then ods.goal_adjective_4
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1301 then ods.goal_rit_score_5
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1302 then ods.goal_adjective_5
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1303 then ods.goal_rit_score_5
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1304 then ods.goal_adjective_5
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1305 then ods.goal_rit_score_5
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1306 then ods.goal_adjective_5
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1401 then ods.goal_rit_score_6
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1402 then ods.goal_adjective_6
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1403 then ods.goal_rit_score_6
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1404 then ods.goal_adjective_6
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1405 then ods.goal_rit_score_6
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1406 then ods.goal_adjective_6
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1501 then ods.goal_rit_score_7
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1502 then ods.goal_adjective_7
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1503 then ods.goal_rit_score_7
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1504 then ods.goal_adjective_7
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1505 then ods.goal_rit_score_7
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1506 then ods.goal_adjective_7
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1601 then ods.goal_rit_score_8
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1602 then ods.goal_adjective_8
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1603 then ods.goal_rit_score_8
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1604 then ods.goal_adjective_8
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1605 then ods.goal_rit_score_8
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1606 then ods.goal_adjective_8
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1701 then ods.goal_rit_score_9
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1702 then ods.goal_adjective_9
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1703 then ods.goal_rit_score_9
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1704 then ods.goal_adjective_9
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1705 then ods.goal_rit_score_9
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1706 then ods.goal_adjective_9
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1801 then ods.percent_correct
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1802 then ods.percent_correct
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1803 then ods.percent_correct
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1901 then ods.percent_correct
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1902 then ods.percent_correct
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1903 then ods.projected_proficiency
                end ) as score

            ,max(case when sadmin.term_name_season = 'Fall' and mi.sort_order = 101 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 102 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 103 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 201 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 202 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 203 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 301 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 302 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 303 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 401 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 402 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 403 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 501 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 502 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 503 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 601 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 701 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 801 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 602 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 702 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 802 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 603 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 703 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 803 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 901 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 902 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 903 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 904 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 905 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 906 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1001 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1002 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1003 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1004 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1005 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1006 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1101 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1102 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1103 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1104 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1105 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1106 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1201 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1202 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1203 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1204 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1205 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1206 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1301 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1302 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1303 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1304 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1305 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1306 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1401 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1402 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1403 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1404 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1405 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1406 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1501 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1502 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1503 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1504 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1505 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1506 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1601 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1602 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1603 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1604 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1605 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1606 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1701 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1702 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1703 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1704 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1705 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1706 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1801 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1802 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1803 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Fall' and mi.sort_order = 1901 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Winter' and mi.sort_order = 1902 then mi.score_sort_type_code
                        when sadmin.term_name_season = 'Spring' and mi.sort_order = 1903 then mi.score_sort_type_code
                end ) as score_type
            , NULL  -- Don't know color yet
            ,1234
            ,now()
        from    v_pmi_ods_nwea as ods
        join    tmp_date_conversion as tdc
                on ods.test_start_date = tdc.test_start_date_str
        join    tmp_stu_admin as sadmin
                on ods.row_num = sadmin.row_num
        join    pm_bbcard_measure as m
                on      m.bb_group_id = v_bb_group_id
                and     m.bb_measure_code = sadmin.test_name_chksum
        join    pm_bbcard_measure_item as mi
                on      m.bb_group_id = mi.bb_group_id
                   and  m.bb_measure_id = mi.bb_measure_id
        group by m.bb_group_id
            ,m.bb_measure_id
            ,mi.bb_measure_item_id
            ,sadmin.student_id
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
                 and mi.bb_measure_item_code in ('growthMeasureFall','growthMeasureWtr','growthMeasureSpr')
        set rpt.score_color = case rpt.score
                                 when 'True'  then 'Green'
                                 when 'Yes'  then 'Green'
                                 when 'Y'  then 'Green'
                                 when 'False' then 'Red'
                                 when 'No' then 'Red'
                                 when 'N' then 'Red'
                                end
        ;

        ##########################
        ## working table cleanup
        ##########################

        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_stu_admin_dups`;
        drop table if exists `tmp_date_conversion`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_id_assign_bb_meas`;
        drop table if exists `tmp_id_assign_bb_meas_item`;
        drop table if exists `tmp_source_test_list`;
        drop table if exists `tmp_source_test_term_goal_list`;
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
