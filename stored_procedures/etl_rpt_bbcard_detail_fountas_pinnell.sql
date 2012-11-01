DROP PROCEDURE IF EXISTS etl_rpt_bbcard_detail_fountas_pinnell //

CREATE DEFINER=`dbadmin`@`localhost` PROCEDURE `etl_rpt_bbcard_detail_fountas_pinnell`()
    SQL SECURITY INVOKER
    COMMENT 'Date: 2012-01-24 etl_rpt_bbcard_detail_fountas_pinnell'
proc: begin

    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_view_exists tinyint(1);
    declare v_bb_group_id int(11);
    declare v_backfill_needed smallint(6);
    declare v_grade_unassigned_id  int(10);
    declare v_school_unassigned_id  int(10);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_fountas_pinnell';
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
        where   bb_group_code = 'fountasPinnell'
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
        

        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_date_conversion`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_xref_table_column_measure_item`;
        

        create table `tmp_stu_admin` (
          `student_id` int(10) NOT NULL,
          `school_year_id` int(10) NOT NULL,
          `test_month` tinyint(2) not null,
          `student_code` varchar(25) default null,
          `grade_code` varchar(15) default null,
          `independent_level` varchar(20) default null,
          `instructional_level` varchar(20) default null,
          `reading_level` varchar(20) default null,
          `backfill_needed_flag` tinyint(1),
          primary key (`student_id`, `school_year_id`,`test_month`)
        ) engine=innodb default charset=latin1
        ;

        
        create table `tmp_date_conversion` (
          `test_year`      int(10) NOT NULL,
          `test_month`     tinyint(2)  NOT NULL,
          `school_year_id`  int(10),  
          primary key (`test_year`,`test_month`)
        ) engine=innodb default charset=latin1
        ;
        
        create table `tmp_student_year_backfill` (
           `student_id` int(10) not null,
           `school_year_id` smallint(4) not null,
           `grade_level_id` int(10) null,
           `school_id` int(10) null,
           primary key  (`student_id`,`school_year_id`)
         ) engine=innodb default charset=latin1
         ;


        CREATE TABLE `tmp_xref_table_column_measure_item` (
          `bb_measure_item_code` varchar(40) NOT NULL,
          `test_month` tinyint(2) default NULL,
          `ods_column` varchar(30) default null
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        ## Date Conversion
        insert tmp_date_conversion (
            test_year 
           ,test_month
        )
        select test_year
              ,test_month
        from  v_pmi_ods_fountas_pinnell
        where test_year is not null and test_month is not null
        group by test_year,test_month
        ;

        update tmp_date_conversion tdc
        join  c_school_year sy
              on    str_to_date(concat(tdc.test_year,'-',tdc.test_month,'-20'), '%Y-%m-%d') between sy.begin_date and sy.end_date
        set   tdc.school_year_id = sy.school_year_id
        ;

        ## Tmp Stu Admin
        insert into tmp_stu_admin (student_id, school_year_id, test_month, student_code, grade_code, independent_level, instructional_level, reading_level, backfill_needed_flag)
        select  s.student_id
                , tdc.school_year_id
                , tdc.test_month
                , ods.student_id as student_code
                , coalesce(ods.grade_code,v_grade_unassigned_id) as grade_code
                , ods.independent_level
                , ods.instructional_level
                , ods.reading_level
                , case when sty.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_fountas_pinnell ods
        join    tmp_date_conversion as tdc
                on    ods.test_year = tdc.test_year
                and   ods.test_month = tdc.test_month
        join    c_student s
                on    ods.student_id = s.student_code
        left join   c_student_year as sty
                    on    sty.student_id = s.student_id
                    and   sty.school_year_id = tdc.school_year_id
        where   ods.independent_level is not null or ods.instructional_level is not null or ods.reading_level is not null
        union all
        select  s.student_id
                , tdc.school_year_id
                , tdc.test_month
                , ods.student_id as student_code
                , coalesce(ods.grade_code,v_grade_unassigned_id) as grade_code
                , ods.independent_level
                , ods.instructional_level
                , ods.reading_level
                , case when sty.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_fountas_pinnell ods
        join    tmp_date_conversion as tdc
                on    ods.test_year = tdc.test_year
                and   ods.test_month = tdc.test_month
        join    c_student s
                on    ods.student_id = s.student_state_code
        left join   c_student_year as sty
                    on    sty.student_id = s.student_id
                    and   sty.school_year_id = tdc.school_year_id
        where   ods.independent_level is not null or ods.instructional_level is not null or ods.reading_level is not null
        union all
        select  s.student_id
                , tdc.school_year_id
                , tdc.test_month
                , ods.student_id as student_code
                , coalesce(ods.grade_code,v_grade_unassigned_id) as grade_code
                , ods.independent_level
                , ods.instructional_level
                , ods.reading_level
                , case when sty.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_fountas_pinnell ods
        join    tmp_date_conversion as tdc
                on    ods.test_year = tdc.test_year
                and   ods.test_month = tdc.test_month
        join    c_student s
                on    ods.student_id = s.fid_code
        left join   c_student_year as sty
                    on    sty.student_id = s.student_id
                    and   sty.school_year_id = tdc.school_year_id
        where   ods.independent_level is not null or ods.instructional_level is not null or ods.reading_level is not null
        on duplicate key update independent_level = values(independent_level)
                                ,instructional_level = values(instructional_level)
                                ,reading_level = values(reading_level)
                                ,grade_code = values(grade_code)
        ;


        select count(*)
        into v_backfill_needed
        from tmp_stu_admin
        where backfill_needed_flag = 1
        ;

        if v_backfill_needed > 0 then

            insert tmp_student_year_backfill (
                   student_id
                  ,school_year_id
                  ,grade_level_id
                  ,school_id
            )
            select   sadmin.student_id
                    ,sadmin.school_year_id
                    ,coalesce(grd.grade_level_id, v_grade_unassigned_id)
                    ,v_school_unassigned_id
            from tmp_stu_admin as sadmin
            left join   v_pmi_xref_grade_level as gxref
                        on  sadmin.grade_code = gxref.client_grade_code
            left join   c_grade_level as grd
                        on  gxref.pmi_grade_code = grd.grade_code
            where sadmin.backfill_needed_flag = 1
            on duplicate key update grade_level_id = values(grade_level_id)
                ,school_id = values(school_id)
            ;

            call etl_hst_load_backfill_stu_year();

        end if;

        INSERT INTO tmp_xref_table_column_measure_item(bb_measure_item_code,test_month,ods_column)
        select mi.bb_measure_item_code
            ,cast(trim(substring_index(mi.bb_measure_item_code,'_',-1)) as signed) as test_month
            ,case when mi.bb_measure_item_code like 'fAndPInstructional%' then 'instructional_level'
                    when mi.bb_measure_item_code like 'fAndPIndependent%' then 'independent_level'
                    when mi.bb_measure_item_code like 'fAndPReading%' then 'reading_level'
            else NULL
            end ods_column
        from    pm_bbcard_measure_item mi
        join    pm_bbcard_measure m
                on    mi.bb_measure_id = m.bb_measure_id
        join    pm_bbcard_group g
                on    m.bb_group_id = g.bb_group_id
        where   g.bb_group_id = v_bb_group_id
          and   m.bb_measure_code like '%fAndP%'
        ;


        INSERT INTO rpt_bbcard_detail_fountas_pinnell(bb_group_id,bb_measure_id,bb_measure_item_id,student_id,school_year_id,score,score_type,score_color,last_user_id,create_timestamp)
        
        
        
        
        select dt.bb_group_id
            ,dt.bb_measure_id
            ,dt.bb_measure_item_id
            ,dt.student_id
            ,dt.school_year_id
            ,dt.score
            ,dt.score_sort_type_code
            ,dt.score_color
            ,dt.last_user_id
            ,dt.create_timestamp
        from (
              select m.bb_group_id
                    ,m.bb_measure_id
                    ,mi.bb_measure_item_id
                    ,sa.student_id
                    ,sa.school_year_id
                    ,case when x.ods_column = 'instructional_level' then sa.instructional_level
                        when x.ods_column = 'independent_level' then sa.independent_level
                        when x.ods_column = 'reading_level' then sa.reading_level
                    end as score
                    ,mi.score_sort_type_code
                    ,null as score_color
                    ,1234 last_user_id
                    ,now() create_timestamp
              from  tmp_stu_admin sa
              join  pm_bbcard_measure m
                    on    m.bb_measure_code like '%fAndP%'
              join  pm_bbcard_measure_item mi
                    on    m.bb_measure_id = mi.bb_measure_id
              join tmp_xref_table_column_measure_item x
                    on    mi.bb_measure_item_code = x.bb_measure_item_code
                    and   sa.test_month = x.test_month
             ) dt
        where dt.score is not null
        on duplicate key update last_user_id = values(last_user_id)
        ;

        # Update Color
        update rpt_bbcard_detail_fountas_pinnell as rpt
        join    c_student_year sy
                on    rpt.student_id = sy.student_id
                and   rpt.school_year_id = sy.school_year_id
        join    c_grade_level gl
                on    sy.grade_level_id = gl.grade_level_id
        join    pm_color_fountas_pinnell as cs
                on    rpt.bb_group_id = cs.bb_group_id
                and   rpt.bb_measure_id = cs.bb_measure_id
                and   rpt.bb_measure_item_id = cs.bb_measure_item_id 
                and   rpt.score = cs.reading_level
                and   rpt.school_year_id BETWEEN cs.begin_year AND cs.end_year
                and   gl.grade_sequence between cs.begin_grade_sequence and cs.end_grade_sequence
        join    pmi_color as c
                on    c.color_id = cs.color_id
        set rpt.score_color = c.moniker
        ;
        
        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_date_conversion`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_xref_table_column_measure_item`;
  
        set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', v_ods_table, '\', \'P\', \'ETL Load Successful\')');

        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;
        
    end if;

end proc//
