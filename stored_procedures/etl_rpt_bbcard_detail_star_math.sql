DROP PROCEDURE IF EXISTS etl_rpt_bbcard_detail_star_math //

CREATE DEFINER=`dbadmin`@`localhost` PROCEDURE `etl_rpt_bbcard_detail_star_math`()
    SQL SECURITY INVOKER
    COMMENT 'Date: 2012-01-24 etl_rpt_bbcard_detail_star'
proc: begin

    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_view_exists tinyint(1);
    declare v_bb_group_id int(11);
    declare v_backfill_needed smallint(6);
    declare v_date_format_mask varchar(15) default '%m/%d/%Y';  
    declare v_grade_unassigned_id  int(10);
    declare v_school_unassigned_id  int(10);
    declare v_test_code varchar(75) default 'starMath';

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_star_math';
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
        where   bb_group_code = 'star'
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
        
        set @star_date_format_mask := pmi_f_get_etl_setting('starDateFormatMask');
    
        if @star_date_format_mask is not null then
            set v_date_format_mask = @star_date_format_mask;
        end if;

        drop table if exists `tmp_stu_admin`;
        #drop table if exists `tmp_stu_admin_score`;
        drop table if exists `tmp_date_conversion`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_xref_table_column_measure_item`;
        

        create table `tmp_stu_admin` (
          `student_id` int(10) NOT NULL,
          `school_year_id` int(10) NOT NULL,
          `term_name_season` varchar(20) not null,
          `ss` varchar(20) not null,
          `test_code` varchar(75) NOT NULL,
          `student_code` varchar(25) default null,
          `assessment_grade` varchar(4) default null,
          `date_taken_str`  varchar(20) default NULL,
          `gp` varchar(20) default null,
          `ge` varchar(20) default null,
          `pr` varchar(20) default null,
          `nce` varchar(20) default null,
          `backfill_needed_flag` tinyint(1),
          primary key (`student_id`, `school_year_id`,`term_name_season`)
        ) engine=innodb default charset=latin1
                ;

        
        create table `tmp_date_conversion` (
          `date_taken`      date NOT NULL,
          `date_taken_str`  varchar(20) NOT NULL,
          `school_year_id`  int unsigned,
          `administration`  enum('Fall','Winter','Spring') not null,  
          primary key (`date_taken`),
          index (`school_year_id`,`date_taken_str`)
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
          `term_name_season` varchar(120) default NULL,
          `ods_column` varchar(3) character set utf8 default NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        ## Date Conversion
        insert tmp_date_conversion (
            date_taken 
           ,date_taken_str
           ,administration
        )
        select distinct
            str_to_date(date_taken, v_date_format_mask)
           ,date_taken
           ,case when month(str_to_date(date_taken, v_date_format_mask)) in (8,9,10,11) then 'Fall'
                when month(str_to_date(date_taken, v_date_format_mask)) in (12,1,2,3) then 'Winter'
                when month(str_to_date(date_taken, v_date_format_mask)) in (4,5,6,7) then 'Spring'
            end
        from v_pmi_ods_star_math ods;

        update tmp_date_conversion tdc
        join c_school_year sy
           on tdc.date_taken between sy.begin_date and sy.end_date
        set tdc.school_year_id = sy.school_year_id;

        ## Tmp Stu Admin
        insert into tmp_stu_admin (student_id, school_year_id, term_name_season, ss, test_code, student_code, assessment_grade, date_taken_str, gp, ge, pr, nce, backfill_needed_flag)
        select  s.student_id
                , tdc.school_year_id
                , tdc.administration as term_name_season
                , ods.SS
                , v_test_code as test_code
                , ods.student_id as student_code
                , coalesce(ods.assessment_grade,v_grade_unassigned_id) as assessment_grade
                , date_format(tdc.date_taken, v_date_format_mask) as date_taken_str
                , ods.GP
                , ods.GE
                , ods.PR
                , ods.nce
                , case when sty.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_star_math ods
        join    tmp_date_conversion as tdc
                on    ods.date_taken = tdc.date_taken_str
        join    c_student s
                on    ods.student_id = s.student_code
        left join   c_student_year as sty
                    on    sty.student_id = s.student_id
                    and   sty.school_year_id = tdc.school_year_id
        where   ods.ss is not null
        union all
        select  s2.student_id
                , tdc2.school_year_id
                , tdc2.administration as term_name_season
                , ods2.SS
                ,v_test_code as test_code
                , ods2.student_id as student_code
                , coalesce(ods2.assessment_grade,v_grade_unassigned_id) as assessment_grade
                , date_format(tdc2.date_taken, v_date_format_mask) as date_taken_str
                , ods2.GP
                , ods2.GE
                , ods2.PR
                , ods2.nce
                , case when sty2.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_star_math ods2
        join    tmp_date_conversion as tdc2
                on    ods2.date_taken = tdc2.date_taken_str
        join    c_student s2
                on    ods2.student_id = s2.student_state_code
        left join   c_student_year as sty2
                    on    sty2.student_id = s2.student_id
                    and   sty2.school_year_id = tdc2.school_year_id
        where   ods2.ss is not null
        union all
        select  s3.student_id
                , tdc3.school_year_id
                , tdc3.administration as term_name_season
                , ods3.SS
                ,v_test_code as test_code
                , ods3.student_id as student_code
                , coalesce(ods3.assessment_grade,v_grade_unassigned_id) as assessment_grade
                , date_format(tdc3.date_taken, v_date_format_mask) as date_taken_str
                , ods3.GP
                , ods3.GE
                , ods3.PR
                , ods3.nce
                , case when sty3.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_star_math ods3
        join    tmp_date_conversion as tdc3
                on    ods3.date_taken = tdc3.date_taken_str
        join    c_student s3
                on    ods3.student_id = s3.fid_code 
        left join   c_student_year as sty3
                    on    sty3.student_id = s3.student_id
                    and   sty3.school_year_id = tdc3.school_year_id
        where   ods3.ss is not null
        order by 1,2,3,4
        on duplicate key update ss = values(ss)
                                ,gp = values(gp)
                                ,ge = values(ge)
                                ,pr = values(pr)
                                ,nce = values(nce)
                                ,assessment_grade = values(assessment_grade)
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
                        on  sadmin.assessment_grade = gxref.client_grade_code
            left join   c_grade_level as grd
                        on  gxref.pmi_grade_code = grd.grade_code
            where sadmin.backfill_needed_flag = 1
            on duplicate key update grade_level_id = values(grade_level_id)
                ,school_id = values(school_id)
            ;

            call etl_hst_load_backfill_stu_year();

        end if;

        INSERT INTO tmp_xref_table_column_measure_item(bb_measure_item_code,term_name_season,ods_column)
        select mi.bb_measure_item_code
            ,trim(substring_index(mi.moniker,'-',-1)) term_name_season
            ,case when mi.bb_measure_item_code like 'starMathGradePlace%' then 'GP'
                    when mi.bb_measure_item_code like 'starMathScaleScore%' then 'SS'
                    when mi.bb_measure_item_code like 'starMathGradeEquiv%' then 'GE'
                    when mi.bb_measure_item_code like 'starMathPercentRank%' then 'PR'
                    when mi.bb_measure_item_code like 'starMathNormCrvEqv%' then 'NCE'
                    when mi.bb_measure_item_code like 'starMathInstructLvl%' then 'IML'
            else NULL
            end ods_column
        from pm_bbcard_measure_item mi
            inner join pm_bbcard_measure m
                on mi.bb_measure_id = m.bb_measure_id
            inner join pm_bbcard_group g
                on m.bb_group_id = g.bb_group_id
        where g.bb_group_code = 'star'
            and m.bb_measure_code = v_test_code
        ;


        INSERT INTO rpt_bbcard_detail_star(bb_group_id,bb_measure_id,bb_measure_item_id,student_id,school_year_id,score,score_type,score_color,last_user_id,create_timestamp)
        
        
        
        
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
                    ,case when x.ods_column = 'SS' then sa.SS
                        when x.ods_column = 'GE' then sa.GE
                        when x.ods_column = 'PR' then sa.PR
                        when x.ods_column = 'GP' then sa.GP
                        when x.ods_column = 'NCE' then sa.NCE
                    end as score
                    ,mi.score_sort_type_code
                    ,null as score_color
                    ,1234 last_user_id
                    ,now() create_timestamp
              from  tmp_stu_admin sa
              join  pm_bbcard_measure m
                    on    sa.test_code = m.bb_measure_code
              join  pm_bbcard_measure_item mi
                    on    m.bb_measure_id = mi.bb_measure_id
              join tmp_xref_table_column_measure_item x
                    on    mi.bb_measure_item_code = x.bb_measure_item_code
                    and   sa.term_name_season = x.term_name_season
             ) dt
        where dt.score is not null
        on duplicate key update last_user_id = values(last_user_id), last_edit_timestamp = now()
        ;

        # Update Color
        update rpt_bbcard_detail_star as rpt
        join    c_student_year sy
                on    rpt.student_id = sy.student_id
                and   rpt.school_year_id = sy.school_year_id
        join    c_grade_level gl
                on    sy.grade_level_id = gl.grade_level_id
        join    pm_color_star as cs
                on    rpt.bb_group_id = cs.bb_group_id
                and   rpt.bb_measure_id = cs.bb_measure_id
                and   rpt.bb_measure_item_id = cs.bb_measure_item_id 
                and   rpt.score between cs.min_score and cs.max_score
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
