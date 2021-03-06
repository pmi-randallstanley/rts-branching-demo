drop procedure if exists etl_hst_load_fl_faa//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_fl_faa()
contains sql
sql security invoker
comment '$Rev:  $'

proc: begin 
  
    Declare v_no_more_rows boolean;
    Declare var_ayp_subject_id int(11);
    Declare var_ayp_strand_id int(11);
    Declare var_column_moniker varchar(25);
    Declare v_ods_table varchar(40);
    Declare v_ods_view varchar(40);
    Declare v_view_exists smallint(3);
    Declare v_school_unassigned_id  int(10);
    Declare v_grade_unassigned_id int(10);
    Declare v_backfill_needed int(10);
    Declare v_delete_count int(10);
    Declare v_ayp_subject_id_faa_math  int(11);
    Declare v_ayp_subject_id_faa_reading  int(11);
    Declare v_ayp_subject_id_faa_science  int(11);
    Declare v_ayp_subject_id_faa_writing  int(11);
 
            
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_fl_faa';
    set v_ods_view = 'v_pmi_ods_fl_faa';

    select school_id
    into v_school_unassigned_id 
    from c_school
    where school_code = 'unassigned';


    select grade_level_id
    into v_grade_unassigned_id
    from c_grade_level
    where grade_code = 'unassigned';
    
    
    select  count(*) 
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;

    if v_view_exists > 0 then
   
        #########################
        ## Load Working Tables ##
        #########################
        
        drop table if exists `tmp_subject_list`;
        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_date_conversion`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_school`;

        

        CREATE TABLE `tmp_date_conversion` (
          `test_mo_year` varchar(10) not null,
          `test_month` tinyint(2) not null,
          `test_date` date not null,
          UNIQUE KEY `uq_tmp_date_conversion` (`test_mo_year`)
        ) engine=innodb default charset=latin1
        ;

        CREATE TABLE `tmp_subject_list` (
          `ayp_subject_id` int(10) NOT NULL,
          `client_ayp_subject_code` varchar(2) NOT NULL,
          `ayp_subject_code` varchar(50) NOT NULL,
          PRIMARY KEY  (`ayp_subject_id`),
          UNIQUE KEY `uq_tmp_subject_list` (`client_ayp_subject_code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        create table `tmp_school` (
            `school_code` varchar(15) not null,
            `school_id` int (10) not null,
        Unique Key `ind_school_code` (`school_code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        CREATE TABLE `tmp_stu_admin` (
           `student_code` varchar(15) NOT NULL,
           `client_ayp_subject_code` varchar(20) NOT NULL,
           `ods_test_date` varchar(10) NOT NULL,
           `row_num` int(10) NOT NULL,
           `student_id` int(10) NOT NULL,
           `ayp_subject_id` int(10) NOT NULL,
           `test_month` tinyint(2) NOT NULL,
           `school_year_id` smallint(4) NOT NULL,
           `grade_code` varchar(15) default null,
           `school_code` varchar(15) default null,
           `backfill_needed_flag` tinyint(1),
         UNIQUE KEY `uq_tmp_stu_admin` (`student_code`,`client_ayp_subject_code`, `ods_test_date`),
         KEY `ind_tmp_stu_admin_stu` (`student_id`,`ayp_subject_id`,`school_year_id`,`test_month`)
         ) ENGINE=InnoDB DEFAULT CHARSET=latin1
         ;

         CREATE TABLE `tmp_student_year_backfill` (
           `ods_row_num` int(10) NOT NULL,
           `student_id` int(10) NOT NULL,
           `school_year_id` smallint(4) NOT NULL,
           `grade_level_id` int(10) null,
           `school_id` int(10) null,
           PRIMARY KEY  (`ods_row_num`),
           UNIQUE KEY `uq_tmp_student_year_backfill` (`student_id`, `school_year_id`)
         ) ENGINE=InnoDB DEFAULT CHARSET=latin1
         ;
        
        ## Populate tmp_test_date        
        
        insert into tmp_date_conversion (
            test_mo_year
            ,test_month
            ,test_date
        )

        select  ods.test_mo_year
            ,min(month(str_to_date(ods.test_mo_year, '%m%Y'))) as test_month
            ,min(str_to_date(concat('15', ods.test_mo_year), '%d%m%Y')) as test_date
        from    v_pmi_ods_fl_faa as ods
        group by    ods.test_mo_year
        ;
        
        
        ## Get Subject Variables
        select  min(case when ayp_subject_code = 'faaMath' then ayp_subject_id end)
               ,min(case when ayp_subject_code = 'faaReading' then ayp_subject_id end)
               ,min(case when ayp_subject_code = 'faaScience' then ayp_subject_id end)
               ,min(case when ayp_subject_code = 'faaWriting' then ayp_subject_id end)
        into    v_ayp_subject_id_faa_math
               ,v_ayp_subject_id_faa_reading
               ,v_ayp_subject_id_faa_science
               ,v_ayp_subject_id_faa_writing

        from    c_ayp_subject as sub
        where   ayp_subject_code in ('faaMath','faaReading','faaScience','faaWriting')
        ;
        


        insert tmp_school (
            school_id
            ,school_code
        )
        select school_id
                ,school_code
        from c_school
        where school_code is not null      
        union
        select school_id
               ,school_state_code
        from c_school
        where school_state_code is not null
        ;
        
        
        ### FAA Math
        insert  tmp_stu_admin (
            row_num
            ,student_code
            ,client_ayp_subject_code
            ,ods_test_date
            ,student_id
            ,ayp_subject_id
            ,test_month
            ,school_year_id
            ,grade_code
            ,school_code
            ,backfill_needed_flag
        )
        select  ods.row_num
            ,ods.student_id
            ,'math'
            ,ods.test_mo_year
            ,s.student_id
            ,v_ayp_subject_id_faa_math
            ,dte.test_month
            ,sty.school_year_id
            ,ods.grade_code
            ,ods.school_id
            ,case when syr.school_year_id is null then 1 
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_fl_faa as ods
        join    tmp_date_conversion as dte
                on      ods.test_mo_year = dte.test_mo_year
        join    c_student as s
                on      s.student_code = ods.student_id
        join    c_school_year as sty
                on     dte.test_date between sty.begin_date and sty.end_date
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sty.school_year_id
        where ods.math_ss REGEXP '^[0-9]' > 0
        union all
        select  ods.row_num
            ,ods.student_id
            ,'math'
            ,ods.test_mo_year
            ,s.student_id
            ,v_ayp_subject_id_faa_math
            ,dte.test_month
            ,sty.school_year_id
            ,ods.grade_code
            ,ods.school_id
            ,case when syr.school_year_id is null then 1
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_fl_faa as ods
        join    tmp_date_conversion as dte
                on      ods.test_mo_year = dte.test_mo_year
        join    c_student as s
                on      s.student_state_code = ods.student_id
        join    c_school_year as sty
                on     dte.test_date between sty.begin_date and sty.end_date
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sty.school_year_id
        where ods.math_ss REGEXP '^[0-9]' > 0
        union all
        select  ods.row_num
            ,ods.student_id
            ,'math'
            ,ods.test_mo_year
            ,s.student_id
            ,v_ayp_subject_id_faa_math
            ,dte.test_month
            ,sty.school_year_id
            ,ods.grade_code
            ,ods.school_id
            ,case when syr.school_year_id is null then 1
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_fl_faa as ods
        join    tmp_date_conversion as dte
                on      ods.test_mo_year = dte.test_mo_year
        join    c_student as s
                on      s.fid_code = ods.student_id
        join    c_school_year as sty
                on     dte.test_date between sty.begin_date and sty.end_date
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sty.school_year_id
        where ods.math_ss REGEXP '^[0-9]' > 0
        on duplicate key update row_num = values(row_num)
        ;
        
        
        ### FAA Reading
        insert  tmp_stu_admin (
            row_num
            ,student_code
            ,client_ayp_subject_code
            ,ods_test_date
            ,student_id
            ,ayp_subject_id
            ,test_month
            ,school_year_id
            ,grade_code
            ,school_code
            ,backfill_needed_flag
        )
        select  ods.row_num
            ,ods.student_id
            ,'reading'
            ,ods.test_mo_year
            ,s.student_id
            ,v_ayp_subject_id_faa_reading
            ,dte.test_month
            ,sty.school_year_id
            ,ods.grade_code
            ,ods.school_id
            ,case when syr.school_year_id is null then 1 
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_fl_faa as ods
        join    tmp_date_conversion as dte
                on      ods.test_mo_year = dte.test_mo_year
        join    c_student as s
                on      s.student_code = ods.student_id
        join    c_school_year as sty
                on     dte.test_date between sty.begin_date and sty.end_date
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sty.school_year_id
        where ods.reading_ss REGEXP '^[0-9]' > 0
        union all
        select  ods.row_num
            ,ods.student_id
            ,'reading'
            ,ods.test_mo_year
            ,s.student_id
            ,v_ayp_subject_id_faa_reading
            ,dte.test_month
            ,sty.school_year_id
            ,ods.grade_code
            ,ods.school_id
            ,case when syr.school_year_id is null then 1
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_fl_faa as ods
        join    tmp_date_conversion as dte
                on      ods.test_mo_year = dte.test_mo_year
        join    c_student as s
                on      s.student_state_code = ods.student_id
        join    c_school_year as sty
                on     dte.test_date between sty.begin_date and sty.end_date
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sty.school_year_id
        where ods.reading_ss REGEXP '^[0-9]' > 0
        union all
        select  ods.row_num
            ,ods.student_id
            ,'reading'
            ,ods.test_mo_year
            ,s.student_id
            ,v_ayp_subject_id_faa_reading
            ,dte.test_month
            ,sty.school_year_id
            ,ods.grade_code
            ,ods.school_id
            ,case when syr.school_year_id is null then 1
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_fl_faa as ods
        join    tmp_date_conversion as dte
                on      ods.test_mo_year = dte.test_mo_year
        join    c_student as s
                on      s.fid_code = ods.student_id
        join    c_school_year as sty
                on     dte.test_date between sty.begin_date and sty.end_date
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sty.school_year_id
        where ods.reading_ss REGEXP '^[0-9]' > 0
        on duplicate key update row_num = values(row_num)
        ;
        
        ### FAA Science
        insert  tmp_stu_admin (
            row_num
            ,student_code
            ,client_ayp_subject_code
            ,ods_test_date
            ,student_id
            ,ayp_subject_id
            ,test_month
            ,school_year_id
            ,grade_code
            ,school_code
            ,backfill_needed_flag
        )
        select  ods.row_num
            ,ods.student_id
            ,'science'
            ,ods.test_mo_year
            ,s.student_id
            ,v_ayp_subject_id_faa_science
            ,dte.test_month
            ,sty.school_year_id
            ,ods.grade_code
            ,ods.school_id
            ,case when syr.school_year_id is null then 1 
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_fl_faa as ods
        join    tmp_date_conversion as dte
                on      ods.test_mo_year = dte.test_mo_year
        join    c_student as s
                on      s.student_code = ods.student_id
        join    c_school_year as sty
                on     dte.test_date between sty.begin_date and sty.end_date
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sty.school_year_id
        where ods.science_ss REGEXP '^[0-9]' > 0
        union all
        select  ods.row_num
            ,ods.student_id
            ,'science'
            ,ods.test_mo_year
            ,s.student_id
            ,v_ayp_subject_id_faa_science
            ,dte.test_month
            ,sty.school_year_id
            ,ods.grade_code
            ,ods.school_id
            ,case when syr.school_year_id is null then 1
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_fl_faa as ods
        join    tmp_date_conversion as dte
                on      ods.test_mo_year = dte.test_mo_year
        join    c_student as s
                on      s.student_state_code = ods.student_id
        join    c_school_year as sty
                on     dte.test_date between sty.begin_date and sty.end_date
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sty.school_year_id
        where ods.science_ss REGEXP '^[0-9]' > 0
        union all
        select  ods.row_num
            ,ods.student_id
            ,'science'
            ,ods.test_mo_year
            ,s.student_id
            ,v_ayp_subject_id_faa_science
            ,dte.test_month
            ,sty.school_year_id
            ,ods.grade_code
            ,ods.school_id
            ,case when syr.school_year_id is null then 1
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_fl_faa as ods
        join    tmp_date_conversion as dte
                on      ods.test_mo_year = dte.test_mo_year
        join    c_student as s
                on      s.fid_code = ods.student_id
        join    c_school_year as sty
                on     dte.test_date between sty.begin_date and sty.end_date
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sty.school_year_id
        where ods.science_ss REGEXP '^[0-9]' > 0
        on duplicate key update row_num = values(row_num)
        ;
        
        
        ### FAA Writing
        insert  tmp_stu_admin (
            row_num
            ,student_code
            ,client_ayp_subject_code
            ,ods_test_date
            ,student_id
            ,ayp_subject_id
            ,test_month
            ,school_year_id
            ,grade_code
            ,school_code
            ,backfill_needed_flag
        )
        select  ods.row_num
            ,ods.student_id
            ,'writing'
            ,ods.test_mo_year
            ,s.student_id
            ,v_ayp_subject_id_faa_writing
            ,dte.test_month
            ,sty.school_year_id
            ,ods.grade_code
            ,ods.school_id
            ,case when syr.school_year_id is null then 1 
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_fl_faa as ods
        join    tmp_date_conversion as dte
                on      ods.test_mo_year = dte.test_mo_year
        join    c_student as s
                on      s.student_code = ods.student_id
        join    c_school_year as sty
                on     dte.test_date between sty.begin_date and sty.end_date
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sty.school_year_id
        where ods.writing_ss REGEXP '^[0-9]' > 0
        union all
        select  ods.row_num
            ,ods.student_id
            ,'writing'
            ,ods.test_mo_year
            ,s.student_id
            ,v_ayp_subject_id_faa_writing
            ,dte.test_month
            ,sty.school_year_id
            ,ods.grade_code
            ,ods.school_id
            ,case when syr.school_year_id is null then 1
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_fl_faa as ods
        join    tmp_date_conversion as dte
                on      ods.test_mo_year = dte.test_mo_year
        join    c_student as s
                on      s.student_state_code = ods.student_id
        join    c_school_year as sty
                on     dte.test_date between sty.begin_date and sty.end_date
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sty.school_year_id
        where ods.writing_ss REGEXP '^[0-9]' > 0
        union all
        select  ods.row_num
            ,ods.student_id
            ,'writing'
            ,ods.test_mo_year
            ,s.student_id
            ,v_ayp_subject_id_faa_writing
            ,dte.test_month
            ,sty.school_year_id
            ,ods.grade_code
            ,ods.school_id
            ,case when syr.school_year_id is null then 1
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_fl_faa as ods
        join    tmp_date_conversion as dte
                on      ods.test_mo_year = dte.test_mo_year
        join    c_student as s
                on      s.fid_code = ods.student_id
        join    c_school_year as sty
                on     dte.test_date between sty.begin_date and sty.end_date
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sty.school_year_id
        where ods.writing_ss REGEXP '^[0-9]' > 0
        on duplicate key update row_num = values(row_num)
        ;

        ##########################################
        ## Backfill for c_student_year 
        ## Need to detect and load c_student_year 
        ## records when supporting ones don't exist
        ##############################################

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
            select  sadmin.row_num
                    ,sadmin.student_id
                    ,sadmin.school_year_id
                  ,coalesce(grd.grade_level_id, v_grade_unassigned_id)
                  ,coalesce(sch.school_id, v_school_unassigned_id)
            from tmp_stu_admin as sadmin
            left join v_pmi_xref_grade_level as gxref
                    on sadmin.grade_code = gxref.client_grade_code
            left join c_grade_level as grd
                    on gxref.pmi_grade_code = grd.grade_code
            left join tmp_school as sch
                    on sadmin.school_code = sch.school_code
            where sadmin.backfill_needed_flag = 1
            on duplicate key update grade_level_id = values(grade_level_id)
                ,school_id = values(school_id)
            ;

            ##########################################
            ## proc developed to standardize loading
            ## c_student_year
            ############################################

            call etl_hst_load_backfill_stu_year();
        
        end if;
        
        #######################
        ## Load Subject Data ##
        #######################
        ## Math
        insert c_ayp_subject_student (
            student_id
            ,ayp_subject_id
            ,school_year_id
            ,month_id
            ,alt_ayp_score
            ,score_type_code
            ,ayp_score
            ,last_user_id
            ,create_timestamp
        )
        select  tsa.student_id
            ,tsa.ayp_subject_id
            ,tsa.school_year_id
            ,tsa.test_month
            ,ods.math_ss
            ,'n'
            ,ods.math_pl
            ,1234
            ,now()
        from    v_pmi_ods_fl_faa as ods
        join    tmp_stu_admin as tsa
                on      tsa.row_num = ods.row_num
        join    c_ayp_subject sub
                on      tsa.ayp_subject_id = sub.ayp_subject_id
                and     sub.ayp_subject_code = 'faaMath'
        on duplicate key update last_user_id = values(last_user_id)
            ,ayp_score = values(ayp_score)
        ;
        
        ## Reading
        insert c_ayp_subject_student (
            student_id
            ,ayp_subject_id
            ,school_year_id
            ,month_id
            ,alt_ayp_score
            ,score_type_code
            ,ayp_score
            ,last_user_id
            ,create_timestamp
        )
        select  tsa.student_id
            ,tsa.ayp_subject_id
            ,tsa.school_year_id
            ,tsa.test_month
            ,ods.reading_ss
            ,'n'
            ,ods.reading_pl
            ,1234
            ,now()
        from    v_pmi_ods_fl_faa as ods
        join    tmp_stu_admin as tsa
                on      tsa.row_num = ods.row_num
        join    c_ayp_subject sub
                on      tsa.ayp_subject_id = sub.ayp_subject_id
                and     sub.ayp_subject_code = 'faaReading'
        on duplicate key update last_user_id = values(last_user_id)
            ,ayp_score = values(ayp_score)
        ;
        
        ## Science
        insert c_ayp_subject_student (
            student_id
            ,ayp_subject_id
            ,school_year_id
            ,month_id
            ,alt_ayp_score
            ,score_type_code
            ,ayp_score
            ,last_user_id
            ,create_timestamp
        )
        select  tsa.student_id
            ,tsa.ayp_subject_id
            ,tsa.school_year_id
            ,tsa.test_month
            ,ods.science_ss
            ,'n'
            ,ods.science_pl
            ,1234
            ,now()
        from    v_pmi_ods_fl_faa as ods
        join    tmp_stu_admin as tsa
                on      tsa.row_num = ods.row_num
        join    c_ayp_subject sub
                on      tsa.ayp_subject_id = sub.ayp_subject_id
                and     sub.ayp_subject_code = 'faaScience'
        on duplicate key update last_user_id = values(last_user_id)
            ,ayp_score = values(ayp_score)
        ;
        
        ## Writing
        insert c_ayp_subject_student (
            student_id
            ,ayp_subject_id
            ,school_year_id
            ,month_id
            ,alt_ayp_score
            ,score_type_code
            ,ayp_score
            ,last_user_id
            ,create_timestamp
        )
        select  tsa.student_id
            ,tsa.ayp_subject_id
            ,tsa.school_year_id
            ,tsa.test_month
            ,ods.writing_ss
            ,'n'
            ,ods.writing_pl
            ,1234
            ,now()
        from    v_pmi_ods_fl_faa as ods
        join    tmp_stu_admin as tsa
                on      tsa.row_num = ods.row_num
        join    c_ayp_subject sub
                on      tsa.ayp_subject_id = sub.ayp_subject_id
                and     sub.ayp_subject_code = 'faaWriting'
        on duplicate key update last_user_id = values(last_user_id)
            ,ayp_score = values(ayp_score)
        ;
 

        #######################################################################################
        ## Delete records that exist in source with 0 month_id (single admin) 
        ## based on student, school_year and subject
        #######################################################################################

        select  count(*)
        into    v_delete_count
        from    tmp_stu_admin as tmp1
        join    c_ayp_subject_student as ss
                on      tmp1.student_id = ss.student_id
                and     tmp1.ayp_subject_id = ss.ayp_subject_id
                and     tmp1.school_year_id = ss.school_year_id
                and     ss.month_id = 0
        where   tmp1.test_month != 0
        ;
        
        if v_delete_count > 0 then

            delete ayp_str.*
            from tmp_stu_admin as tmp1
            join    c_ayp_subject_student as ss
                    on      tmp1.student_id = ss.student_id
                    and     tmp1.ayp_subject_id = ss.ayp_subject_id
                    and     tmp1.school_year_id = ss.school_year_id
                    and     ss.month_id = 0
            join c_ayp_strand_student as ayp_str
                    on      ayp_str.student_id = ss.student_id
                    and     ayp_str.ayp_subject_id = ss.ayp_subject_id
                    and     ayp_str.school_year_id = ss.school_year_id
                    and     ayp_str.month_id = ss.month_id
             ;

            delete  ss.*
            from    tmp_stu_admin as tmp1
            join    c_ayp_subject_student as ss
                    on      ss.student_id = tmp1.student_id
                    and     ss.ayp_subject_id = tmp1.ayp_subject_id
                    and     ss.school_year_id = tmp1.school_year_id
                    and     ss.month_id = 0
            ;
             
        end if; 
        
    #########################
    ## clean-up tmp tables ##
    #########################
    
        drop table if exists `tmp_subject_list`;

        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_date_conversion`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_school`;

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
