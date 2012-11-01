drop procedure if exists etl_hst_load_co_tcap//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_co_tcap()
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
    
    Declare v_ayp_subject_id int(11);
    Declare v_ayp_strand_id int(11);
    declare v_ayp_subject_code varchar(40);
    declare v_ayp_strand_code varchar(40);
    declare v_begin_school_year_id int(11);
    declare v_end_school_year_id int(11);
    declare v_begin_grade_sequence varchar(2);
    declare v_end_grade_sequence varchar(2);
    declare v_str_score_col varchar(50);
 
    declare v_strand_cursor cursor for
        select ayp_subject_id
              ,ayp_subject_code
              ,ayp_strand_id
              ,ayp_strand_code
              ,begin_school_year_id
              ,end_school_year_id
              ,begin_grade_sequence
              ,end_grade_sequence
              ,str_score_col
        from tmp_strand_score_moniker;

    declare continue handler for not found 
    set v_no_more_rows = true;
            
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_co_tcap';
    set v_ods_view = 'v_pmi_ods_co_tcap';

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
        drop table if exists `tmp_test_date`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_school`;
        drop table if exists `tmp_strand_score_moniker`;

        
        CREATE TABLE `tmp_test_date` (
          `ods_test_date` varchar(8) NOT NULL,
          `test_date` date NOT NULL,
          PRIMARY KEY  (`ods_test_date`),
          UNIQUE KEY `tmp_test_year_date` (`test_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_subject_list` (
          `ayp_subject_id` int(10) NOT NULL,
          `client_ayp_subject_code` varchar(5) NOT NULL,
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
           `client_ayp_subject_code` varchar(5) NOT NULL,
           `ods_test_date` varchar(10) NOT NULL,
           `row_num` int(10) NOT NULL,
           `student_id` int(10) NOT NULL,
           `ayp_subject_id` int(10) NOT NULL,
           `test_month` tinyint(2) NOT NULL,
           `school_year_id` smallint(4) NOT NULL,
           `grade_code` varchar(15) default null,
           `school_code` varchar(15) default null,
           `backfill_needed_flag` tinyint(1),
         Primary KEY (`row_num`),
         UNIQUE KEY `uq_tmp_stu_admin` (`student_code`, `client_ayp_subject_code`,`ods_test_date`),
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
         
         create table `tmp_strand_score_moniker` (
            `ayp_subject_id` int(10) not null,
            `ayp_subject_code` varchar(40) not null,
            `ayp_strand_id` int(10) not null,
            `ayp_strand_code` varchar(40) not null,
            `begin_school_year_id` int(11) not null,
            `end_school_year_id` int(11) not null,
            `begin_grade_sequence` varchar(2) not null,
            `end_grade_sequence` varchar(2) not null,
            `str_score_col` varchar(50) not null,
            PRIMARY KEY  (`ayp_subject_id`,`ayp_strand_id`,`begin_school_year_id`,`end_school_year_id`,`begin_grade_sequence`,`end_grade_sequence`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        ## Populate tmp_test_date        
        insert into tmp_test_date (
                    ods_test_date
                    ,test_date
        )

        select      test_date
                  ,str_to_date(concat(left(ods.test_date, 2),substring(ods.test_date,3,2),right(ods.test_date, 2)), '%m%d%y')
        from v_pmi_ods_co_tcap ods
        group by    test_date
                   ,str_to_date(concat(left(ods.test_date, 2),substring(ods.test_date,3,2),right(ods.test_date, 2)), '%m%d%y')
        ;

        insert  tmp_subject_list (
            ayp_subject_id
            ,client_ayp_subject_code
            ,ayp_subject_code
        )
        select  sub.ayp_subject_id
                ,xhs.client_ayp_subject_code
                ,sub.ayp_subject_code
        from    c_ayp_test_type as tt
        join    c_ayp_subject as sub
                on  tt.ayp_test_type_id = sub.ayp_test_type_id
        join    v_pmi_xref_ayp_subject_hst_mapping as xhs
                on  xhs.pmi_ayp_subject_code = sub.ayp_subject_code
                and xhs.pmi_test_type_code = tt.moniker
        where   tt.moniker in ('CO TCAP')
        ;

        insert tmp_school (
            school_id
            ,school_code
        )
        select school_id
                ,school_code
        from c_school
        where school_code is not null
#########  Add this back later once school file is fixed
/*        
        union
        select school_id
               ,school_state_code
        from c_school
        where school_state_code is not null
*/
        ;
        
        
        SET @sql_text := '';
        SET @sql_text := concat(@sql_text,'
        insert  tmp_strand_score_moniker (
            ayp_subject_id
            ,ayp_subject_code
            ,ayp_strand_id
            ,ayp_strand_code
            ,begin_school_year_id
            ,end_school_year_id
            ,begin_grade_sequence
            ,end_grade_sequence
            ,str_score_col
        )
        select  ayps.ayp_subject_id
                ,csub.ayp_subject_code
                ,ayps.ayp_strand_id
                ,cstr.ayp_strand_code
                ,ayps.begin_school_year_id
                ,ayps.end_school_year_id
                ,ayps.begin_grade_sequence
                ,ayps.end_grade_sequence
                ,min(sc.moniker) as str_score_col
        from ',@db_name_ods,'.imp_table_column_ayp_strand_grade ayps #v_imp_table_column_ayp_strand as ayps
        join v_imp_table as tab
                on ayps.table_id = tab.table_id
        join v_imp_table_column as sc 
                on  ayps.table_id = sc.table_id
                and ayps.score_column_id = sc.column_id
                and ayps.active_flag = 1
        join c_ayp_subject as csub
                on ayps.ayp_subject_id = csub.ayp_subject_id
        join c_ayp_strand as cstr
                on ayps.ayp_strand_id = cstr.ayp_strand_id
        where tab.target_table_name = ''', v_ods_table,'''
        group by ayps.ayp_subject_id
                ,ayps.ayp_strand_id
                ,ayps.begin_school_year_id
                ,ayps.end_school_year_id
                ,ayps.begin_grade_sequence
                ,ayps.end_grade_sequence
        ; ' );
        
        prepare stmt from @sql_text;
        execute stmt;
        deallocate prepare stmt;
        
        
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
            ,ods.local_student_id
            ,ods.content_area
            ,ods.test_date
            ,s.student_id
            ,sub.ayp_subject_id
            ,Month(tmp.test_date)
            ,sty.school_year_id
            ,ods.grade_code
            ,ods.school_id
            ,case when syr.school_year_id is null then 1 
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_co_tcap as ods
        join    tmp_test_date as tmp
                on      ods.test_date = tmp.ods_test_date
        join    c_student as s
                on      s.student_code = ods.local_student_id
        join    c_school_year as sty
                on     tmp.test_date between sty.begin_date and sty.end_date
        join    tmp_subject_list as sub
                on      ods.content_area = sub.client_ayp_subject_code
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sty.school_year_id
        where ods.total_scale_score REGEXP '^[0-9]' > 0
        union all
        select  ods.row_num
            ,ods.state_student_id
            ,ods.content_area
            ,ods.test_date
            ,s.student_id
            ,sub.ayp_subject_id
            ,Month(tmp.test_date)
            ,sty.school_year_id
            ,ods.grade_code
            ,ods.school_id
            ,case when syr.school_year_id is null then 1 
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_co_tcap as ods
        join    tmp_test_date as tmp
                on      ods.test_date = tmp.ods_test_date
        join    c_student as s
                on      s.student_state_code = ods.state_student_id
        join    c_school_year as sty
                on     tmp.test_date between sty.begin_date and sty.end_date
        join    tmp_subject_list as sub
                on      ods.content_area = sub.client_ayp_subject_code
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sty.school_year_id
        where ods.total_scale_score REGEXP '^[0-9]' > 0
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
    
        insert c_ayp_subject_student (
            student_id
            ,ayp_subject_id
            ,school_year_id
            ,month_id
            ,alt_ayp_score
            ,score_type_code
            ,ayp_score
            ,ayp_score_color
            ,last_user_id
            ,create_timestamp
        )
        select  tsa.student_id
            ,tsa.ayp_subject_id
            ,tsa.school_year_id
            ,tsa.test_month
            ,null
            ,'n'
            ,ods.total_scale_score
            ,null as ayp_score_color
            ,1234
            ,now()
        from    v_pmi_ods_co_tcap as ods
        join    tmp_stu_admin as tsa
                on      tsa.row_num = ods.row_num
        on duplicate key update last_user_id = values(last_user_id)
            ,ayp_score = values(ayp_score)
        ;
        
        
        ######################
        ## Load Strand Data ##
        ######################
       
       set v_no_more_rows = false;
 
       Open v_strand_cursor;
        loop_strand_cursor: loop

        Fetch v_strand_cursor 
        into v_ayp_subject_id, v_ayp_subject_code, v_ayp_strand_id, v_ayp_strand_code,  
                v_begin_school_year_id, v_end_school_year_id, v_begin_grade_sequence, v_end_grade_sequence, v_str_score_col;
        
            if v_no_more_rows then
                close v_strand_cursor;
                leave loop_strand_cursor;
            end if;

                    set @sql_text := '';
                    set @sql_text := concat(@sql_text, ' insert c_ayp_strand_student (ayp_subject_id, ayp_strand_id ,student_id, school_year_id, '
                                                       , ' month_id, ayp_score, points_earned, points_possible, last_user_id, create_timestamp) '
                                                       , ' select ', v_ayp_subject_id
                                                       ,        ', ', v_ayp_strand_id
                                                       , '       , tsa.student_id '
                                                       , '       , tsa.school_year_id '
                                                       , '       , tsa.test_month '
                                                       , '       , cast(ods.', v_str_score_col, ' as decimal(9,3)) '
                                                       , '       , null '
                                                       , '       , null '
                                                       , '       , 1234 '
                                                       , '       , now() '
                                                       , ' from v_pmi_ods_co_tcap as ods '
                                                       , ' join tmp_stu_admin as tsa '
                                                       , '          on tsa.row_num = ods.row_num '
                                                       , '          and tsa.ayp_subject_id = ',v_ayp_subject_id
                                                       ,'           and tsa.school_year_id between ', v_begin_school_year_id, ' and ', v_end_school_year_id
                                                       ,'           and cast(tsa.grade_code as signed) between cast(', v_begin_grade_sequence, ' as signed) and cast(', v_end_grade_sequence, ' as signed)'
                                                       , ' join c_ayp_subject_student as stu '
                                                       , '          on stu.student_id = tsa.student_id '
                                                       , '          and stu.ayp_subject_id = tsa.ayp_subject_id '
                                                       , '          and stu.school_year_id = tsa.school_year_id '
                                                       , '          and stu.month_id = tsa.test_month '
                                                       , ' where ods.', v_str_score_col, ' REGEXP \'^[0-9]\' > 0 '
                                                       , ' ON DUPLICATE KEY UPDATE last_user_id = values(last_user_id), ayp_score = values(ayp_score);');


                    prepare sql_text from @sql_text;
                    execute sql_text;
                    deallocate prepare sql_text;


            end loop loop_strand_cursor;
 

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
        drop table if exists `tmp_test_date`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_school`;
        drop table if exists `tmp_strand_score_moniker`;

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
