
drop procedure if exists etl_hst_load_fl_fcat_math_reading_20//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_fl_fcat_math_reading_20()
contains sql
sql security invoker
comment 'zendesk ticket 19687'

proc: begin 

    declare v_ayp_subject_id            int(11);
    declare v_ayp_strand_id             int(11);
    declare v_column_pe                 varchar(64);
    declare v_column_pp                 varchar(64);
    declare v_ayp_score_col             varchar(64);
    declare v_alt_ayp_score_col         varchar(64);
    declare v_no_more_rows              boolean;
    declare v_ods_table                 varchar(64);
    declare v_ods_view                  varchar(64);
    declare v_view_exists               tinyint(1);
    declare v_school_unassigned_id      int(10);
    declare v_grade_unassigned_id       int(10);
    declare v_backfill_needed           int(10);
    declare v_delete_count              int(10);
    declare v_ayp_subject_id_fcat_math  int(11);
    declare v_ayp_subject_id_fcat_read  int(11);
    declare v_strand_grade_table_exists smallint(4);
    
    declare v_ayp_subject_code varchar(40);
    declare v_ayp_strand_code varchar(40);
    declare v_begin_school_year_id int(11);
    declare v_end_school_year_id int(11);
    declare v_begin_grade_sequence varchar(2);
    declare v_end_grade_sequence varchar(2);
    declare v_column_ss varchar(50);
    declare v_column_dev varchar(50);
    
    declare v_subject_cursor cursor for
            select ayp_subject_id, scale_score_moniker, dev_score_moniker
            from tmp_ayp_subject_to_ods_col_list
            ;

    declare v_strand_cursor cursor for
            select ayp_subject_id
                  ,ayp_subject_code
                  ,ayp_strand_id
                  ,ayp_strand_code
                  ,begin_school_year_id
                  ,end_school_year_id
                  ,begin_grade_sequence
                  ,end_grade_sequence
                  ,column_pe
                  ,column_pp
                  ,column_ss
                  ,column_dev
            from tmp_strand_score_moniker;

    declare continue handler for not found 
    set v_no_more_rows = true;
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend ,@db_name_dw);

    set v_ods_table = 'pmi_ods_fcat_math_reading_20';
    set v_ods_view = 'v_pmi_ods_fcat_math_reading_20';


    select  count(*)
    into    v_view_exists
    from    information_schema.views as t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;
    
    select  count(*)
    into    v_strand_grade_table_exists
    from    information_schema.tables as t
    where   t.table_schema = @db_name_ods
    and     t.table_name = 'imp_table_column_ayp_strand_grade';
    

    if v_view_exists > 0 and v_strand_grade_table_exists > 0 then

        #######################
        ## Load Working Tables ##
        #######################

        drop table if exists `tmp_ayp_subject_to_ods_col_list`;
        #drop table if exists `tmp_fcat_strand_list`;
        drop table if exists `tmp_strand_score_moniker`;
        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_stu_admin_subject`;
        drop table if exists `tmp_fcat_date_conversion`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_school`;
 
        create table `tmp_ayp_subject_to_ods_col_list` (
          `ayp_subject_id` int(10) not null,
          `ayp_subject_code` varchar(50) not null,
          `scale_score_moniker` varchar(25) not null,
          `dev_score_moniker` varchar(25) null,
          primary key  (`ayp_subject_id`),
          unique key `uq_tmp_ayp_subject_to_ods_col_list` (`scale_score_moniker`)
        ) engine=innodb default charset=latin1
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
            `column_pe` varchar(50) not null,
            `column_pp` varchar(50) not null,
            `column_ss` varchar(50) not null,
            `column_dev` varchar(50) not null,
            PRIMARY KEY  (`ayp_subject_id`,`ayp_strand_id`,`begin_school_year_id`,`end_school_year_id`,`begin_grade_sequence`,`end_grade_sequence`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

      
        create table `tmp_stu_admin` (
          `student_code` varchar(15) not null,
          `test_mo_year` varchar(10) not null,
          `row_num` int(10) not null,
          `student_id` int(10) not null,
          `ayp_subject_id` int(10) not null,
          `test_month` tinyint(2) not null,
          `school_year_id` smallint(4) not null,
          `grade_code` varchar(15) default null,
          `school_code` varchar(15) default null,
          `backfill_needed_flag` tinyint(1),
          primary key (`student_id`,`ayp_subject_id`,`school_year_id`,`test_month`),
          unique key `uq_tmp_stu_admin` (`student_code`, `ayp_subject_id`,`test_mo_year`),
          key `ind_tmp_stu_admin` (`row_num`, `ayp_subject_id`)
        ) engine=innodb default charset=latin1
        ;
        
        create table `tmp_stu_admin_subject` (
          `row_num` int(10) not null,
          `student_code` varchar(15) not null,
          `test_mo_year` varchar(10) not null,
          `ayp_subject_id` int(10) not null,
          `grade_code` varchar(15) default null,
          `school_code` varchar(15) default null,
          unique key `uq_tmp_stu_admin_subject` (`student_code`,`test_mo_year`,`ayp_subject_id`)
        ) engine=innodb default charset=latin1
        ;

        create table `tmp_fcat_date_conversion` (
          `test_mo_year` varchar(10) not null,
          `test_month` tinyint(2) not null,
          `test_date` date not null,
          unique key `uq_tmp_fcat_date_conversion` (`test_mo_year`)
        ) engine=innodb default charset=latin1
        ;
        
       
        create table `tmp_school` (
          `school_code` varchar(15) not null,
          `school_id` int (10) not null,
          unique key `ind_tmp_school_school_code` (`school_code`)
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
        

        select school_id
        into    v_school_unassigned_id 
        from    c_school
        where   school_code = 'unassigned'
        ;
    
        select  grade_level_id
        into    v_grade_unassigned_id
        from    c_grade_level
        where   grade_code = 'unassigned'
        ;

        select  min(case when ayp_subject_code = 'fcatMath' then ayp_subject_id end)
            ,min(case when ayp_subject_code = 'fcatReading' then ayp_subject_id end)
        into    v_ayp_subject_id_fcat_math
            ,v_ayp_subject_id_fcat_read

        from    c_ayp_subject as sub
        where   ayp_subject_code in ('fcatMath','fcatReading')
        ;
        

     ##############################################################################
     ## test date provided as mmyyyy.  test date needed for c_student_year join, ## 
     ## test month and year needed for multi admins                              ##
     ##############################################################################

        insert into tmp_fcat_date_conversion (
            test_mo_year
            ,test_month
            ,test_date
        )

        select  ods.test_mo_year
            ,min(month(str_to_date(ods.test_mo_year, '%m%Y'))) as test_month
            ,min(str_to_date(concat('15', ods.test_mo_year), '%d%m%Y')) as test_date
        from    v_pmi_ods_fcat_math_reading_20 as ods
        group by    ods.test_mo_year
        ;


        insert into tmp_ayp_subject_to_ods_col_list (
             ayp_subject_id
            ,ayp_subject_code
            ,scale_score_moniker
            ,dev_score_moniker
        )
        select  ayps.ayp_subject_id
            ,sub.ayp_subject_code
            ,sco.moniker as scale_score_moniker 
            ,alt.moniker as dev_score_moniker 
        from    v_imp_table_column_ayp_subject as ayps
        join    v_imp_table as tab
                on      ayps.table_id = tab.table_id
                and     tab.target_table_name = v_ods_table
        join    v_imp_table_column as sco
                on      ayps.table_id = sco.table_id
                and     ayps.score_column_id = sco.column_id
        join    v_imp_table_column as alt
                on      ayps.table_id = alt.table_id
                and     ayps.alt_score_column_id = alt.column_id
        join    c_ayp_subject as sub
                on      ayps.ayp_subject_id = sub.ayp_subject_id
        join    c_ayp_test_type as typ
                on      sub.ayp_test_type_id = typ.ayp_test_type_id
                and     typ.moniker = 'fcat'
        where   ayps.active_flag = 1
        group by ayps.ayp_subject_id
            ,sub.ayp_subject_code
        ;

        /*insert into tmp_fcat_strand_list (
            ayp_subject_id
            ,ayp_strand_id
            ,column_pe
            ,column_pp
        )
        select  ayps.ayp_subject_id
            ,ayps.ayp_strand_id
            ,pe.moniker as column_pe
            ,pp.moniker  as column_pp

        from    v_imp_table_column_ayp_strand as ayps
        join    v_imp_table as tab
                on      ayps.table_id = tab.table_id
                and     tab.target_table_name = v_ods_table
        join    v_imp_table_column as pe
                on      ayps.table_id = pe.table_id
                and     ayps.pe_column_id = pe.column_id
        join    v_imp_table_column as pp
                on      ayps.table_id = pp.table_id
                and     ayps.pp_column_id = pp.column_id
        join    c_ayp_subject as sub
                on      ayps.ayp_subject_id = sub.ayp_subject_id
        join    c_ayp_test_type as typ
                on      sub.ayp_test_type_id = typ.ayp_test_type_id
                and     typ.moniker = 'fcat'
        where   ayps.active_flag = 1
        group by ayps.ayp_subject_id
                ,ayps.ayp_strand_id
                ,pe.moniker
                ,pp.moniker 
        ;*/
        
        ########### NOTE ####################################################################################
        #  ods.imp_table_column_ayp_strand_grade shouldn't be dropped. extention of imp_table_column_ayp_strand
        #  begin school year, end school year, begin grade sequence and end grade sequence
        #####################################################################################################
        
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
            ,column_pe
            ,column_pp
            ,column_ss
            ,column_dev
        )
        select  ayps.ayp_subject_id
                ,csub.ayp_subject_code
                ,ayps.ayp_strand_id
                ,cstr.ayp_strand_code
                ,ayps.begin_school_year_id
                ,ayps.end_school_year_id
                ,ayps.begin_grade_sequence
                ,ayps.end_grade_sequence
                ,min(pe.moniker) as column_pe
                ,min(pp.moniker) as column_pp
                ,min(sublist.scale_score_moniker) as column_ss
                ,min(sublist.dev_score_moniker) as column_dev
        from ',@db_name_ods,'.imp_table_column_ayp_strand_grade ayps #v_imp_table_column_ayp_strand as ayps
        join v_imp_table as tab
                on ayps.table_id = tab.table_id
        join v_imp_table_column as pe 
                on  ayps.table_id = pe.table_id
                and ayps.pe_column_id = pe.column_id
                and ayps.active_flag = 1
        join v_imp_table_column as pp 
                on  ayps.table_id = pp.table_id
                and ayps.pp_column_id = pp.column_id
                and ayps.active_flag = 1
        join c_ayp_subject as csub
                on ayps.ayp_subject_id = csub.ayp_subject_id
        join c_ayp_strand as cstr
                on ayps.ayp_strand_id = cstr.ayp_strand_id
        join tmp_ayp_subject_to_ods_col_list as sublist
                on sublist.ayp_subject_id = ayps.ayp_subject_id
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
        
        # This table is in place b/c it is possible that a student's math score can come in on one line, and reading on another line
        #   for the same administration.
        insert  tmp_stu_admin_subject (
                    row_num
                    ,student_code
                    ,test_mo_year
                    ,ayp_subject_id
                    ,grade_code
                    ,school_code
        
        )
        select      ods.row_num
                    ,ods.student_id
                    ,ods.test_mo_year
                    ,v_ayp_subject_id_fcat_math
                    ,ods.grade
                    ,ods.school_state_code
        from        v_pmi_ods_fcat_math_reading_20 as ods
        where       ods.student_id is not null  
               and  ods.test_name != 'FCAT SPRING 2011 RETAKE'   ## Retakes will use older process
               and (ods.math_scale_score REGEXP '^[0-9]' > 0
                    OR ods.math_dev_score REGEXP '^[0-9]' > 0)
        union all
        select      ods2.row_num
                    ,ods2.student_id
                    ,ods2.test_mo_year
                    ,v_ayp_subject_id_fcat_read
                    ,ods2.grade
                    ,ods2.school_state_code
        from        v_pmi_ods_fcat_math_reading_20 as ods2
        where       ods2.student_id is not null 
               and  ods2.test_name != 'FCAT SPRING 2011 RETAKE'   ## Retakes will use older process
               and (ods2.read_scale_score REGEXP '^[0-9]' > 0
                    OR ods2.read_dev_score REGEXP '^[0-9]' > 0)
        on duplicate key update row_num = values(row_num)
        ;
        
        insert  tmp_stu_admin (
            row_num
            ,student_code
            ,test_mo_year
            ,student_id
            ,ayp_subject_id
            ,test_month
            ,school_year_id
            ,grade_code
            ,school_code
            ,backfill_needed_flag
        )
            
        select  tmpSas.row_num
            ,tmpSas.student_code as student_code
            ,tmpSas.test_mo_year
            ,s.student_id
            ,tmpSas.ayp_subject_id
            ,c_date.test_month
            ,sy.school_year_id
            ,tmpSas.grade_code
            ,tmpSas.school_code
            ,case when syr.school_year_id is null then 1
                  when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1 
             end as backfill_needed_flag
        from    tmp_stu_admin_subject as tmpSas
        join    c_student as s
                on      s.student_code = tmpSas.student_code
        JOIN tmp_fcat_date_conversion as c_date
                on tmpSas.test_mo_year = c_date.test_mo_year
        JOIN c_school_year as sy 
                on c_date.test_date BETWEEN sy.begin_date AND sy.end_date
        left join   c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sy.school_year_id
        union all
        select  tmpSas2.row_num
            ,tmpSas2.student_code as student_code
            ,tmpSas2.test_mo_year
            ,s2.student_id
            ,tmpSas2.ayp_subject_id
            ,c_date2.test_month
            ,sy2.school_year_id
            ,tmpSas2.grade_code
            ,tmpSas2.school_code
            ,case when syr2.school_year_id is null then 1
                  when syr2.school_year_id is not null and syr2.grade_level_id = v_grade_unassigned_id then 1 
             end as backfill_needed_flag
        from    tmp_stu_admin_subject as tmpSas2
        join    c_student as s2
                on      s2.student_state_code = tmpSas2.student_code
        JOIN tmp_fcat_date_conversion as c_date2
                on tmpSas2.test_mo_year = c_date2.test_mo_year
        JOIN c_school_year as sy2 
                on c_date2.test_date BETWEEN sy2.begin_date AND sy2.end_date
        left join   c_student_year as syr2
                on syr2.student_id = s2.student_id
                and syr2.school_year_id = sy2.school_year_id
        union all
        select  tmpSas3.row_num
            ,tmpSas3.student_code as student_code
            ,tmpSas3.test_mo_year
            ,s3.student_id
            ,tmpSas3.ayp_subject_id
            ,c_date3.test_month
            ,sy3.school_year_id
            ,tmpSas3.grade_code
            ,tmpSas3.school_code
            ,case when syr3.school_year_id is null then 1
                  when syr3.school_year_id is not null and syr3.grade_level_id = v_grade_unassigned_id then 1 
             end as backfill_needed_flag
        from    tmp_stu_admin_subject as tmpSas3
        join    c_student as s3
                on      s3.fid_code = tmpSas3.student_code
        JOIN tmp_fcat_date_conversion as c_date3
                on tmpSas3.test_mo_year = c_date3.test_mo_year
        JOIN c_school_year as sy3
                on c_date3.test_date BETWEEN sy3.begin_date AND sy3.end_date
        left join   c_student_year as syr3
                on syr3.student_id = s3.student_id
                and syr3.school_year_id = sy3.school_year_id
        on duplicate key update row_num = values(row_num);
        
        
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

        open v_subject_cursor;
        loop_subject_cursor: loop
        FETCH v_subject_cursor 
        INTO  v_ayp_subject_id, v_ayp_score_col, v_alt_ayp_score_col;

            if v_no_more_rows then
                close v_subject_cursor;
                leave loop_subject_cursor;
            end if;
     
            SET @sql_text := '';
            SET @sql_text := concat('insert c_ayp_subject_student (student_id,ayp_subject_id,school_year_id, month_id, ayp_score, alt_ayp_score, last_user_id,create_timestamp)'
                                , ' SELECT  sadmin.student_id,',v_ayp_subject_id ,', sadmin.school_year_id, sadmin.test_month, cast(m.',v_ayp_score_col ,' as decimal(9,3)), cast(m.', v_alt_ayp_score_col ,' as decimal(9,3)),1234, now() '
                                , ' FROM v_pmi_ods_fcat_math_reading_20 AS m ' 
                                , ' join tmp_stu_admin as sadmin on m.row_num = sadmin.row_num '
                                , '     and sadmin.ayp_subject_id = ',v_ayp_subject_id,' '
                                , ' ON DUPLICATE KEY UPDATE '
                                , 'last_user_id = values(last_user_id), ayp_score = values(ayp_score), alt_ayp_score = values(alt_ayp_score); ');
                                
            prepare stmt from @sql_text;
            execute stmt;
            deallocate prepare stmt;
            
            end loop loop_subject_cursor;

        ######################
        ## Load Strand Data ##
        ######################
       
       set v_no_more_rows = false;
 
       Open v_strand_cursor;
        loop_strand_cursor: loop

        Fetch v_strand_cursor 
        #into v_ayp_subject_id, v_ayp_strand_id, v_column_pe, v_column_pp;
        into v_ayp_subject_id, v_ayp_subject_code, v_ayp_strand_id, v_ayp_strand_code,  
                v_begin_school_year_id, v_end_school_year_id, v_begin_grade_sequence, v_end_grade_sequence, v_column_pe, v_column_pp, v_column_ss,v_column_dev;
        
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
                                                       , '       , round((cast(ods.', v_column_pe, ' as decimal(9,3))/cast(ods.', v_column_pp, ' as decimal(9,3))) * 100, 3) '
                                                       , '       , cast(ods.', v_column_pe, ' as decimal(9,3)) '
                                                       , '       , cast(ods.', v_column_pp, ' as decimal(9,3)) '
                                                       , '       , 1234 '
                                                       , '       , now() '
                                                       , ' from v_pmi_ods_fcat_math_reading_20 as ods '
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
                                                       , ' where ods.', v_column_pe, ' REGEXP \'^[0-9]\' > 0 '
                                                       , ' and   ods.', v_column_pp, ' REGEXP \'^[0-9]\' > 0 '
                                                       , ' and  ( ods.', v_column_ss, ' REGEXP \'^[0-9]\' > 0 '
                                                       , ' or  ods.', v_column_dev, ' REGEXP \'^[0-9]\' > 0 )'
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
        
            delete  ayp_str.*
            from    tmp_stu_admin as tmp1
            join    c_ayp_subject_student as ss
                    on      tmp1.student_id = ss.student_id
                    and     tmp1.ayp_subject_id = ss.ayp_subject_id
                    and     tmp1.school_year_id = ss.school_year_id
                    and     ss.month_id = 0
            join    c_ayp_strand_student as ayp_str
                    on      ayp_str.ayp_subject_id = ss.ayp_subject_id
                    and     ayp_str.student_id = ss.student_id
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
            
        drop table if exists `tmp_ayp_subject_to_ods_col_list`;
        #drop table if exists `tmp_fcat_strand_list`;
        drop table if exists `tmp_strand_score_moniker`;
        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_stu_admin_subject`;
        drop table if exists `tmp_fcat_date_conversion`;
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