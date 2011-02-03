/*
$Rev: 9335 $ 
$Author: randall.stanley $ 
$Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_hst_load_fl_fcat_writing.sql $
$Id: etl_hst_load_fl_fcat_writing.sql 9335 2010-10-03 18:10:23Z randall.stanley $ 
*/

drop procedure if exists etl_hst_load_fl_fcat_writing//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_fl_fcat_writing()
contains sql
sql security invoker
comment '$Rev: 9335 $'

proc: begin 

    declare v_ayp_subject_id int(11);
    declare v_column_moniker varchar(64);
    declare v_no_more_rows boolean;
    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_view_exists smallint(4);
    declare v_school_unassigned_id  int(10);
    declare v_grade_unassigned_id int(10);
    declare v_backfill_needed int(10);
    declare v_delete_count int(10);
    declare v_ayp_subject_id_writing int(11);

    declare v_subject_cursor cursor for
            select ayp_subject_id, score_col_name
            from tmp_ayp_subject_to_ods_col_list
            ;
            
    declare continue handler for not found 
    set v_no_more_rows = true;
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    set v_ods_table := 'pmi_ods_fcat_writing';
    set v_ods_view := 'v_pmi_ods_fcat_writing';
    
    select  school_id
    into    v_school_unassigned_id 
    from    c_school
    where   school_code = 'unassigned'
    ;

    select  grade_level_id
    into    v_grade_unassigned_id
    from    c_grade_level
    where   grade_code = 'unassigned'
    ;
    
    select  ayp_subject_id
    into    v_ayp_subject_id_writing
    from    c_ayp_subject
    where   ayp_subject_code = 'fcatWriting'
    ;
    
    select  count(*)
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;
    

    if v_view_exists > 0 then

        #######################
        ## Load Working Tables ##
        #######################

        drop table if exists `tmp_ayp_subject_to_ods_col_list`;
        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_fcat_date_conversion`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_school`;
 
        create table `tmp_ayp_subject_to_ods_col_list` (
          `ayp_subject_id` int(10) not null,
          `ayp_subject_code` varchar(50) not null,
          `score_col_name` varchar(64) not null,
          primary key  (`ayp_subject_id`),
          unique key `uq_tmp_ayp_subject_to_ods_col_list` (`score_col_name`)
        ) engine=innodb default charset=latin1
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
          unique key `ind_school_code` (`school_code`)
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
        from    v_pmi_ods_fcat_writing as ods
        group by    ods.test_mo_year
        ;

        insert into tmp_ayp_subject_to_ods_col_list (
             ayp_subject_id
            ,ayp_subject_code
            ,score_col_name
         )
        select ayps.ayp_subject_id
               ,sub.ayp_subject_code
               ,min(sco.moniker) as score_col_name 
        from v_imp_table_column_ayp_subject as ayps
        join v_imp_table as tab
                on ayps.table_id = tab.table_id
                and target_table_name = v_ods_table
                and ayps.active_flag = 1
        join v_imp_table_column as sco
                on ayps.table_id = sco.table_id
                and ayps.score_column_id = sco.column_id
        join c_ayp_subject as sub
                on ayps.ayp_subject_id = sub.ayp_subject_id
        join c_ayp_test_type as typ
                on sub.ayp_test_type_id = typ.ayp_test_type_id
                and typ.moniker = 'fcat'
        group by ayps.ayp_subject_id
               ,sub.ayp_subject_code
        ;

        #Currently, school is not in ods table.  But keeping here in case uploader table is modified. Nominal impact
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
            
        select  ods.row_num
            ,ods.student_id as student_code
            ,ods.test_mo_year
            ,s.student_id
            ,v_ayp_subject_id_writing
            ,dte.test_month
            ,sy.school_year_id
            ,ods.grade
            ,ods.school_state_code
            ,case when sty.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_fcat_writing as ods
        join    c_student as s
                on      s.student_code = ods.student_id
        join    tmp_fcat_date_conversion as dte
                on      ods.test_mo_year = dte.test_mo_year
        join    c_school_year as sy 
                on      dte.test_date between sy.begin_date and sy.end_date
        left join   c_student_year as sty 
                on      sty.student_id = s.student_id 
                and     sty.school_year_id = sy.school_year_id
        where   ods.student_id is not null
        and     ods.score REGEXP '^[0-9]' > 0
        union all
        select  ods2.row_num
            ,ods2.student_id as student_code
            ,ods2.test_mo_year
            ,s2.student_id
            ,v_ayp_subject_id_writing
            ,dte2.test_month
            ,sy2.school_year_id
            ,ods2.grade
            ,ods2.school_state_code
            ,case when sty2.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_fcat_writing as ods2
        join    c_student as s2
                on      s2.student_state_code = ods2.student_id
        join    tmp_fcat_date_conversion as dte2
                on      ods2.test_mo_year = dte2.test_mo_year
        join    c_school_year as sy2 
                on      dte2.test_date between sy2.begin_date and sy2.end_date
        left join c_student_year as sty2
                on      sty2.student_id = s2.student_id 
                and     sty2.school_year_id = sy2.school_year_id
        where   ods2.student_id is not null
        and     ods2.score REGEXP '^[0-9]' > 0
        union all
        select  ods3.row_num
            ,ods3.student_id as student_code
            ,ods3.test_mo_year
            ,s3.student_id
            ,v_ayp_subject_id_writing
            ,dte3.test_month
            ,sy3.school_year_id
            ,ods3.grade
            ,ods3.school_state_code
            ,case when sty3.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_fcat_writing as ods3
        join    c_student as s3
                on      s3.fid_code = ods3.student_id
        join    tmp_fcat_date_conversion as dte3
                on      ods3.test_mo_year = dte3.test_mo_year
        join    c_school_year as sy3 
                on      dte3.test_date between sy3.begin_date and sy3.end_date
        left join c_student_year as sty3 
                on      sty3.student_id = s3.student_id 
                and     sty3.school_year_id = sy3.school_year_id
        where   ods3.student_id is not null
        and     ods3.score REGEXP '^[0-9]' > 0
        on duplicate key update row_num = values(row_num);

        
        ##########################################
        ## Backfill for c_student_year 
        ## Need to detect and load c_student_year 
        ## records when supporting ones don't exist
        ##############################################

        select count(*)
        into    v_backfill_needed
        from    tmp_stu_admin
        where   backfill_needed_flag = 1
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
            from    tmp_stu_admin as sadmin
            left join   v_pmi_xref_grade_level as gxref
                    on      sadmin.grade_code = gxref.client_grade_code
            left join   c_grade_level as grd
                    on      gxref.pmi_grade_code = grd.grade_code
            left join   tmp_school as sch
                    on      sadmin.school_code = sch.school_code
            where   sadmin.backfill_needed_flag = 1
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
        INTO  v_ayp_subject_id, v_column_moniker;

            if v_no_more_rows then
                close v_subject_cursor;
                leave loop_subject_cursor;
            end if;
            
            SET @sql_text := '';
            SET @sql_text := concat('insert c_ayp_subject_student (student_id,ayp_subject_id,school_year_id, month_id, ayp_score,last_user_id,create_timestamp)'
                                , ' SELECT  sadmin.student_id,', v_ayp_subject_id, ',sadmin.school_year_id, sadmin.test_month, cast(m.',v_column_moniker ,' as decimal(9,3)) as ayp_score '
                                , ' ,1234 as last_user_id, now() create_timestamp '
                                , ' FROM v_pmi_ods_fcat_writing AS m ' 
                                , ' join tmp_stu_admin as sadmin on m.row_num = sadmin.row_num '
                                , '    and sadmin.ayp_subject_id = ',v_ayp_subject_id, ' '
                                , ' ON DUPLICATE KEY UPDATE last_user_id = values(last_user_id), ayp_score = values(ayp_score); ');

            prepare stmt from @sql_text;
            execute stmt;
            deallocate prepare stmt;
            
            end loop loop_subject_cursor;


         
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
        drop table if exists `tmp_stu_admin`;
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
