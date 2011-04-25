/*
$Rev: 9335 $ 
$Author: randall.stanley $ 
$Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_hst_load_mn_mca_ii.sql $
$Id: etl_hst_load_mn_mca_ii.sql 9335 2010-10-03 18:10:23Z randall.stanley $ 
*/

drop procedure if exists etl_hst_load_mn_mca_ii//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_mn_mca_ii()
contains sql
sql security invoker
comment '$Rev:  $'

proc: begin

    declare v_no_more_rows boolean;
    declare v_ayp_subject_id int(11);
    declare v_ayp_strand_id int(11);
    declare v_column_pe varchar(50);
    declare v_column_pp varchar(50);
    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_school_year_id smallint(4);
    declare v_view_exists int(10);
    declare v_school_unassigned_id  int(10);
    declare v_grade_unassigned_id int(10);
    declare v_backfill_needed int(10);
    declare v_delete_count int(10);
    
    declare v_strand_cursor cursor for
        select  ayp_subject_id
            ,ayp_strand_id
            ,column_pe
            ,column_pp
        from    tmp_strand_list;
              
    declare continue handler for not found 
    set v_no_more_rows = true;

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    set v_ods_table := 'pmi_ods_mn_mca_ii';
    set v_ods_view := 'v_pmi_ods_mn_mca_ii';

    select  school_id
    into    v_school_unassigned_id 
    from    c_school
    where   school_code = 'unassigned';


    select  grade_level_id
    into    v_grade_unassigned_id
    from    c_grade_level
    where   grade_code = 'unassigned';


    select  count(*)
    into    v_view_exists
    from    information_schema.views as t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;

    if v_view_exists > 0 then

        #########################
        ## Load Working Tables ##
        #########################

        drop table if exists `tmp_subject_list`;
        drop table if exists `tmp_strand_list`;
        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_test_date`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_school`;
        

        CREATE TABLE `tmp_subject_list` (
          `ayp_subject_id` int(10) NOT NULL,
          `client_ayp_subject_code` varchar(2) NOT NULL,
          `ayp_subject_code` varchar(50) NOT NULL,
          PRIMARY KEY  (`ayp_subject_id`),
          UNIQUE KEY `uq_tmp_subject_list` (`client_ayp_subject_code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        CREATE TABLE `tmp_strand_list` (
          `ayp_subject_id` int(10) NOT NULL, 
          `ayp_strand_id` int(10) NOT NULL,
          `column_pe` varchar(50) NOT NULL,
          `column_pp` varchar(50) NOT NULL,
          PRIMARY KEY  (`ayp_subject_id`,`ayp_strand_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        CREATE TABLE `tmp_school` (
          `school_code` varchar(15) not null,
          `school_id` int (10) not null,
          UNIQUE KEY `uq_tmp_school` (`school_code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        CREATE TABLE `tmp_test_date` (
          `ods_test_date` varchar(8) NOT NULL,
          `test_date` date NOT NULL,
          PRIMARY KEY  (`test_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_stu_admin` (
          `student_code` varchar(15) NOT NULL,
          `client_ayp_subject_code` varchar(2) NOT NULL,
          `ods_test_date` varchar(10) NOT NULL,
          `row_num` int(10) NOT NULL,
          `student_id` int(10) NOT NULL,
          `ayp_subject_id` int(10) NOT NULL,
          `test_month` tinyint(2) NOT NULL,
          `school_year_id` smallint(4) NOT NULL,
          `ayp_score` decimal(9,3) default NULL,
          `grade_code` varchar(15) default null,
          `school_code` varchar(15) default null,
          `backfill_needed_flag` tinyint(1),
          PRIMARY KEY (`student_id`, `ayp_subject_id`, `school_year_id`, `test_month`),
          UNIQUE KEY `uq_tmp_stu_admin` (`student_code`, `client_ayp_subject_code`,`ods_test_date`),
          KEY `ind_tmp_stu_admin` (`row_num`,`ayp_subject_id`)
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


        ### Populate tmp tables
        
        insert into tmp_test_date (
            ods_test_date
            ,test_date
        )
        select  ods.test_date as ods_test_date
            ,max(str_to_date(ods.test_date, '%Y%m%d')) as test_date
        from v_pmi_ods_mn_mca_ii as ods
        group by    ods.test_date
        ;
        
        # tmp_subject_list
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
        where   tt.moniker = 'MCA-II'
        ;
        
        # tmp_strand_list
        insert  tmp_strand_list (
            ayp_subject_id
            ,ayp_strand_id
            ,column_pe
            ,column_pp
        )
        select  ayps.ayp_subject_id
            ,ayps.ayp_strand_id
            ,min(pe.moniker) as column_pe
            ,min(pp.moniker)  as column_pp
        from v_imp_table_column_ayp_strand as ayps
        join v_imp_table as tab
                on ayps.table_id = tab.table_id
        join v_imp_table_column as pe
                on ayps.table_id = pe.table_id
                and ayps.pe_column_id = pe.column_id
        join v_imp_table_column as pp
                on ayps.table_id = pp.table_id
                and ayps.pp_column_id = pp.column_id
        where tab.target_table_name = v_ods_table
        group by ayps.ayp_subject_id
                ,ayps.ayp_strand_id
        ;
        
        # tmp_school
        insert tmp_school (
            school_id
            ,school_code
        )
        select  school_id
            ,school_code
        from c_school
        where school_code is not null
        union
        select  school_id
            ,school_state_code
        from c_school
        where school_state_code is not null
        ;

        #tmp_stu_admin
        insert  tmp_stu_admin (
            row_num
            ,student_code
            ,client_ayp_subject_code
            ,ods_test_date
            ,student_id
            ,ayp_subject_id
            ,test_month
            ,school_year_id
            ,ayp_score
            ,grade_code
            ,school_code
            ,backfill_needed_flag
        )
        select  ods.row_num
            ,ods.student_eid
            ,ods.test_subject_code
            ,ods.test_date
            ,s.student_id
            ,sub.ayp_subject_id
            ,month(dt.test_date)
            ,sy.school_year_id
            ,cast(ods.scale_score as decimal(9,3))
            ,ods.grade_level
            ,ods.school_id
            ,case when syr.school_year_id is null then 1
            when syr.school_year_id is not null and gl.grade_code = 'unassigned' then 1 
             end as backfill_needed_flag
        from    v_pmi_ods_mn_mca_ii as ods
        join    tmp_test_date dt
                on      ods.test_date = dt.ods_test_date
        join    c_student as s
                on      s.student_code = ods.student_eid
        join    c_school_year as sy
                on     dt.test_date between sy.begin_date and sy.end_date
        join    tmp_subject_list as sub
                on      ods.test_subject_code = sub.client_ayp_subject_code
        left join   (c_student_year as syr
            inner join c_grade_level gl
            on syr.grade_level_id = gl.grade_level_id) 
                on syr.student_id = s.student_id
                and syr.school_year_id = sy.school_year_id
        where   ods.student_eid is not null
        and     ods.scale_score REGEXP '^[0-9]' > 0
        union all
        select  ods2.row_num
            ,ods2.student_eid
            ,ods2.test_subject_code
            ,ods2.test_date
            ,s2.student_id
            ,sub2.ayp_subject_id
            ,month(dt2.test_date)
            ,sy2.school_year_id
            ,cast(ods2.scale_score as decimal(9,3))
            ,ods2.grade_level
            ,ods2.school_id
            ,case when syr2.school_year_id is null then 1 
             when syr2.school_year_id is not null and gl.grade_code = 'unassigned' then 1 
            end as backfill_needed_flag
        from    v_pmi_ods_mn_mca_ii as ods2
        join    tmp_test_date dt2
                on      ods2.test_date = dt2.ods_test_date
        join    c_student as s2
                on      s2.student_state_code = ods2.student_eid
        join    c_school_year as sy2
                on     dt2.test_date between sy2.begin_date and sy2.end_date
        join    tmp_subject_list as sub2
                on      ods2.test_subject_code = sub2.client_ayp_subject_code
        left join   (c_student_year as syr2
            inner join c_grade_level gl
            on syr2.grade_level_id = gl.grade_level_id) 
                on syr2.student_id = s2.student_id
                and syr2.school_year_id = sy2.school_year_id
        where   ods2.student_eid is not null
        and     ods2.scale_score REGEXP '^[0-9]' > 0
        union all
        select  ods3.row_num
            ,ods3.student_eid
            ,ods3.test_subject_code
            ,ods3.test_date
            ,s3.student_id
            ,sub3.ayp_subject_id
            ,month(dt3.test_date)
            ,sy3.school_year_id
            ,cast(ods3.scale_score as decimal(9,3))
            ,ods3.grade_level
            ,ods3.school_id
            ,case when syr3.school_year_id is null then 1
             when syr3.school_year_id is not null and gl.grade_code = 'unassigned' then 1 
             end as backfill_needed_flag
        from    v_pmi_ods_mn_mca_ii as ods3
        join    tmp_test_date dt3
                on      ods3.test_date = dt3.ods_test_date
        join    c_student as s3
                on      s3.fid_code = ods3.student_eid
        join    c_school_year as sy3
                on     dt3.test_date between sy3.begin_date and sy3.end_date
        join    tmp_subject_list as sub3
                on      ods3.test_subject_code = sub3.client_ayp_subject_code
        left join   (c_student_year as syr3
            inner join c_grade_level gl
            on syr3.grade_level_id = gl.grade_level_id) 
                on syr3.student_id = s3.student_id
                and syr3.school_year_id = sy3.school_year_id
        where   ods3.student_eid is not null
        and     ods3.scale_score REGEXP '^[0-9]' > 0
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
            from    tmp_stu_admin as sadmin
            left join   v_pmi_xref_grade_level as gxref
                        on  sadmin.grade_code = gxref.client_grade_code
            left join   c_grade_level as grd
                        on  gxref.pmi_grade_code = grd.grade_code
            left join   tmp_school as sch
                        on  sadmin.school_code = sch.school_code
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

        insert c_ayp_subject_student (
            student_id
            ,ayp_subject_id
            ,school_year_id
            ,month_id
            ,ayp_score
            ,score_type_code
            ,last_user_id
            ,create_timestamp
        )
        select  tsa.student_id
            ,tsa.ayp_subject_id
            ,tsa.school_year_id
            ,tsa.test_month
            ,tsa.ayp_score
            ,'n'
            ,1234
            ,now()
        from    tmp_stu_admin as tsa
        on duplicate key update last_user_id = values(last_user_id)
            ,ayp_score = values(ayp_score)
        ;

        ######################
        ## Load Strand Data ##
        ######################

        Open v_strand_cursor;
        loop_strand_cursor: loop

        Fetch v_strand_cursor 
        into v_ayp_subject_id, v_ayp_strand_id, v_column_pe, v_column_pp;
        
            if v_no_more_rows then
                close v_strand_cursor;
                leave loop_strand_cursor;
            end if;

            set @sql_text := concat(    ' insert c_ayp_strand_student (ayp_subject_id, ayp_strand_id ,student_id, school_year_id, ', 
                                        ' month_id, ayp_score, points_earned, points_possible, last_user_id, create_timestamp) ',
                                        ' select ', v_ayp_subject_id, ', ', v_ayp_strand_id, ', tsa.student_id, tsa.school_year_id,',
                                        '  tsa.test_month, round((cast(ods.', v_column_pe, ' as decimal(9,3))/cast(ods.', v_column_pp, ' as decimal(9,3))) * 100, 3), cast(ods.', v_column_pe, ' as decimal(9,3)) ', 
                                        ' , cast(ods.',v_column_pp, ' as decimal(9,3)), 1234, now() ',
                                        ' from    ',v_ods_view ,' as ods ',
                                        ' join    tmp_stu_admin as tsa ',
                                        '          on      tsa.row_num = ods.row_num and tsa.ayp_subject_id = ', v_ayp_subject_id,
                                           ' where   ods.', v_column_pe, ' REGEXP \'^[0-9]\' > 0 ',
                                        ' and     ods.', v_column_pp, ' REGEXP \'^[0-9]\' > 0 ',
                                        ' ON DUPLICATE KEY UPDATE last_user_id = values(last_user_id), ayp_score = values(ayp_score) ');

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
            
        ################################
        ## Clean up Working Tables Log
        ################################

        drop table if exists `tmp_subject_list`;
        drop table if exists `tmp_strand_list`;
        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_test_date`;
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