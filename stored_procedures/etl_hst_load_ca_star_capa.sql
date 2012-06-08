
drop procedure if exists etl_hst_load_ca_star_capa//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_ca_star_capa()
contains sql
sql security invoker
comment 'zendesk ticket 19687'

proc: begin

    declare v_no_more_rows boolean;
    declare v_ayp_subject_id int(11);
    declare v_ayp_strand_id int(11);
    declare v_column_pe varchar(50);
    declare v_column_pp varchar(50);
    declare v_column_ss varchar(50);
    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_school_year_id smallint(4);
    declare v_view_exists int(10);
    declare v_school_unassigned_id  int(10);
    declare v_grade_unassigned_id int(10);
    declare v_backfill_needed int(10);
    declare v_delete_count int(10);
    declare v_star_test_type_capa varchar(2);
    
    declare v_subject_cursor cursor for
            select ayp_subject_id
                  ,column_ss
            from tmp_subject_score_moniker;
              
    declare continue handler for not found 
    set v_no_more_rows = true;
              

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @p_db_name_dw);

    set v_ods_table := 'pmi_ods_ca_star';
    set v_ods_view := 'v_pmi_ods_ca_star';
    # CAPA Records are designated by test type = '02'
    set v_star_test_type_capa := '02';

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

        drop table if exists `tmp_subject_score_moniker`;
        drop table if exists `tmp_subject_list`;
        drop table if exists `tmp_strand_list`;
        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_test_date`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_school`;
        
        CREATE TABLE `tmp_subject_score_moniker` (
                `ayp_subject_id` int(10) not null,
                `column_ss` varchar(50) not null,
                 primary key (`ayp_subject_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        CREATE TABLE `tmp_subject_list` (
          `ayp_subject_id` int(10) NOT NULL,
          `client_ayp_subject_code` varchar(2) NOT NULL,
          `ayp_subject_code` varchar(50) NOT NULL,
          PRIMARY KEY  (`ayp_subject_id`)#,
          #UNIQUE KEY `uq_tmp_subject_list` (`client_ayp_subject_code`)
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
          `ods_test_date` varchar(10) NOT NULL,
          `row_num` int(10) NOT NULL,
          `student_id` int(10) NOT NULL,
          `ayp_subject_id` int(10) NOT NULL,
          `test_month` tinyint(2) NOT NULL,
          `school_year_id` smallint(4) NOT NULL,
          `grade_code` varchar(15) default null,
          `school_code` varchar(15) default null,
          `backfill_needed_flag` tinyint(1),
          PRIMARY KEY (`student_id`, `ayp_subject_id`, `school_year_id`, `test_month`),
          #UNIQUE KEY `uq_tmp_stu_admin` (`student_code`, `client_ayp_subject_code`,`ods_test_date`),
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
            ,max(str_to_date(ods.test_date, '%m%d%Y')) as test_date
        from v_pmi_ods_ca_star as ods
        where ods.star_test_type = v_star_test_type_capa
        group by    ods.test_date
        ;
        
        # subject column metadata
        insert  tmp_subject_score_moniker (
            ayp_subject_id
            ,column_ss
        )
        select  sub.ayp_subject_id
                ,sco.moniker
        from    v_imp_table_column_ayp_subject as ayp
        join    c_ayp_subject as sub
                on  ayp.ayp_subject_id = sub.ayp_subject_id
        join    c_ayp_test_type as tt
                on sub.ayp_test_type_id = tt.ayp_test_type_id
                and tt.moniker = 'CAPA'
        join    v_imp_table as tab
                on ayp.table_id = tab.table_id
                and tab.target_table_name = v_ods_table
        join    v_imp_table_column as sco
                on ayp.table_id = sco.table_id
                and ayp.score_column_id = sco.column_id
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
        where   tt.moniker = 'CAPA'
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
            ,coalesce(ods.state_student_code, ods.sis_student_code)
            ,ods.test_date
            ,s.student_id
            ,sub.ayp_subject_id
            ,month(dt.test_date)
            ,sy.school_year_id
            ,ods.grade_code
            ,ods.state_school_code
            ,case when syr.school_year_id is null then 1
              when syr.school_year_id is not null and syr.grade_level_id = v_grade_unassigned_id then 1
              end as backfill_needed_flag
        from    v_pmi_ods_ca_star as ods
        join    tmp_test_date dt
                on      ods.test_date = dt.ods_test_date
        join    c_student as s
                on      s.student_state_code = ods.state_student_code
                or      s.student_code = ods.sis_student_code
        join    c_school_year as sy
                on     dt.test_date between sy.begin_date and sy.end_date
        join    tmp_subject_list as sub
                on      ods.capa_level = sub.client_ayp_subject_code
        left join    c_student_year as syr
                on syr.student_id = s.student_id
                and syr.school_year_id = sy.school_year_id
        where   (ods.state_student_code is not null or ods.sis_student_code is not null)
          and   ods.star_test_type = v_star_test_type_capa
        on duplicate key update row_num = values(row_num)
        ;


        ##########################################
        ## Backfill for c_student_year 
        ## Need to detect and load c_student_year 
        ## records when supporting ones don''t exist
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


        ########################
        ## Load Target Tables ##
        ########################
        # No Strand Data for CAPA.  Only subject level data
    
        Open v_subject_cursor;
        loop_subject_cursor: loop

        Fetch v_subject_cursor 
        into v_ayp_subject_id,  v_column_ss;
        
            if v_no_more_rows then
                close v_subject_cursor;
                leave loop_subject_cursor;
            end if;


            SET @sql_text := '';
            SET @sql_text := concat(@sql_text,  ' insert c_ayp_subject_student ( student_id, ayp_subject_id, school_year_id '
                                               ,' ,month_id, ayp_score, score_type_code, last_user_id, create_timestamp ) '
                                               ,' select sadmin.student_id, '
                                               ,        v_ayp_subject_id
                                               ,'       , sadmin.school_year_id '
                                               ,'       , sadmin.test_month '
                                               ,'       ,cast(ods.', v_column_ss, ' as decimal(9,3)) '
                                               ,'       ,\'n\' '
                                               ,'       , 1234 '
                                               ,'       , now() '
                                               ,' from tmp_stu_admin as sadmin '
                                               ,' join ', v_ods_view, ' as ods '
                                               ,'       on sadmin.row_num = ods.row_num '
                                               ,'       and sadmin.ayp_subject_id = ', v_ayp_subject_id
                                               ,' where ', v_column_ss, ' REGEXP \'^[0-9]\' > 0 '
                                               ,' on duplicate key update last_user_id = values(last_user_id) '
                                               ,'                         ,ayp_score = values(ayp_score) ;');

            
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

        drop table if exists `tmp_subject_score_moniker`;
        drop table if exists `tmp_subject_list`;
        drop table if exists `tmp_strand_list`;
        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_test_date`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_school`;

        #################
        ## Update Log
        #################

        /*  This call happens in etl_hst_load_ca_star()
        set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', v_ods_table, '\', \'P\', \'ETL Load Successful\')');

        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;
        */
      

    end if;


end proc;
//