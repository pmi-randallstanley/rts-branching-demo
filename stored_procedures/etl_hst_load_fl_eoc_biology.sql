

drop procedure if exists etl_hst_load_fl_eoc_biology//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_fl_eoc_biology()
contains sql
sql security invoker
comment 'zendesk ticket 19687'

proc: begin 

    declare v_ods_table varchar(50);
    declare v_ods_view varchar(50);
    declare v_view_exists smallint(4);
    declare v_no_more_rows boolean;
    declare v_ayp_subject_id int(10); 
    declare v_ayp_strand_id int(10); 
    declare v_column_pe varchar(50);
    declare v_column_pp varchar(50);
    declare v_column_ss varchar(50);
    
    declare v_school_unassigned_id  int(10);
    declare v_grade_unassigned_id int(10);
    declare v_backfill_needed int(10);
    declare v_delete_count int(10);
    
    declare v_strand_cursor cursor for
            select ayp_subject_id
                  ,ayp_strand_id
                  ,column_pe
                  ,column_pp
                  ,subject_column_ss
            from tmp_strand_list;
            
  declare v_subject_cursor cursor for
            select ayp_subject_id
                  ,ayp_subject_column_ss
            from tmp_subject_list;

    declare continue handler for not found 
    set v_no_more_rows = true;
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @p_db_name_dw);

    set v_ods_table = 'pmi_ods_fl_eoc_biology';
    set v_ods_view = 'v_pmi_ods_fl_eoc_biology';
    
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

            ###########################
            ## Create Working Tables ##
            ###########################
    
            drop table if exists tmp_date_conversion;
            drop table if exists `tmp_subject_list`;
            drop table if exists `tmp_strand_list`;
            drop table if exists `tmp_stu_admin`;
            drop table if exists `tmp_student_year_backfill`;
            drop table if exists `tmp_school`;
            drop table if exists `tmp_delete_key`;
            
              CREATE TABLE `tmp_delete_key` (
                `student_id` int(10) NOT NULL,
                `ayp_subject_id` int(10) NOT NULL,
                `school_year_id` smallint(4) NOT NULL,
                `month_id` tinyint(2) not null,
            PRIMARY KEY  (`student_id`,`ayp_subject_id`,`school_year_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;
          
             CREATE TABLE `tmp_subject_list` (
                `ayp_subject_id` int(10) not null,
                `ayp_subject_column_ss` varchar(50) not null,
                 primary key (`ayp_subject_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;
            
            CREATE TABLE `tmp_strand_list` (
              `ayp_subject_id` int(10) NOT NULL, 
              `ayp_strand_id` int(10) NOT NULL,
              `column_pe` varchar(50) NOT NULL,
              `column_pp` varchar(50) NOT NULL,
              `subject_column_ss` varchar(50) NOT NULL,
              PRIMARY KEY  (`ayp_subject_id`,`ayp_strand_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;
            
            CREATE TABLE `tmp_school` (
                `school_code` varchar(15) not null,
                `school_id` int (10) not null,
            Unique Key `ind_school_code` (`school_code`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;
            
            CREATE TABLE `tmp_stu_admin` (
              `student_code` varchar(15) NOT NULL,
              `row_num` int(10) NOT NULL,
              `student_id` int(10) NOT NULL,
              `test_mo_year` varchar(10) not null,
              `test_month` tinyint(2) NOT NULL,
              `school_year_id` smallint(4) NOT NULL,
              `grade_code` varchar(15) default null,
              `school_code` varchar(15) default null,
              `backfill_needed_flag` tinyint(1),
            Primary KEY (`student_id`, `school_year_id`, `test_month`),
            UNIQUE KEY `uq_tmp_stu_admin` (`student_code`, `school_year_id`, `test_mo_year` ),
            KEY `ind_tmp_stu_admin` (`row_num`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;
            
            
            create table `tmp_date_conversion` (
          `test_mo_year` varchar(10) not null,
          `test_month` tinyint(2) not null,
          `test_date` date not null,
          unique key `uq_tmp_eoc_date_conversion` (`test_mo_year`)
          ) engine=innodb default charset=latin1
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
    
                #########################
                ## Load Working Tables ##
                #########################

            ### Populate tmp tables
        
       
             # subject column metadata
            insert  tmp_subject_list (
                ayp_subject_id
                ,ayp_subject_column_ss
            )
            select  sub.ayp_subject_id
                    ,sco.moniker
            from    v_imp_table_column_ayp_subject as ayp
            join    c_ayp_subject as sub
                    on  ayp.ayp_subject_id = sub.ayp_subject_id
            join    v_imp_table as tab
                    on ayp.table_id = tab.table_id
                    and tab.target_table_name = v_ods_table
            join    v_imp_table_column as sco
                    on ayp.table_id = sco.table_id
                    and ayp.score_column_id = sco.column_id
            ;
            
            # tmp_strand_list
            insert  tmp_strand_list (
                ayp_subject_id
                ,ayp_strand_id
                ,column_pe
                ,column_pp
                ,subject_column_ss
            )
            select 
                    ayps.ayp_subject_id
                    ,ayps.ayp_strand_id
                    ,min(pe.moniker) as column_pe
                    ,min(pp.moniker)  as column_pp
                    ,sublist.ayp_subject_column_ss
            from v_imp_table_column_ayp_strand as ayps
            join v_imp_table as tab
                    on ayps.table_id = tab.table_id
            join v_imp_table_column as pe
                    on ayps.table_id = pe.table_id
                    and ayps.pe_column_id = pe.column_id
            join v_imp_table_column as pp
                    on ayps.table_id = pp.table_id
                    and ayps.pp_column_id = pp.column_id
            join tmp_subject_list as sublist
                    on sublist.ayp_subject_id = ayps.ayp_subject_id
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
    
            insert into tmp_date_conversion (
            test_mo_year
            ,test_month
            ,test_date
        )

        select  ods.test_mo_year
            ,min(month(str_to_date(ods.test_mo_year, '%m%Y'))) as test_month
            ,min(str_to_date(concat('15', ods.test_mo_year), '%d%m%Y')) as test_date
        from    v_pmi_ods_fl_eoc_biology as ods
        group by    ods.test_mo_year
        ;
                     
           
            # stu admin data
            insert  tmp_stu_admin (
                    row_num
                    ,student_code
                    ,student_id
                    ,test_mo_year
                    ,test_month
                    ,school_year_id
                    ,grade_code
                    ,school_code
                    ,backfill_needed_flag
             )
            select  ods.row_num
                    ,ods.student_id
                    ,s.student_id
                    ,ods.test_mo_year
                    ,coalesce(dte.test_month,0) as month_id
                    ,sy.school_year_id
                    ,ods.grade_code
                    ,ods.state_school_code
                    ,case when sty.school_year_id is null then 1
                          when sty.school_year_id is not null and sty.grade_level_id = v_grade_unassigned_id then 1
                     end as backfill_needed_flag
            from    v_pmi_ods_fl_eoc_biology as ods
            join    c_student as s
                    on    s.student_code = ods.student_id
            join    tmp_date_conversion as dte
                on      ods.test_mo_year = dte.test_mo_year
            join    c_school_year as sy 
                on      dte.test_date between sy.begin_date and sy.end_date   
            left join c_student_year as sty
                    on    sty.student_id = s.student_id 
                    and   sty.school_year_id = sy.school_year_id
            where   ods.student_id is not null
            union all
            select  ods2.row_num
                    ,ods2.student_id
                    ,s2.student_id
                    ,ods2.test_mo_year
                    ,coalesce(dte2.test_month,0) as month_id
                    ,sy2.school_year_id
                    ,ods2.grade_code
                    ,ods2.state_school_code
                    ,case when sty2.school_year_id is null then 1
                          when sty2.school_year_id is not null and sty2.grade_level_id = v_grade_unassigned_id then 1
                     end as backfill_needed_flag
            from    v_pmi_ods_fl_eoc_biology as ods2
            join    c_student as s2
                    on    s2.student_state_code = ods2.student_id
            join    tmp_date_conversion as dte2
                on      ods2.test_mo_year = dte2.test_mo_year
            join    c_school_year as sy2 
                on      dte2.test_date between sy2.begin_date and sy2.end_date
            left join c_student_year as sty2
                    on    sty2.student_id = s2.student_id 
                    and   sty2.school_year_id = sy2.school_year_id
            where   ods2.student_id is not null
            union all
            select  ods3.row_num
                    ,ods3.student_id
                    ,s3.student_id
                    ,ods3.test_mo_year
                    ,coalesce(dte3.test_month,0) as month_id
                    ,sy3.school_year_id
                    ,ods3.grade_code
                    ,ods3.state_school_code
                    ,case when sty3.school_year_id is null then 1
                          when sty3.school_year_id is not null and sty3.grade_level_id = v_grade_unassigned_id then 1
                     end as backfill_needed_flag
            from    v_pmi_ods_fl_eoc_biology as ods3
            join    c_student as s3
                    on    s3.fid_code = ods3.student_id
            join    tmp_date_conversion as dte3
                on      ods3.test_mo_year = dte3.test_mo_year
            join    c_school_year as sy3 
                on      dte3.test_date between sy3.begin_date and sy3.end_date
            left join c_student_year as sty3
                    on    sty3.student_id = s3.student_id 
                    and   sty3.school_year_id = sy3.school_year_id
            where   ods3.student_id is not null
            on duplicate key update row_num = values(row_num);

        
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
            
          
        ########################
        ## Load Target Tables ##
        ########################
    
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
                                               ,' where ', v_column_ss, ' REGEXP \'^[0-9]\' > 0 '
                                               ,' on duplicate key update last_user_id = values(last_user_id) '
                                               ,'                         ,ayp_score = values(ayp_score) ;');

            prepare stmt from @sql_text;
            execute stmt;
            deallocate prepare stmt;
            
            
            SET @sql_text := '';
            SET @sql_text := concat(@sql_text,  ' insert tmp_delete_key ( student_id, ayp_subject_id, school_year_id, month_id) '
                                               ,' select sadmin.student_id '
                                               ,'         , ', v_ayp_subject_id 
                                               ,'         , sadmin.school_year_id '
                                               ,'         , sadmin.test_month as month_id '
                                               ,' from tmp_stu_admin as sadmin '
                                               ,' join ', v_ods_view, ' as ods '
                                               ,'        on sadmin.row_num = ods.row_num '
                                               ,' where ', v_column_ss, ' REGEXP \'^[0-9]\' > 0 '
                                               ,' on duplicate key update month_id = values(month_id) ;');
            prepare stmt from @sql_text;
            execute stmt;
            deallocate prepare stmt;
            
        end loop loop_subject_cursor;

        set v_no_more_rows = false;
        
            ########################
            ## Load c_ayp_strand 
            ########################
        
            open v_strand_cursor;
            loop_strand_cursor: loop
            FETCH v_strand_cursor 
            INTO  v_ayp_subject_id, v_ayp_strand_id, v_column_pe, v_column_pp,v_column_ss;
    
                if v_no_more_rows then
                    close v_strand_cursor;
                    leave loop_strand_cursor;
                end if;

                    set @sql_text := '';
                    set @sql_text := concat(@sql_text,  ' insert c_ayp_strand_student ( '
                                                       ,'       student_id, ayp_subject_id, ayp_strand_id, school_year_id ,month_id '
                                                       ,'       ,ayp_score, points_earned, points_possible, last_user_id, create_timestamp '
                                                       ,') '
                                                       ,' select  sadmin.student_id '
                                                       ,'         ,', v_ayp_subject_id
                                                       ,'         ,',v_ayp_strand_id 
                                                       ,'         ,sadmin.school_year_id  '
                                                       ,'         ,sadmin.test_month '
                                                       ,'         ,round((cast(ods.', v_column_pe, ' as decimal(9,3))/cast(ods.', v_column_pp, ' as decimal(9,3))) * 100, 3) as ayp_score '
                                                       ,'         ,cast(ods.', v_column_pe, ' as decimal(9,3)) as points_earned '
                                                       ,'         ,cast(ods.', v_column_pp, ' as decimal(9,3)) as points_possible'
                                                       ,'         ,1234 '
                                                       ,'         ,now()'
                                                       ,' from    ',v_ods_view ,' as ods '
                                                       ,' join tmp_stu_admin as sadmin '
                                                       ,'           on ods.row_num = sadmin.row_num '
                                                       #,'           and sadmin.ayp_subject_id = ', v_ayp_subject_id
                                                       ,' where ods.', v_column_pe,  ' REGEXP \'^[0-9]\' > 0 '
                                                       ,' and   ods.', v_column_pp,  ' REGEXP \'^[0-9]\' > 0 '
                                                       ,' and   ods.', v_column_ss,  ' REGEXP \'^[0-9]\' > 0 '                                                      
                                                       ,' ON DUPLICATE KEY UPDATE '
                                                       ,'         last_user_id = values(last_user_id) '
                                                       ,'         , ayp_score = values(ayp_score) '
                                                       ,'         , points_earned = values(points_earned) '
                                                       ,'         , points_possible = values(points_possible);'); 
                                                            
                    
   
                    
                    prepare stmt from @sql_text;
                    execute stmt;
                    deallocate prepare stmt;
                                                            
            end loop loop_strand_cursor;

        #######################################################################################
        ## Delete records that exist in source with 0 month_id (single admin) 
        ## based on student, school_year and subject
        #######################################################################################

        select  count(*)
        into    v_delete_count
        from    tmp_delete_key as tmp1
        join    c_ayp_subject_student as ss
                on      tmp1.student_id = ss.student_id
                and     tmp1.ayp_subject_id = ss.ayp_subject_id
                and     tmp1.school_year_id = ss.school_year_id
                and     ss.month_id = 0
        where   tmp1.month_id != 0
        ;
        
        if v_delete_count > 0 then

            delete ayp_str.*
            from tmp_delete_key as tmp1
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
            from    tmp_delete_key as tmp1
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
                
            drop table if exists `tmp_date_conversion`;
            drop table if exists `tmp_subject_list`;
            drop table if exists `tmp_strand_list`;
            drop table if exists `tmp_stu_admin`;
            drop table if exists `tmp_student_year_backfill`;
            drop table if exists `tmp_school`;
            drop table if exists `tmp_delete_key` ;
        
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
 