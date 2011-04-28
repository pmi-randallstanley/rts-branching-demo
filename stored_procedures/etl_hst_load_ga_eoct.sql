/*
$Rev: 9335 $ 
$Author: randall.stanley $ 
$Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_hst_load_ga_eoct.sql $
$Id: etl_hst_load_ga_eoct.sql 9335 2010-10-03 18:10:23Z randall.stanley $ 
*/


drop procedure if exists etl_hst_load_ga_eoct//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_ga_eoct()
contains sql
sql security invoker
comment '$Rev:  $'

proc: begin 

    declare v_ods_table varchar(50);
    declare v_ods_view varchar(50);
    declare v_view_exists smallint(4);
    declare v_no_more_rows boolean;
    declare v_ayp_subject_id int(10); 
    declare v_ayp_strand_id int(10); 
    declare v_column_pe varchar(50);
    declare v_column_pp varchar(50);
    
    declare v_school_unassigned_id  int(10);
    declare v_grade_unassigned_id int(10);
    declare v_backfill_needed int(10);
    declare v_delete_count int(10);
    
    declare v_strand_cursor cursor for
            select ayp_subject_id
                  ,ayp_strand_id
                  ,column_pe
                  ,column_pp
            from tmp_strand_list;

    declare continue handler for not found 
    set v_no_more_rows = true;
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    set v_ods_table = 'pmi_ods_ga_eoct';
    set v_ods_view = 'v_pmi_ods_ga_eoct';
    
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
            drop table if exists `tmp_test_date`;
            drop table if exists `tmp_student_year_backfill`;
            drop table if exists `tmp_school`;
          
            CREATE TABLE `tmp_subject_list` (
              `ayp_subject_id` int(10) NOT NULL,
              `client_ayp_subject_code` varchar(15) NOT NULL,
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
            Unique Key `ind_school_code` (`school_code`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;
            
            CREATE TABLE `tmp_stu_admin` (
              `student_code` varchar(15) NOT NULL,
              `client_ayp_subject_code` varchar(15) NOT NULL,
              `ods_test_year` varchar(10) NOT NULL,
              `row_num` int(10) NOT NULL,
              `student_id` int(10) NOT NULL,
              `ayp_subject_id` int(10) NOT NULL,
              `test_month` tinyint(2) NOT NULL,
              `school_year_id` smallint(4) NOT NULL,
              `ayp_score` decimal(9,3) NULL,
              `alt_ayp_score` decimal(9,3) NULL,
              `grade_code` varchar(15) default null,
              `school_code` varchar(15) default null,
              `backfill_needed_flag` tinyint(1),
            Primary KEY (`student_id`, `ayp_subject_id`, `school_year_id`, `test_month`),
            UNIQUE KEY `uq_tmp_stu_admin` (`student_code`, `client_ayp_subject_code`,`ods_test_year`, `test_month`),
            KEY `ind_tmp_stu_admin` (`row_num`,`ayp_subject_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;
            
            CREATE TABLE  `tmp_date_conversion` (
                `test_date` datetime not null,
                `mm` tinyint(2) not null,
                `yyyy` smallint(4) not null,
            unique key `tmp_fcat_date_conversion_test_date` (`test_date`),
            key `tmp_fcat_date_conversion_year_mm_dd` (`yyyy`, `mm`)
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
    
                #########################
                ## Load Working Tables ##
                #########################

            ### Populate tmp tables
        
       
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
            where   tt.moniker = 'eoct'
            ;
            
            # tmp_strand_list
            insert  tmp_strand_list (
                ayp_subject_id
                ,ayp_strand_id
                ,column_pe
                ,column_pp
            )
            select 
                    ayps.ayp_subject_id
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
    
            # Choose a date in the month - by default using the 15th
            insert into tmp_date_conversion (
                        mm
                        ,yyyy
                        ,test_date
            )
            select      mm
                        ,yyyy
                       ,max(str_to_date(concat(mm,'/15/',yyyy), '%m/%d/%Y'))
            from        v_pmi_ods_ga_eoct as ods
            group by    mm
                        ,yyyy
            ;
                     

            #Stu admin
            insert  tmp_stu_admin (
                row_num
                ,student_code
                ,client_ayp_subject_code
                ,ods_test_year
                ,student_id
                ,ayp_subject_id
                ,test_month
                ,school_year_id
                ,ayp_score
                ,alt_ayp_score
                ,grade_code
                ,school_code
                ,backfill_needed_flag
            )
            select  ods.row_num
                    ,ods.gtid
                    ,ods.section
                    ,ods.yyyy
                    ,s.student_id
                    ,sub.ayp_subject_id
                    ,month(c_date.test_date)
                    ,sy.school_year_id
                    ,CAST(ods.ss as decimal(9,3))
                    ,CAST(ods.grade_conversion as decimal(9,3))
                    ,v_grade_unassigned_id  #there is no grade code in the file
                    ,ods.sch_code
                    ,case when sty.school_year_id is null then 1
                          when sty.school_year_id is not null and sty.grade_level_id = v_grade_unassigned_id then 1 
                      end as backfill_needed_flag
            from    v_pmi_ods_ga_eoct as ods
            join    tmp_date_conversion as c_date
                    on    ods.yyyy = c_date.yyyy
                    and   ods.mm = c_date.mm
            join    c_student s
                    on    s.student_state_code = ods.gtid
            join    c_school_year as sy 
                    on    c_date.test_date BETWEEN sy.begin_date AND sy.end_date
            join    tmp_subject_list as sub
                    on    ods.section = sub.client_ayp_subject_code
            left join   c_student_year as sty
                    on    sty.student_id = s.student_id 
                    and   sty.school_year_id = sy.school_year_id
            where   ods.gtid is not null
              and   ods.ss  REGEXP '^[0-9]' > 0
            union all
            select  ods2.row_num
                    ,ods2.ssn
                    ,ods2.section
                    ,ods2.yyyy
                    ,s2.student_id
                    ,sub2.ayp_subject_id
                    ,month(c_date2.test_date)
                    ,sy2.school_year_id
                    ,CAST(ods2.ss as decimal(9,3))
                    ,CAST(ods2.grade_conversion as decimal(9,3))
                    ,v_grade_unassigned_id  #there is no grade code in the file
                    ,ods2.sch_code
                    ,case when sty2.school_year_id is null then 1
                          when sty2.school_year_id is not null and sty2.grade_level_id = v_grade_unassigned_id then 1 
                      end as backfill_needed_flag
            from    v_pmi_ods_ga_eoct as ods2
            join    tmp_date_conversion as c_date2
                    on    ods2.yyyy = c_date2.yyyy
                    and   ods2.mm = c_date2.mm
            join    c_student s2
                    on    s2.fid_code = ods2.ssn
            join    c_school_year as sy2
                    on    c_date2.test_date BETWEEN sy2.begin_date AND sy2.end_date
            join    tmp_subject_list as sub2
                    on    ods2.section = sub2.client_ayp_subject_code
            left join  c_student_year as sty2
                    on    sty2.student_id = s2.student_id 
                    and   sty2.school_year_id = sy2.school_year_id
            where   ods2.ssn is not null
            and     ods2.ss  REGEXP '^[0-9]' > 0
            union all
            select  ods3.row_num
                    ,ods3.ssn
                    ,ods3.section
                    ,ods3.yyyy
                    ,s3.student_id
                    ,sub3.ayp_subject_id
                    ,month(c_date3.test_date)
                    ,sy3.school_year_id
                    ,CAST(ods3.ss as decimal(9,3))
                    ,CAST(ods3.grade_conversion as decimal(9,3))
                    ,v_grade_unassigned_id
                    ,ods3.sch_code
                    ,case when sty3.school_year_id is null then 1
                          when sty3.school_year_id is not null and sty3.grade_level_id = v_grade_unassigned_id then 1 
                      end as backfill_needed_flag
            from    v_pmi_ods_ga_eoct as ods3
            join    tmp_date_conversion as c_date3
                    on    ods3.yyyy = c_date3.yyyy
                    and   ods3.mm = c_date3.mm
            join    c_student s3
                    on    s3.student_state_code = ods3.ssn
            join    c_school_year as sy3
                    on    c_date3.test_date BETWEEN sy3.begin_date AND sy3.end_date
            join    tmp_subject_list as sub3
                    on    ods3.section = sub3.client_ayp_subject_code
            left join   c_student_year as sty3
                    on    sty3.student_id = s3.student_id 
                    and   sty3.school_year_id = sy3.school_year_id
            where   ods3.ssn is not null
            and     ods3.ss  REGEXP '^[0-9]' > 0
                
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
            
            ########################
            ## Load c_ayp_subject 
            ########################
    
            insert c_ayp_subject_student (
                student_id
                ,ayp_subject_id
                ,school_year_id
                ,month_id
                ,alt_ayp_score
                ,ayp_score
                ,score_type_code
                ,last_user_id
                ,create_timestamp
            )
            select  tsa.student_id
                    ,tsa.ayp_subject_id
                    ,tsa.school_year_id
                    ,tsa.test_month
                    ,tsa.alt_ayp_score
                    ,tsa.ayp_score
                    ,'n'
                  ,1234
                    ,now()
            from    tmp_stu_admin tsa
            on duplicate key update last_user_id = values(last_user_id)
                                    ,ayp_score = values(ayp_score)
                                    ,alt_ayp_score = values(alt_ayp_score)
            ;
        

            ########################
            ## Load c_ayp_strand 
            ########################
        
            open v_strand_cursor;
            loop_strand_cursor: loop
            FETCH v_strand_cursor 
            INTO  v_ayp_subject_id, v_ayp_strand_id, v_column_pe, v_column_pp;
    
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
                                                       ,'           and sadmin.ayp_subject_id = ', v_ayp_subject_id
                                                       ,' where ods.', v_column_pe,  ' REGEXP \'^[0-9]\' > 0 '
                                                       ,' and   ods.', v_column_pp,  ' REGEXP \'^[0-9]\' > 0 '                                                       
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
                
            drop table if exists `tmp_date_conversion`;
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