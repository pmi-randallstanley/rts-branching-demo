
/*
$Rev$ 
$Author$ 
$Date$
$HeadURL$
$Id$ 
*/
 

drop procedure if exists etl_hst_load_nj_hspa//

create definer=`dbadmin`@`localhost` procedure  `etl_hst_load_nj_hspa`()
    SQL SECURITY INVOKER
    COMMENT 'zendesk ticket 19687'
proc: begin 

    declare v_school_year_id smallint(4);
    declare v_ods_table varchar(50);
    declare v_ods_view varchar(50);
    declare v_view_exists smallint(4);
    declare v_no_more_rows boolean;
    declare v_ayp_subject_id int(10);
    declare v_ayp_strand_id int(10);
    declare v_column_ss varchar(50);
    declare v_column_pe varchar(50);
    declare v_school_unassigned_id  int(10);
    declare v_grade_unassigned_id int(10);
    declare v_backfill_needed int(10);
    declare v_delete_count int(10);
    
    declare v_strand_cursor cursor for
            select ayp_subject_id
                  ,ayp_strand_id
                  ,ayp_strand_column_pe
                  ,ayp_subject_column_ss
            from tmp_strand_list;
            
    declare v_subject_cursor cursor for
            select ayp_subject_id
                  ,ayp_subject_column_ss
            from tmp_subject_list;
              
    declare continue handler for not found 
    set v_no_more_rows = true;
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    set @use_strand_raw_score := pmi_f_get_etl_setting('njhspawritingrawscore');
    

    set v_ods_table = 'pmi_ods_nj_hspa';
    set v_ods_view = 'v_pmi_ods_nj_hspa';
    
    select  count(*)
    into    v_view_exists
    from    information_schema.views as t
    where   t.table_schema = database()
            and t.table_name = v_ods_view;
    
    select  school_id
    into    v_school_unassigned_id 
    from    c_school
    where   school_code = 'unassigned';


    select  grade_level_id
    into    v_grade_unassigned_id
    from    c_grade_level
    where   grade_code = 'unassigned';

    if v_view_exists > 0 then

            
            drop table if exists `tmp_delete_key`;
            drop table if exists `tmp_stu_admin`;
            drop table if exists `tmp_subject_list`;
            drop table if exists `tmp_strand_list`;
            drop table if exists `tmp_student_year_backfill`;
            drop table if exists `tmp_school`;
           

            CREATE TABLE `tmp_delete_key` (
                `student_id` int(10) NOT NULL,
                `ayp_subject_id` int(10) NOT NULL,
                `school_year_id` smallint(4) NOT NULL,
                `month_id` tinyint(2) not null,
            PRIMARY KEY  (`student_id`,`ayp_subject_id`,`school_year_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;
           
            CREATE TABLE `tmp_stu_admin` (
              `student_code` varchar(15) NOT NULL,
              `row_num` int(10) NOT NULL,
              `student_id` int(10) NOT NULL,
              `test_month` tinyint(2) NOT NULL,
              `school_year_id` smallint(4) NOT NULL,
              `grade_code` varchar(15) default null,
              `school_code` varchar(15) default null,
              `backfill_needed_flag` tinyint(1),
            Primary KEY (`student_id`, `school_year_id`, `test_month`),
            UNIQUE KEY `uq_tmp_stu_admin` (`student_code`, `school_year_id`, `test_month` ),
            KEY `ind_tmp_stu_admin` (`row_num`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;
            
       
            CREATE TABLE `tmp_subject_list` (
                `ayp_subject_id` int(10) not null,
                `ayp_subject_column_ss` varchar(50) not null,
                 primary key (`ayp_subject_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;

            create table `tmp_strand_list` (
                `ayp_subject_id` int(10) not null,
                `ayp_strand_id` int(10) not null,
                `ayp_strand_column_pe` varchar(50) not null,
                `ayp_subject_column_ss` varchar(50) not null,
                PRIMARY KEY  (`ayp_subject_id`,`ayp_strand_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;

            create table `tmp_school` (
                `school_code` varchar(15) not null,
                `school_id` int (10) not null,
            Unique Key `ind_school_code` (`school_code`)
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
    
            
            insert  tmp_strand_list (
                ayp_subject_id
                ,ayp_strand_id
                ,ayp_strand_column_pe
                ,ayp_subject_column_ss
            )
            select  ayps.ayp_subject_id
                    ,ayp_strand_id
                    ,min(tpe.moniker) as ayp_strand_column_pe
                    ,ayp_subject_column_ss
            from v_imp_table_column_ayp_strand as ayps
            join v_imp_table as tab
                    on ayps.table_id = tab.table_id
            join v_imp_table_column as tpe 
                    on  ayps.table_id = tpe.table_id
                    and ayps.pe_column_id = tpe.column_id
                    and ayps.active_flag = 1
            join tmp_subject_list as sublist
                    on sublist.ayp_subject_id = ayps.ayp_subject_id
            where tab.target_table_name = v_ods_table
            group by ayps.ayp_subject_id
                    ,ayps.ayp_strand_id
                    ,tpe.moniker
            ;
  
   
            
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
        
            
            insert  tmp_stu_admin (                                                                                                                                                                                
                    row_num
                    ,student_code
                    ,student_id
                    ,test_month
                    ,school_year_id
                    ,grade_code
                    ,school_code
                    ,backfill_needed_flag
             )                                                                                                                                                                                                        
             
            select  ods.row_num
                    ,ods.studentid
                    ,s.student_id
                    ,coalesce(ods.test_month,0) as month_id
                    ,ods.school_year
                    ,ods.grade
                    ,ods.school_id
                    ,case when sty.school_year_id is null then 1 
                     when sty.school_year_id is not null and sty.grade_level_id = v_grade_unassigned_id then 1
                      end as backfill_needed_flag
            from    v_pmi_ods_nj_hspa as ods
            join    c_student as s
                    on    s.student_code = ods.studentid      
            left join c_student_year as sty
                    on    sty.student_id = s.student_id 
                    and   sty.school_year_id = ods.school_year
            where   ods.studentid is not null
            union all
            select  ods2.row_num
                    ,ods2.studentid
                    ,s2.student_id
                    ,coalesce(ods2.test_month,0) as month_id
                    ,ods2.school_year
                    ,ods2.grade
                    ,ods2.school_id
                    ,case when sty2.school_year_id is null then 1 
                          when sty2.school_year_id is not null and sty2.grade_level_id = v_grade_unassigned_id then 1
                      end as backfill_needed_flag
            from    v_pmi_ods_nj_hspa as ods2
            join    c_student as s2
                    on    s2.student_state_code = ods2.studentid
             left join c_student_year as sty2
                    on    sty2.student_id = s2.student_id 
                    and   sty2.school_year_id = ods2.school_year
            where   ods2.studentid is not null
            union all
            select  ods3.row_num
                    ,ods3.studentid
                    ,s3.student_id
                    ,coalesce(ods3.test_month,0) as month_id
                    ,ods3.school_year
                    ,ods3.grade
                    ,ods3.school_id
                    ,case when sty3.school_year_id is null then 1 
                          when sty3.school_year_id is not null and sty3.grade_level_id = v_grade_unassigned_id then 1
                      end as backfill_needed_flag
            from    v_pmi_ods_nj_hspa as ods3
            join    c_student as s3
                    on    s3.fid_code = ods3.studentid
            left join c_student_year as sty3
                    on    sty3.student_id = s3.student_id 
                    and   sty3.school_year_id = ods3.school_year
            where   ods3.studentid is not null
            on duplicate key update row_num = values(row_num);


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
                
 
               call etl_hst_load_backfill_stu_year();  
            
            end if;

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

        Open v_strand_cursor;
        loop_strand_cursor: loop

        Fetch v_strand_cursor 
        into v_ayp_subject_id, v_ayp_strand_id, v_column_pe,v_column_ss;

       
            if v_no_more_rows then
                close v_strand_cursor;
                leave loop_strand_cursor;
            end if;
            
                SET @sql_text := '';
                SET @sql_text := concat(@sql_text,' insert c_ayp_strand_student (ayp_subject_id, ayp_strand_id, student_id, school_year_id '
                                             ' ,month_id, ayp_score, points_earned,points_possible,last_user_id, create_timestamp) '
                                             ,' select  ', v_ayp_subject_id, ' , ', v_ayp_strand_id,' ,sadmin.student_id ,'
                                             ,'  sadmin.school_year_id ,'
                                             ,' sadmin.test_month, '
                                             ,'  if(@use_strand_raw_score=''y'' and spp.ayp_subject_code= ''hspaLangArtsLiter'' and spp.ayp_strand_code IN(''hspaLangArtsLiterWP'',''hspaLangArtsLiterWS''),' ,v_column_pe
                                             ,' ,round((cast(ods.', v_column_pe, ' as decimal(9,3))/ cast(spp.pp as decimal(9,3))) * 100)) as ayp_score, '
                                             , ' ods.', v_column_pe,  ','
                                             ,'spp.pp ,'
                                             ,' 1234 as last_user_id, '
                                             ,' now() as create_timestamp '
                                             ,' from ', v_ods_view, ' as ods '
                                             ,' join tmp_stu_admin as sadmin '
                                             ,'     on ods.row_num = sadmin.row_num '
                                             ,' join c_student_year sy'
                                             ,'      on sadmin.student_id = sy.student_id '
                                             ,'      and sadmin.school_year_id = sy.school_year_id'
                                             ,' join c_grade_level as gl'
                                             ,'       on sy.grade_level_id = gl.grade_level_id'
                                             ,' join c_ayp_strand str '
                                             ,'       on str.ayp_subject_id = ',v_ayp_subject_id
                                             ,'       and str.ayp_strand_id = ', v_ayp_strand_id 
                                             ,' JOIN  v_pmi_ods_ayp_strand_points_possible as spp '
                                             ,'      on sy.school_year_id between spp.begin_year and spp.end_year'
                                             ,'      and gl.grade_sequence between spp.begin_grade_sequence and spp.end_grade_sequence '
                                             ,'      and  spp.ayp_strand_code = str.ayp_strand_code'
                                             ,'      and  spp.pp > 0 '
                                             ,' WHERE ods.', v_column_pe, ' > 0 '
                                             ,'     and ods.', v_column_ss, ' > 0 '
                                             ,' on duplicate key update '
                                             ,' ayp_score = values(ayp_score),points_earned = values(points_earned), points_possible = values(points_possible), '
                                             ,' last_user_id = values(last_user_id) ;');

                prepare stmt from @sql_text;
                execute stmt;
                deallocate prepare stmt;
            
        end loop loop_strand_cursor;
                                            


        
        
        
        

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
    
    
    
    
    
            
    drop table if exists `tmp_delete_key`;
    drop table if exists `tmp_stu_admin`;
    drop table if exists `tmp_subject_list`;
    drop table if exists `tmp_strand_list`;
    drop table if exists `tmp_student_year_backfill`;
    drop table if exists `tmp_school`;
   
    
    
    
    set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', v_ods_table, '\', \'P\', \'ETL Load Successful\')');

    prepare sql_scan_log from @sql_scan_log;
    execute sql_scan_log;
    deallocate prepare sql_scan_log;
           
    end if;

end proc;
//

