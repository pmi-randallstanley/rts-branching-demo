drop procedure if exists etl_hst_load_nj_sgp//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_nj_sgp()
contains sql
sql security invoker
comment '$Rev$ $Date$'

proc: begin 

    declare v_school_year_id smallint(4);
    declare v_ods_table varchar(50);
    declare v_ods_view varchar(50);
    declare v_view_exists smallint(4);
    declare v_ayp_subject_id int(10);
    declare v_column_ss varchar(50);
    declare v_column_pe varchar(50);
    declare v_school_unassigned_id  int(10);
    declare v_grade_unassigned_id int(10);
    declare v_backfill_needed int(10);

    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_nj_sgp';
    set v_ods_view = 'v_pmi_ods_nj_sgp';
    
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

            ###########################
            ## Create Working Tables ##
            ###########################
    
            drop table if exists `tmp_stu_admin`;
            drop table if exists `tmp_subject_list`;
            drop table if exists `tmp_student_year_backfill`;
            drop table if exists `tmp_school`;
            drop table if exists `tmp_date_conversion`;
            drop table if exists `tmp_subject_list`;


            CREATE TABLE `tmp_subject_list` (
              `ayp_subject_id` int(10) NOT NULL,
              `client_ayp_subject_code` varchar(10) NOT NULL,
              `ayp_subject_code` varchar(50) NOT NULL,
              PRIMARY KEY  (`ayp_subject_id`),
              UNIQUE KEY `uq_tmp_subject_list` (`client_ayp_subject_code`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;
           
            CREATE TABLE `tmp_stu_admin` (
              `student_code` varchar(15) NOT NULL,
              `row_num` int(10) NOT NULL,
              `student_id` int(10) NOT NULL,
              `test_year` varchar(10) not null,
              `test_month` tinyint(2) NOT NULL,
              `school_year_id` smallint(4) NOT NULL,
              `ayp_subject_id` int(10) not null,
              `grade_code` varchar(15) default null,
              `school_code` varchar(15) default null,
              `backfill_needed_flag` tinyint(1),
            Primary KEY (`student_id`, `school_year_id`, `ayp_subject_id`, `test_month`),
            KEY `ind_tmp_stu_admin` (`row_num`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;
            
            create table `tmp_date_conversion` (
              `test_year` varchar(4) not null,
              `test_month` tinyint(2) not null,
              `test_date` date not null,
              Primary key  (`test_year`,`test_month`)
            ) engine=innodb default charset=latin1
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
            
           
            
            #########################
            ## Load Working Tables ##
            #########################
            
             
            insert into tmp_date_conversion (
            test_year
            ,test_month
            ,test_date
            )
    
            select  ods.test_year
                ,ods.test_month
                ,min(str_to_date(concat(ods.test_month,'/15/', ods.test_year), '%m/%d/%Y')) as test_date
            from    v_pmi_ods_nj_sgp as ods
            group by    ods.test_year, ods.test_month
            ;
            
        
            ## Hardcoding this data b/c sgp shares same test type as ask
            ##   Don't need to do this, see hsa load proc
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
            where   tt.moniker = 'ask'
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
        
            # stu admin data
            insert  tmp_stu_admin (                                                                                                                                                                                
                    row_num
                    ,student_code
                    ,student_id
                    ,test_year
                    ,test_month
                    ,school_year_id
                    ,ayp_subject_id
                    ,grade_code
                    ,school_code
                    ,backfill_needed_flag
             )                                                                                                                                                                                                        
             
            select  ods.row_num
                    ,ods.local_student_id
                    ,s.student_id
                    ,ods.test_year
                    ,ods.test_month
                    ,sy.school_year_id
                    ,sub.ayp_subject_id
                    ,ods.grade_code
                    ,ods.school_code
                    ,case when sty.school_year_id is null then 1 
                          when sty.school_year_id is not null and sty.grade_level_id = v_grade_unassigned_id then 1
                     end as backfill_needed_flag
            from    v_pmi_ods_nj_sgp as ods
            join    c_student as s
                    on    s.student_code = ods.local_student_id    
            join    tmp_date_conversion as dte
                    on      ods.test_month = dte.test_month
                    and     ods.test_year = dte.test_year
            join    c_school_year as sy 
                    on      dte.test_date between sy.begin_date and sy.end_date   
            join    tmp_subject_list sub
                    on      ods.subject_code = sub.client_ayp_subject_code
            left join c_student_year as sty
                    on    sty.student_id = s.student_id 
                    and   sty.school_year_id = sy.school_year_id
            where   ods.local_student_id is not null
            union all
            select  ods2.row_num
                    ,ods2.state_student_id
                    ,s2.student_id
                    ,ods2.test_year
                    ,ods2.test_month
                    ,sy2.school_year_id
                    ,sub2.ayp_subject_id
                    ,ods2.grade_code
                    ,ods2.school_code
                    ,case when sty2.school_year_id is null then 1 
                          when sty2.school_year_id is not null and sty2.grade_level_id = v_grade_unassigned_id then 1
                     end as backfill_needed_flag
            from    v_pmi_ods_nj_sgp as ods2
            join    c_student as s2
                    on    s2.student_code = ods2.state_student_id    
            join    tmp_date_conversion as dte2
                    on      ods2.test_month = dte2.test_month
                    and     ods2.test_year = dte2.test_year
            join    c_school_year as sy2
                    on      dte2.test_date between sy2.begin_date and sy2.end_date   
            join    tmp_subject_list sub2
                    on      ods2.subject_code = sub2.client_ayp_subject_code
            left join c_student_year as sty2
                    on    sty2.student_id = s2.student_id 
                    and   sty2.school_year_id = sy2.school_year_id
            where   ods2.state_student_id is not null
            on duplicate key update row_num = values(row_num)
            ;

            ##########################################
            ## Backfill for c_student_year 
            ## Need to detect and load c_student_year 
            ## records when supporting ones does not exist
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
  
            insert into c_ayp_subject_student (student_id, ayp_subject_id, school_year_id, month_id, ayp_score, score_type_code, last_user_id, create_timestamp)
            select  sadmin.student_id
                    ,sadmin.ayp_subject_id
                    ,sadmin.school_year_id
                    ,sadmin.test_month
                    ,ods.growth_percentile
                    ,'n'
                    ,1234
                    ,now()
            from    tmp_stu_admin sadmin
            join    v_pmi_ods_nj_sgp ods
                    on    sadmin.row_num = ods.row_num
            where   ods.growth_percentile REGEXP '^[0-9]' > 0
            on duplicate key update last_user_id = values (last_user_id),
                                    ayp_score = ods.growth_percentile
            ;

      

    #########################
    ## clean-up tmp tables ##
    #########################
    
            
    drop table if exists `tmp_stu_admin`;
    drop table if exists `tmp_subject_list`;
    drop table if exists `tmp_student_year_backfill`;
    drop table if exists `tmp_school`;
    drop table if exists `tmp_date_conversion`;
    drop table if exists `tmp_subject_list`;
    
    
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

