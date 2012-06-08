      
drop procedure if exists etl_hst_load_oh_ogt_student_plus//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_oh_ogt_student_plus()
contains sql
sql security invoker
comment 'zendesk ticket 19687'

proc: begin 
  
    declare v_no_more_rows boolean;
    declare v_ods_table varchar(40);
    declare v_ods_view varchar(40);
    declare v_view_exists smallint(4);
    declare v_school_unassigned_id  int(10);
    declare v_grade_unassigned_id int(10);
    declare v_backfill_needed int(10);
    declare v_delete_count int(10);
    declare v_ltdb_date_format_mask varchar(25);
    declare v_subtest_name varchar(25);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend,@p_db_name_dw);

    #########################
    ## Load Variables      ##
    #########################
    set v_ods_table = 'pmi_ods_ltdb_studentplus';
    set v_ods_view = 'v_pmi_ods_ltdb_studentplus';
    
    # Test Date Mask 
    set v_ltdb_date_format_mask = '%m/%d/%Y';
    #subtest name
    set v_subtest_name = 'Ohio OGT';
    
    
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

  
    select  count(*) 
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view
    ;


   if v_view_exists > 0 then
   
        #########################
        ## Load Working Tables ##
        #########################
        
        drop table if exists `tmp_ogt_subject_xref`;
        drop table if exists `tmp_ogt_strand_xref`;
        drop table if exists `tmp_ogt_stu_admin`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_school`;
        drop table if exists `tmp_subject_score_type_xref`;
        drop table if exists `tmp_test_date`;
        
         CREATE TABLE `tmp_test_date` (
          `ods_test_date` varchar(10) NOT NULL,
          `test_date` date NOT NULL,
          PRIMARY KEY  (`test_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        CREATE TABLE `tmp_ogt_subject_xref` (
          `ayp_subject_id` int(10) NOT NULL,
          `ayp_subject_code` varchar(50) NOT NULL,
          `client_ayp_subject_code` varchar(25) NOT NULL,
          PRIMARY KEY  (`ayp_subject_id`),
          UNIQUE KEY `uq_tmp_ogt_subject_xref` (`client_ayp_subject_code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_ogt_strand_xref` (
          `ayp_subject_id` int(10) NOT NULL,
          `ayp_strand_id` int(10) NOT NULL,
          `client_ayp_strand_code` varchar(50) NOT NULL,
          `ayp_subject_code` varchar(50) NOT NULL,
          `ayp_strand_code` varchar(50) NOT NULL,
          PRIMARY KEY  (`ayp_subject_id`,`ayp_strand_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        CREATE TABLE `tmp_ogt_stu_admin` (
          `student_code` varchar(15) not null,
          `test_date` datetime not null,
          `row_num` int(10) not null,
          `student_id` int(10) not null,
          `ayp_subject_id` int(10) not null,
          `test_month` tinyint(2) not null,
          `ltdb_score_type` varchar(50) not null,
          `ltdb_score` decimal(9,3) not null,
          `school_year_id` smallint(4) not null,
          `grade_code` varchar(15) default null,
          `school_code` varchar(15) default null,
          `backfill_needed_flag` tinyint(1),
          primary key (`student_id`,`ayp_subject_id`,`school_year_id`,`test_month`, `ltdb_score_type`),
          unique key `uq_tmp_stu_admin` (`student_code`, `ayp_subject_id`, `school_year_id`, `test_month`, `ltdb_score_type`),
          key `ind_tmp_stu_admin` (`row_num`)
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
        
        create table `tmp_school` (
          `school_code` varchar(15) not null,
          `school_id` int (10) not null,
          unique key `ind_school_code` (`school_code`)
        ) engine=innodb default charset=latin1
        ;
        
        CREATE TABLE `tmp_subject_score_type_xref` (
          `ltdb_scale_score_code` varchar(15) NOT NULL,
          `comment` varchar(25) NOT NULL,
          PRIMARY KEY  (`ltdb_scale_score_code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;


        #School List
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
        
        # Subject Xref
        insert  tmp_ogt_subject_xref (
            ayp_subject_id
            ,ayp_subject_code
            ,client_ayp_subject_code
        )
        select  sub.ayp_subject_id
            ,sub.ayp_subject_code
            ,xhs.client_ayp_subject_code
        from    c_ayp_test_type as tt
        join    c_ayp_subject as sub
                on      tt.ayp_test_type_id = sub.ayp_test_type_id
        join    v_pmi_xref_ayp_subject_hst_mapping as xhs
                on      xhs.pmi_ayp_subject_code = sub.ayp_subject_code
                and     xhs.pmi_test_type_code = tt.moniker
        where   tt.moniker = 'ogt'
        ;

        # Strand Xref
        insert  tmp_ogt_strand_xref (
                  ayp_subject_id
                  ,ayp_strand_id
                  ,client_ayp_strand_code
                  ,ayp_subject_code
                  ,ayp_strand_code
                )
        select  sub.ayp_subject_id
            ,str.ayp_strand_id
            ,xhs.client_ayp_strand_code
            ,sub.ayp_subject_code
            ,str.ayp_strand_code
        from    c_ayp_test_type as tt
        join    c_ayp_subject as sub
                on      tt.ayp_test_type_id = sub.ayp_test_type_id
        join    c_ayp_strand as str
                on      sub.ayp_subject_id = str.ayp_subject_id
        join    v_pmi_xref_ayp_strand_hst_mapping as xhs
                on      xhs.pmi_ayp_subject_code = sub.ayp_subject_code
                and     xhs.pmi_ayp_strand_code = str.ayp_strand_code
                and     xhs.pmi_test_type_code = tt.moniker
        where   tt.moniker = 'ogt'
        ;
        
        insert into tmp_test_date (
            ods_test_date
            ,test_date
        )
        select  ods.test_date as ods_test_date
            ,max(str_to_date(ods.test_date, v_ltdb_date_format_mask)) as test_date
        from v_pmi_ods_ltdb_studentplus as ods
        group by    ods.test_date
        ;
        
        # We don't have metadata to identify score types in LTDB associated with Scale Score
        #   Decision:  Encapsulate this type of data inside of LTDB procs.  Using comment column of DB table
        #              as self documenting data for future reference.  Comment column is not used.
        insert  tmp_subject_score_type_xref (
                  ltdb_scale_score_code
                  , `comment`
                )
        values  ('Scale', 'OH_HBOE');
      
        #Temp Stu Admin - Load admin data by student, subject, year, month and score type
        insert  tmp_ogt_stu_admin (
            student_code
            ,test_date
            ,row_num
            ,student_id
            ,ayp_subject_id
            ,test_month
            ,ltdb_score_type
            ,ltdb_score
            ,school_year_id
            ,grade_code
            ,school_code
            ,backfill_needed_flag
        )
       select ods.student_id as student_code
           ,td.test_date
           ,ods.row_num
           ,s.student_id
           ,sub.ayp_subject_id
           ,month(td.test_date) as test_month
           ,ods.score_typ
           ,case when ods.score_typ ='scale' then ods.score else concat(INSERT(ods.score,3,1,'.'),substring(ods.score,3),'00') end
           ,sy.school_year_id
           ,ods.grade
           ,ods.building
           ,case  when sty.school_year_id is null then 1
                  when sty.school_year_id is not null and sty.grade_level_id = v_grade_unassigned_id then 1
            end as backfill_needed_flag
 
       from    v_pmi_ods_ltdb_studentplus as ods
       join  tmp_test_date td
             on ods.test_date = td.ods_test_date
       join    c_student as s
               on      s.student_code = ods.student_id
               and     ods.score REGEXP '^[0-9]' > 0
       join    c_school_year as sy
               on      td.test_date between sy.begin_date and sy.end_date
       join    tmp_ogt_subject_xref as sub
               on      ods.subtest = sub.client_ayp_subject_code
       left join   c_student_year as sty
                  on    sty.student_id = s.student_id
                  and   sty.school_year_id = sy.school_year_id
       where ods.subtest_name =v_subtest_name and  ods.student_id is not null and ods.score_typ is not null
       union all
       select ods2.student_id as student_code
           ,td2.test_date
           ,ods2.row_num
           ,s2.student_id
           ,sub2.ayp_subject_id
           ,month(td2.test_date) as test_month
           ,ods2.score_typ
           ,case when ods2.score_typ ='scale' then ods2.score else concat(INSERT(ods2.score,3,1,'.'),substring(ods2.score,3),'00') end
           ,sy2.school_year_id
           ,ods2.grade
           ,ods2.building
           ,case  when sty2.school_year_id is null then 1 
                  when sty2.school_year_id is not null and sty2.grade_level_id = v_grade_unassigned_id then 1
           end as backfill_needed_flag
 
       from    v_pmi_ods_ltdb_studentplus as ods2
       join  tmp_test_date td2
             on ods2.test_date = td2.ods_test_date
       join    c_student as s2
               on      s2.student_state_code = ods2.student_id
               and     ods2.score REGEXP '^[0-9]' > 0
       join    c_school_year as sy2
               on      td2.test_date between sy2.begin_date and sy2.end_date
       join    tmp_ogt_subject_xref as sub2
               on      ods2.subtest = sub2.client_ayp_subject_code
       left join   c_student_year as sty2
                  on    sty2.student_id = s2.student_id
                  and   sty2.school_year_id = sy2.school_year_id
       where ods2.subtest_name =v_subtest_name and  ods2.student_id is not null and ods2.score_typ is not null
       union all
       select ods3.student_id as student_code
           ,td3.test_date
           ,ods3.row_num
           ,s3.student_id
           ,sub3.ayp_subject_id
           ,month(td3.test_date) as test_month
           ,ods3.score_typ
           ,ods3.score
           ,sy3.school_year_id
           ,case when ods3.score_typ ='scale' then ods3.score else concat(INSERT(ods3.score,3,1,'.'),substring(ods3.score,3),'00') end
           ,ods3.building
           ,case  when sty3.school_year_id is null then 1
                  when sty3.school_year_id is not null and sty3.grade_level_id = v_grade_unassigned_id then 1
            end as backfill_needed_flag
 
       from    v_pmi_ods_ltdb_studentplus as ods3
       join  tmp_test_date td3
             on ods3.test_date = td3.ods_test_date
       join    c_student as s3
               on      s3.fid_code = ods3.student_id
               and     ods3.score REGEXP '^[0-9]' > 0
       join    c_school_year as sy3
               on      td3.test_date between sy3.begin_date and sy3.end_date
       join    tmp_ogt_subject_xref as sub3
               on      ods3.subtest = sub3.client_ayp_subject_code
       left join   c_student_year as sty3
                  on    sty3.student_id = s3.student_id
                  and   sty3.school_year_id = sy3.school_year_id
       where ods3.subtest_name =v_subtest_name and ods3.student_id is not null and ods3.score_typ is not null
       on duplicate key update row_num = values(row_num)
              ,ltdb_score = values(ltdb_score)
       ;
        
        ##########################################
        ## Backfill for c_student_year 
        ## Need to detect and load c_student_year 
        ## records when supporting ones don't exist
        ##############################################
        
        select count(*)
        into v_backfill_needed
        from tmp_ogt_stu_admin
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
            from    tmp_ogt_stu_admin as sadmin
            join    tmp_subject_score_type_xref as sst  # only need to backfill if scale score is present
                on      sadmin.ltdb_score_type = sst.ltdb_scale_score_code
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


        ###################################
        ## Load Subject Data             ##
        ###################################
    
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
            ,tsa.ltdb_score
            ,'n'
            ,1234
            ,now()
        from    tmp_ogt_stu_admin as tsa
        join    tmp_subject_score_type_xref as sst
                on      tsa.ltdb_score_type = sst.ltdb_scale_score_code
        on duplicate key update last_user_id = values(last_user_id)
            ,ayp_score = values(ayp_score)
        ;
  
        ######################
        ## Load Strand Data ##
        ######################
  
                    
        insert c_ayp_strand_student
        (ayp_subject_id, ayp_strand_id ,student_id, school_year_id, month_id, ayp_score,points_earned,points_possible,score_type_code, last_user_id, create_timestamp)
        select  tsa.ayp_subject_id
                ,hsl.ayp_strand_id
                ,tsa.student_id
                ,tsa.school_year_id
                ,tsa.test_month
                #,concat(INSERT(score,3,1,'.'),substring(score,3),'00')
                ,round((cast(tsa.ltdb_score as decimal(9,3))/cast(pp.pp as decimal(9,3))) * 100, 3)  as ayp_score
                ,tsa.ltdb_score as points_earned
                ,pp.pp
                ,'n'
                ,1234
                ,now()
        from tmp_ogt_stu_admin as tsa
        join  tmp_ogt_strand_xref as hsl
                on  tsa.ayp_subject_id = hsl.ayp_subject_id
                and tsa.ltdb_score_type = hsl.client_ayp_strand_code
        join c_ayp_subject_student as cass
                on  tsa.student_id = cass.student_id
                and tsa.ayp_subject_id = cass.ayp_subject_id
                and tsa.school_year_id = cass.school_year_id
                and tsa.test_month = cass.month_id
        join v_pmi_ods_ayp_strand_points_possible pp
             on hsl.ayp_subject_code = pp.ayp_subject_code
             and hsl.ayp_strand_code = pp.ayp_strand_code
             and  tsa.school_year_id between pp.begin_year and pp.end_year
             and cast(tsa.grade_code as signed) between cast(pp.begin_grade_sequence as signed) and cast(pp.end_grade_sequence as signed)
        ON DUPLICATE KEY UPDATE last_user_id = values(last_user_id) 
                                ,ayp_score = values(ayp_score)
        ;
  

        #######################################################################################
        ## Delete records that exist in source with 0 month_id (single admin) 
        ## based on student, school_year and subject
        #######################################################################################
  
        select  count(*)
        into    v_delete_count
        from    tmp_ogt_stu_admin as tmp1
        join    c_ayp_subject_student as ss
                on      tmp1.student_id = ss.student_id
                and     tmp1.ayp_subject_id = ss.ayp_subject_id
                and     tmp1.school_year_id = ss.school_year_id
                and     ss.month_id = 0
        where   tmp1.test_month != 0
        ;
      
        if v_delete_count > 0 then
        
            delete  ayp_str.*
            from    tmp_ogt_stu_admin as tmp1
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
            from    tmp_ogt_stu_admin as tmp1
            join    c_ayp_subject_student as ss
                    on      ss.student_id = tmp1.student_id
                    and     ss.ayp_subject_id = tmp1.ayp_subject_id
                    and     ss.school_year_id = tmp1.school_year_id
                    and     ss.month_id = 0
            ;
            
        end if; 

        #################
        ## Clean up
        #################
    
        drop table if exists `tmp_ogt_subject_xref`;
        drop table if exists `tmp_ogt_strand_xref`;
        drop table if exists `tmp_ogt_stu_admin`;
        drop table if exists `tmp_student_year_backfill`;
        drop table if exists `tmp_school`;
        drop table if exists `tmp_subject_score_type_xref`;
    
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

