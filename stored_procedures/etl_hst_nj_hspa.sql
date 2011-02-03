/*
$Rev: 8451 $ 
$Author: mike.torian $ 
$Date: 2010-04-21 08:43:44 -0400 (Wed, 21 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_hst_nj_hspa.sql $
$Id: etl_hst_nj_hspa.sql 8451 2010-04-21 12:43:44Z mike.torian $ 
 */

DROP PROCEDURE IF EXISTS etl_hst_nj_hspa //

create definer=`dbadmin`@`localhost` procedure etl_hst_nj_hspa()
contains sql
sql security invoker
COMMENT '$Rev: 8451 $ $Date: 2010-04-21 08:43:44 -0400 (Wed, 21 Apr 2010) $'
SQL SECURITY INVOKER

BEGIN 


    Declare done int Default 1;
    Declare var_ayp_subject_id int(11);
    Declare var_ayp_strand_id int(11);
    Declare var_column_moniker varchar(25);
    
    Declare strand_cursor cursor for
            select ayp_subject_id
                  ,ayp_strand_id
                  ,column_moniker
            from tmp_hspa_strand_list;
              
    Declare continue handler for sqlstate '02000' set done = 0;
            
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    set @ods_table := 'pmi_ods_nj_hspa';
    set @ods_view := 'v_pmi_ods_nj_hspa';
    
    
    select  school_year_id
    into    @school_year_id
    from    c_school_year sy
    where   sy.active_flag = 1;
    
    select  count(*) 
    into    @view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = @ods_view;

   if @view_exists > 0 then
   
        #########################
        ## Load Working Tables ##
        #########################
        
        drop table if exists `tmp_hspa_strand_list`;
        drop table if exists `tmp_hspa_stu_admin`;
        drop table if exists tmp_hspa_stu;
    
        CREATE TABLE `tmp_hspa_strand_list` (
          `ayp_subject_id` int(10) NOT NULL,
          `ayp_strand_id` int(10) NOT NULL,
          `column_moniker` varchar(50) NOT NULL,
          PRIMARY KEY  (`ayp_subject_id`,ayp_strand_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        CREATE TABLE `tmp_hspa_stu_admin` (
          `student_code` varchar(15) NOT NULL,
          `row_num` int(10) NOT NULL,
          `student_id` int(10) NOT NULL,
          `school_year_id` smallint(4) NOT NULL,
          `grade_sequence` int NOT NULL,
          `ayp_subject_id` int(10) NOT NULL,
          `score` int,
          UNIQUE KEY `uq_tmp_hspa_subject_list` (`student_code`,`ayp_subject_id`),
          KEY `ind_tmp_hspa_stu_admin_row_num` (`row_num`),
          KEY `ind_tmp_hspa_stu_admin_stu` (`student_id`),
          KEY `ind_tmp_hspa_stu_admin_sub` (`ayp_subject_id`,`student_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        CREATE TABLE `tmp_hspa_stu` (
          `ayp_subject_id` int(11) NOT NULL,
          `row_num` int(10) NOT NULL,
          `student_code` varchar(25) NOT NULL,
          `school_year` smallint(4) NOT NULL,
          `score` int,
          KEY `ind_tmp_hspa_stu_admin_row_num` (`row_num`),
          KEY `ind_tmp_hspa_stu_admin_stu` (`student_code`),
          KEY `ind_tmp_hspa_stu_admin_sub` (`ayp_subject_id`,`student_code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        insert  tmp_hspa_strand_list (ayp_subject_id, ayp_strand_id, column_moniker)
        select          cas.ayp_subject_id 
                        ,cas.ayp_strand_id
                        ,c.moniker
        from v_imp_table t 
        join v_imp_table_column c 
                on      c.table_id = t.table_id
        join v_imp_table_column_ayp_strand cas 
                on      cas.table_id = t.table_id and cas.column_id = c.column_id
        where target_table_name = @ods_table;
        
        insert tmp_hspa_stu (
                ayp_subject_id
                ,row_num
                ,student_code
                ,school_year
                ,score)
        select sub.ayp_subject_id
                ,row_num
                ,m.studentid
                ,m.school_year
                      ,(CASE sub.ayp_subject_code
                      WHEN 'hspaLangArtsLiter' THEN m.lang_ss
                      WHEN 'hspaMathematics' THEN m.math_ss
                      WHEN 'hspaScience' THEN m.science_ss
                      ELSE NULL
                    END ) as score
       from v_pmi_ods_nj_hspa AS m
       JOIN c_ayp_subject AS sub
       join    c_ayp_test_type tt
             on      tt.ayp_test_type_id = sub.ayp_test_type_id
             and     tt.moniker = 'hspa';

        insert  tmp_hspa_stu_admin (
            row_num
            ,student_code
            ,student_id
            ,school_year_id
            ,grade_sequence
            ,ayp_subject_id
            ,score
        )
        select  ods.row_num
            ,ods.student_code
            ,s.student_id
            ,ods.school_year
            ,gl.grade_sequence
            ,ods.ayp_subject_id
            ,ods.score
        from tmp_hspa_stu as ods          
        join    c_student as s
                on      s.student_code = ods.student_code
        join    c_student_year as sty
                on      sty.student_id = s.student_id
                and     sty.school_year_id = ods.school_year
        join    c_grade_level gl
                on      gl.grade_level_id = sty.grade_level_id
        where   ods.student_code is not null and ods.score is not null
        union all
        select  ods.row_num
            ,ods.student_code
            ,s.student_id
            ,ods.school_year
            ,gl.grade_sequence
            ,ods.ayp_subject_id
            ,ods.score
        from tmp_hspa_stu as ods          
        join    c_student as s
                on      s.student_state_code = ods.student_code
        join    c_student_year as sty
                on      sty.student_id = s.student_id
                and     sty.school_year_id = ods.school_year
        join    c_grade_level gl
                on      gl.grade_level_id = sty.grade_level_id
        where   ods.student_code is not null and ods.score is not null
        union all
        select  ods.row_num
            ,ods.student_code
            ,s.student_id
            ,ods.school_year
            ,gl.grade_sequence
            ,ods.ayp_subject_id
            ,ods.score
        from tmp_hspa_stu as ods          
        join    c_student as s
                on      s.fid_code = ods.student_code
        join    c_student_year as sty
                on      sty.student_id = s.student_id
                and     sty.school_year_id = ods.school_year
        join    c_grade_level gl
                on      gl.grade_level_id = sty.grade_level_id
        where   ods.student_code is not null and ods.score is not null
        on duplicate key update row_num = values(row_num)
        ;


        ################################
        ## Backfill of c_student_year ##
        ################################
      
        if @school_year_id <> @hspa_year_id then
        
          -- Call Proc to backfill c_student_year
          select 'Need Something to handle c_student_year' as Alert;
        
        end if;



        #######################
        ## Load Subject Data ##
        #######################
    
        insert c_ayp_subject_student (
            student_id
            ,ayp_subject_id
            ,school_year_id
            ,ayp_score
            ,score_type_code
            ,last_user_id
            ,create_timestamp
        )
        select  tsa.student_id
            ,tsa.ayp_subject_id
            ,tsa.school_year_id
            ,tsa.score
            ,'n'
            ,1234
            ,now()
        from    v_pmi_ods_nj_hspa as ods
        join    tmp_hspa_stu_admin as tsa
                on      tsa.row_num = ods.row_num
        on duplicate key update last_user_id = values(last_user_id)
            ,ayp_score = tsa.score
        ;
 
        ######################
        ## Load Strand Data ##
        ######################

        Open strand_cursor;
        
        Fetch strand_cursor into var_ayp_subject_id, var_ayp_strand_id, var_column_moniker;
        
        While done DO
        
                    set @sql_text := '';
                    set @sql_text := concat(@sql_text, ' insert c_ayp_strand_student (ayp_subject_id, ayp_strand_id ,student_id, school_year_id, ayp_score, score_type_code, last_user_id, create_timestamp)');
                    set @sql_text := concat(@sql_text, ' select ', var_ayp_subject_id, ', ', var_ayp_strand_id, ', tsa.student_id, tsa.school_year_id, round((', var_column_moniker, '/pp.pp*100),0)', var_column_moniker, ' , \'n\', 1234, now()');
                    set @sql_text := concat(@sql_text, ' from    ',@ods_view ,' as ods');
                    set @sql_text := concat(@sql_text, ' join    tmp_hspa_stu_admin as tsa');
                    set @sql_text := concat(@sql_text, '          on      tsa.row_num = ods.row_num and tsa.ayp_subject_id = ', var_ayp_subject_id);
                    set @sql_text := concat(@sql_text, ' join    c_student_year as sy');
                    set @sql_text := concat(@sql_text, ' on      tsa.student_id = sy.student_id');
                    set @sql_text := concat(@sql_text, ' and     tsa.school_year_id = sy.school_year_id');
                    set @sql_text := concat(@sql_text, ' join    c_grade_level as gl');
                    set @sql_text := concat(@sql_text, ' on      sy.grade_level_id = gl.grade_level_id');
                    set @sql_text := concat(@sql_text, ' join    c_ayp_strand as str');
                    set @sql_text := concat(@sql_text, ' on      str.ayp_subject_id = ', var_ayp_subject_id);
                    set @sql_text := concat(@sql_text, ' and     str.ayp_strand_id = ', var_ayp_strand_id);
                    set @sql_text := concat(@sql_text, ' join    v_pmi_ods_ayp_strand_points_possible pp');
                    set @sql_text := concat(@sql_text, ' on      sy.school_year_id between pp.begin_year and pp.end_year');
                    set @sql_text := concat(@sql_text, ' and     gl.grade_sequence between pp.begin_grade_sequence and pp.end_grade_sequence');
                    set @sql_text := concat(@sql_text, ' and     pp.ayp_strand_code = str.ayp_strand_code');
                    
                    set @sql_text := concat(@sql_text, ' where   ', var_column_moniker, ' is not null');
                    set @sql_text := concat(@sql_text, ' ON DUPLICATE KEY UPDATE last_user_id = 1234, ayp_score = ', var_column_moniker, ';');
 

                    prepare sql_text from @sql_text;
                    execute sql_text;
                    deallocate prepare sql_text;  

                    Fetch strand_cursor into var_ayp_subject_id, var_ayp_strand_id, var_column_moniker;
        
        End while;
        
        close strand_cursor;
    
       drop table if exists `tmp_hspa_strand_list`;
       drop table if exists `tmp_hspa_stu_admin`;
       drop table if exists `tmp_hspa_stu`;
    
        #################
        ## Update Log
        #################
        set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', @ods_table, '\', \'P\', \'ETL Load Successful\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;  

    end if;
    
end;
//
