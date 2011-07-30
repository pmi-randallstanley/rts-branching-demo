/*
$Rev: 8587 $
$Author: mike.torian $
$Date: 2010-05-13 08:49:42 -0400 (Thu, 13 May 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_hst_load_ga_ghsgt.sql $
$Id: etl_hst_load_ga_ghsgt.sql 8587 2010-05-13 12:49:42Z mike.torian $
 */

drop procedure if exists etl_hst_load_ga_ghsgt//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_ga_ghsgt()
contains sql
sql security invoker
comment '$Rev: 8587 $ $Date: 2010-05-13 08:49:42 -0400 (Thu, 13 May 2010) $'

proc: begin

    Declare v_ayp_subject_id int(11);
    Declare v_ayp_strand_id int(11);
    Declare v_score_moniker varchar(25);
    Declare v_sub_score_moniker varchar(25);
    Declare v_no_more_rows boolean;
    Declare v_school_year int(4);
    Declare v_ods_table varchar(20);
    Declare v_ods_view varchar(20);
    Declare v_view_exists smallint(4);
    Declare v_delete_count int(10);

    Declare v_subject_cursor cursor for
            select ayp_subject_id, score_column
            from tmp_ghsgt_subject_list
            ;

    Declare v_strand_cursor cursor for
            select ayp_subject_id, ayp_strand_id, score_column, sub_score_column
            from tmp_ghsgt_strand_list
            ;

    declare continue handler for not found 
    set v_no_more_rows = true;
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table := 'pmi_ods_ga_ghsgt';
    set v_ods_view := 'v_pmi_ods_ga_ghsgt';
    

    select  count(*)
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;
    

    if v_view_exists > 0 then

        #######################
        ## Load Working Tables ##
        #######################

        drop table if exists `tmp_ghsgt_subject_list`;
        drop table if exists `tmp_ghsgt_strand_list`;
        drop table if exists `tmp_ghsgt_stu_admin`;
        drop table if exists `tmp_ghsgt_date_conversion`;
        drop table if exists `tmp_delete_key`;
 
        CREATE TABLE `tmp_ghsgt_subject_list` (
          `ayp_subject_id` int(10) NOT NULL,
          `score_column` varchar(20) NOT NULL,
          `ayp_subject_code` varchar(50) NOT NULL,
          PRIMARY KEY  (`ayp_subject_id`)
          ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_ghsgt_strand_list` (
          `ayp_subject_id` int(10) NOT NULL,
          `ayp_strand_id` int(10) NOT NULL,
          `score_column` varchar(50) NOT NULL,
          `sub_score_column` varchar(20) not null,
          PRIMARY KEY  (`ayp_subject_id`,ayp_strand_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_ghsgt_stu_admin` (
          `student_code` varchar(15) NOT NULL,
          `test_year` varchar(4) NOT NULL,
          `row_num` int(10) NOT NULL,
          `student_id` int(10) NOT NULL,
          `test_month` tinyint(2) NOT NULL,
          `school_year_id` smallint(4) NOT NULL,
          UNIQUE KEY `uq_tmp_hsa_subject_list` (`student_code`, `test_year`,`test_month`),
          KEY `ind_tmp_hsa_stu_admin_row_num` (`row_num`),
          KEY `ind_tmp_hsa_stu_admin_stu` (`student_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_ghsgt_date_conversion` (
          `testmo` varchar(2) NOT NULL,
          `testyr` varchar(4) NOT NULL,
          `test_year` int(4) NOT NULL,
          `test_month` int(2) Not Null,
          `test_date` date NOT NULL,
          UNIQUE KEY `uq_tmp_hsa_subject_list` (`testmo`, `testyr`)
          ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_delete_key` (
            `student_id` int(10) NOT NULL,
            `ayp_subject_id` int(10) NOT NULL,
            `school_year_id` smallint(4) NOT NULL,
            `month_id` tinyint(2) not null,
        PRIMARY KEY  (`student_id`,`ayp_subject_id`, `school_year_id`, `month_id`),
        KEY `key_ayp_strand_student` (`ayp_subject_id`,`student_id`, `school_year_id`, `month_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

       ## No test date provided in source so date conversion is done here along with a generated date for joins

        insert into tmp_ghsgt_date_conversion (
            testmo
            ,testyr
            ,test_year
            ,test_month
            ,test_date
        )
        select  testmo 
                ,testyr
                ,case when length(testyr) = 4 then testyr else 2000 + cast(testyr as signed) end as test_year
                ,cast(testmo as signed) as test_month
                ,cast(concat(case when length(testyr) = 4 then testyr else 2000 + cast(testyr as signed) end,'-', testmo, '-15') as date) as test_date
        from v_pmi_ods_ga_ghsgt
        group by testmo, testyr;


        insert into tmp_ghsgt_subject_list (
             ayp_subject_id
            ,score_column
            ,ayp_subject_code
        )
        select ayps.ayp_subject_id, col.moniker, sub.ayp_subject_code
        from v_imp_table_column_ayp_subject as ayps
        join v_imp_table_column as col
                on ayps.score_column_id = col.column_id
        join  v_imp_table as tab
                on ayps.table_id = tab.table_id
                and target_table_name = v_ods_table
        join c_ayp_subject as sub
                on ayps.ayp_subject_id = sub.ayp_subject_id
        ;


        insert  tmp_ghsgt_strand_list (
             ayp_subject_id
            ,ayp_strand_id
            ,score_column
            ,sub_score_column
        )
        select str.ayp_subject_id, str.ayp_strand_id
               , col.moniker, sub.score_column
        from v_imp_table_column_ayp_strand as str
        join v_imp_table_column as col
                on str.table_id = col.table_id
                and str.score_column_id = col.column_id
        join v_imp_table as tab
                on col.table_id = tab.table_id
                and  tab.target_table_name = v_ods_table
        join tmp_ghsgt_subject_list as sub
                on str.ayp_subject_id = sub.ayp_subject_id
        ;


        insert  tmp_ghsgt_stu_admin (
            row_num
            ,student_code
            ,test_year
            ,student_id
            ,test_month
            ,school_year_id
        )
            
        select  ods.row_num
            ,ods.stuid
            ,c_date.test_year
            ,s.student_id
            ,c_date.test_month
            ,sy.school_year_id
        from    v_pmi_ods_ga_ghsgt as ods
        join    c_student as s
                on      s.student_code = ods.stuid
        JOIN tmp_ghsgt_date_conversion as c_date
                on ods.testmo = c_date.testmo
                and ods.testyr = c_date.testyr
        JOIN c_school_year as sy 
                on c_date.test_date BETWEEN sy.begin_date AND sy.end_date
        JOIN c_student_year as sty 
                on sty.student_id = s.student_id 
                and sty.school_year_id = sy.school_year_id
        where   ods.stuid is not null
        union all
        select  ods.row_num
            ,ods.stuid
            ,c_date.test_year
            ,s.student_id
            ,c_date.test_month
            ,sy.school_year_id
        from    v_pmi_ods_ga_ghsgt as ods
        join    c_student as s
                on      s.student_state_code = ods.stuid
        JOIN tmp_ghsgt_date_conversion as c_date
                on ods.testmo = c_date.testmo
                and ods.testyr = c_date.testyr
        JOIN c_school_year as sy 
                on c_date.test_date BETWEEN sy.begin_date AND sy.end_date
        JOIN c_student_year as sty 
                on sty.student_id = s.student_id 
                and sty.school_year_id = sy.school_year_id
        where   ods.stuid is not null
        union all
        select  ods.row_num
            ,ods.stuid
            ,c_date.test_year
            ,s.student_id
            ,c_date.test_month
            ,sy.school_year_id
        from    v_pmi_ods_ga_ghsgt as ods
        join    c_student as s
                on      s.fid_code = ods.stuid
        JOIN tmp_ghsgt_date_conversion as c_date
                on ods.testmo = c_date.testmo
                and ods.testyr = c_date.testyr
        JOIN c_school_year as sy 
                on c_date.test_date BETWEEN sy.begin_date AND sy.end_date
        JOIN c_student_year as sty 
                on sty.student_id = s.student_id 
                and sty.school_year_id = sy.school_year_id
        where   ods.stuid is not null
        on duplicate key update row_num = values(row_num);


        #######################
        ## Load Subject Data ##
        #######################

        open v_subject_cursor;
        loop_subject_cursor: loop
        FETCH v_subject_cursor 
        INTO  v_ayp_subject_id, v_score_moniker;

            if v_no_more_rows then
                close v_subject_cursor;
                leave loop_subject_cursor;
            end if;
     
            SET @sql_text := '';
            SET @sql_text := concat('insert c_ayp_subject_student (student_id,ayp_subject_id,school_year_id, month_id, ayp_score,last_user_id,create_timestamp)'
                                , ' SELECT  tsa.student_id, ',v_ayp_subject_id,' ,tsa.school_year_id, tsa.test_month, m.',v_score_moniker ,' ,1234, now() '
                                , ' FROM v_pmi_ods_ga_ghsgt AS m ' 
                                , ' join tmp_ghsgt_stu_admin as tsa ' 
                                , '         on m.row_num = tsa.row_num '
                                , ' WHERE m.',v_score_moniker,' REGEXP \'^[0-9]\' > 0  '
                                , ' ON DUPLICATE KEY UPDATE last_user_id = values(last_user_id), ayp_score = values(ayp_score); ');

            prepare stmt from @sql_text;
            execute stmt;
            deallocate prepare stmt;
                                

            SET @sql_text := '';
            SET @sql_text := concat('insert tmp_delete_key (student_id,ayp_subject_id,school_year_id, month_id)'
                                , ' SELECT  tsa.student_id, ',v_ayp_subject_id,' ,tsa.school_year_id, 0 as month_id'
                                , ' FROM v_pmi_ods_ga_ghsgt AS m ' 
                                , ' join tmp_ghsgt_stu_admin as tsa ' 
                                , '         on m.row_num = tsa.row_num '
                                , ' WHERE m.',v_score_moniker,' REGEXP \'^[0-9]\' > 0  '
                                , '       and tsa.test_month != 0 '
                                , ' ON DUPLICATE KEY UPDATE month_id = values(month_id); ');
                                
                                
            prepare stmt from @sql_text;
            execute stmt;
            deallocate prepare stmt;
            
            end loop loop_subject_cursor;

        ######################
        ## Load Strand Data ##
        ######################
 
        set v_no_more_rows = false;
        
        open v_strand_cursor;
        loop_strand_cursor: loop
        FETCH v_strand_Cursor 
        INTO  v_ayp_subject_id, v_ayp_strand_id, v_score_moniker, v_sub_score_moniker;

            if v_no_more_rows then
                close v_strand_cursor;
                leave loop_strand_cursor;
            end if;

            SET @sql_text := '';
            SET @sql_text := concat( 'insert c_ayp_strand_student (student_id, ayp_subject_id, ayp_strand_id, school_year_id, month_id, '
                            ,  ' ayp_score, last_user_id, create_timestamp)'
                            ,  ' SELECT  tsa.student_id, ', v_ayp_subject_id,', ', v_ayp_strand_id, ', tsa.school_year_id, '
                            ,  ' tsa.test_month, ', v_score_moniker, ' ,1234, now() ' 
                            ,  ' FROM v_pmi_ods_ga_ghsgt AS m '
                            ,  ' join tmp_ghsgt_stu_admin as tsa '
                            ,  '          on m.row_num = tsa.row_num '
                            ,  ' where ', v_score_moniker, ' REGEXP \'^[0-9]\' > 0 '
                            , '         and ',v_sub_score_moniker,' REGEXP \'^[0-9]\' > 0  '
                            ,  ' ON DUPLICATE KEY UPDATE last_user_id = values(last_user_id), ayp_score = values(ayp_score), month_id = values(month_id);');


            prepare stmt from @sql_text;
            execute stmt;
            deallocate prepare stmt;

            end loop loop_strand_cursor;

        #######################################################################################
        ## Delete records that exist in source with 0 month_id (single admin) 
        ## based on student, school_year and subject
        #######################################################################################

        select count(*)
        into v_delete_count
        from tmp_delete_key;
        
        if v_delete_count > 0 then

            delete ayp_str
            from c_ayp_strand_student as ayp_str
            join tmp_delete_key as del
                on ayp_str.student_id = del.student_id
                and ayp_str.ayp_subject_id = del.ayp_subject_id
                and ayp_str.school_year_id = del.school_year_id
                and ayp_str.month_id = del.month_id
             ;
    
            delete ayp_sub
            from c_ayp_subject_student as ayp_sub
            join tmp_delete_key as del
                on ayp_sub.student_id = del.student_id
                and ayp_sub.ayp_subject_id = del.ayp_subject_id
                and ayp_sub.school_year_id = del.school_year_id
                and ayp_sub.month_id = del.month_id
             ;

        end if;

    #########################
    ## clean-up tmp tables ##
    #########################
            
        drop table if exists `tmp_ghsgt_subject_list`;
        drop table if exists `tmp_ghsgt_strand_list`;
        drop table if exists `tmp_ghsgt_stu_admin`;
        drop table if exists `tmp_ghsgt_date_conversion`;
        drop table if exists `tmp_delete_key`;
        
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
