/*
$Rev: 8152 $
$Author: mike.torian $
$Date: 2010-02-03 14:54:21 -0500 (Wed, 03 Feb 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_hst_load_ga_ghsgt_single_admin.sql $
$Id: etl_hst_load_ga_ghsgt_single_admin.sql 8152 2010-02-03 19:54:21Z mike.torian $
 */

drop procedure if exists etl_hst_load_ga_ghsgt_single_admin//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_ga_ghsgt_single_admin()
contains sql
sql security invoker
comment '$Rev: 8152 $'


proc: begin

    Declare v_ayp_subject_id int(11);
    Declare v_ayp_strand_id int(11);
    Declare v_column_moniker varchar(25);
    Declare v_no_more_rows boolean;
    Declare v_school_year int(4);
    Declare v_ods_table varchar(20);
    Declare v_ods_view varchar(20);
    Declare v_view_exists smallint(4);

    Declare v_subject_cursor cursor for
            select ayp_subject_id, client_ayp_subject_code
            from tmp_ghsgt_subject_list
            ;

    Declare v_strand_cursor cursor for
            select ayp_subject_id, ayp_strand_id, column_moniker
            from tmp_ghsgt_strand_list
            ;

    declare continue handler for not found 
    set v_no_more_rows = true;
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    set v_ods_table := 'pmi_ods_ga_ghsgt';
    set v_ods_view := 'v_pmi_ods_ga_ghsgt';

    ##########################################################################
    ##  It was determined that single-admins  for GHSGTs would continue for  ##
    ##  for existing school districts for school years <  2010.              ##  
    ##  The proc may need to be revised to pass a parameter as new districts ##
    ##  may want to load all test administrations.                           ##
    ##########################################################################
    
    set v_school_year = 2010;

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
        drop table if exists `tmp_ghsgt_subject_dtls`;
        drop table if exists `tmp_ghsgt_date_conversion`;
 
        CREATE TABLE `tmp_ghsgt_subject_list` (
          `ayp_subject_id` int(10) NOT NULL,
          `client_ayp_subject_code` varchar(20) NOT NULL,
          `ayp_subject_code` varchar(50) NOT NULL,
          PRIMARY KEY  (`ayp_subject_id`),
          UNIQUE KEY `uq_tmp_hsa_subject_list` (`client_ayp_subject_code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_ghsgt_strand_list` (
          `ayp_subject_id` int(10) NOT NULL,
          `ayp_strand_id` int(10) NOT NULL,
          `column_moniker` varchar(50) NOT NULL,
          PRIMARY KEY  (`ayp_subject_id`,ayp_strand_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_ghsgt_subject_dtls` (
          `row_num` int(10) NOT NULL,
          `student_id` int(10) NOT NULL,
          `ayp_subject_id` int(10) NOT NULL,
          `school_year_id` smallint(4) NOT NULL,
          `ayp_score` int(10) NOT NULL,
          PRIMARY KEY  (`row_num`, `student_id`, `ayp_subject_id`, `school_year_id` )
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

## No test date provided in source so date conversion is done here along with a generated date for joins

        CREATE TABLE `tmp_ghsgt_date_conversion` (
          `testmo` varchar(2) NOT NULL,
          `testyr` varchar(4) NOT NULL,
          `test_year` int(4) NOT NULL,
          `test_month` int(2) Not Null,
          `test_date` date NOT NULL,
          UNIQUE KEY `uq_tmp_hsa_subject_list` (`testmo`, `testyr`)
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
            ,client_ayp_subject_code
            ,ayp_subject_code
        )
        select ayps.ayp_subject_id, col.moniker, sub.ayp_subject_code
        from v_imp_table_column_ayp_subject as ayps
        join v_imp_table_column as col
                on ayps.column_id = col.column_id
        join  v_imp_table as tab
                on ayps.table_id = tab.table_id
                and target_table_name = v_ods_table
        join c_ayp_subject as sub
                on ayps.ayp_subject_id = sub.ayp_subject_id
        ;


        insert  tmp_ghsgt_strand_list (
             ayp_subject_id
            ,ayp_strand_id
            ,column_moniker
        )
        select ayp_subject_id, ayp_strand_id, col.moniker
        from v_imp_table_column_ayp_strand as str
        join v_imp_table_column as col
                on str.table_id = col.table_id
                and str.column_id = col.column_id
        join v_imp_table  as tab
                on col.table_id = tab.table_id
                and  tab.target_table_name = v_ods_table
        ;

        ######################################################################
        ## tmp_ghsgt_stu_admin is used to associate student record with     ##
        ## row_num so the correct data can be loaded for ayp_subject        ##
        ## and ayp_strand data. A best score for a subject may come         ##
        ## from different admins within the same year .                     ##
        ######################################################################


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
        JOIN tmp_ghsgt_date_conversion c_date
                on ods.testmo = c_date.testmo
                and ods.testyr = c_date.testyr
        JOIN c_school_year AS sy 
                on c_date.test_date BETWEEN sy.begin_date AND sy.end_date
        JOIN c_student_year AS sty 
                on sty.student_id = s.student_id 
                and sty.school_year_id = sy.school_year_id
                and sy.school_year_id < v_school_year
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
        JOIN tmp_ghsgt_date_conversion c_date
                on ods.testmo = c_date.testmo
                and ods.testyr = c_date.testyr
        JOIN c_school_year AS sy 
                on c_date.test_date BETWEEN sy.begin_date AND sy.end_date
        JOIN c_student_year AS sty 
                on sty.student_id = s.student_id 
                and sty.school_year_id = sy.school_year_id
                and sy.school_year_id < v_school_year
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
        JOIN tmp_ghsgt_date_conversion c_date
                on ods.testmo = c_date.testmo
                and ods.testyr = c_date.testyr
        JOIN c_school_year AS sy 
                on c_date.test_date BETWEEN sy.begin_date AND sy.end_date
        JOIN c_student_year AS sty 
                on sty.student_id = s.student_id 
                and sty.school_year_id = sy.school_year_id
                and sy.school_year_id < v_school_year
        where   ods.stuid is not null
        on duplicate key update row_num = values(row_num);
 
        ######################################################################
        ## Load tmp_ayp_subject_dtls with cursor                            ##
        ## Max ayp_subject and assoc row_num needs to be pivoted            ##
        ## c_ayp_strand_student data needs the max ayp_subject score        ##
        ## which may come from different admins with in one year            ##
        ######################################################################

        open v_subject_cursor;
        loop_subject_cursor: loop
        FETCH v_subject_cursor 
        INTO  v_ayp_subject_id, v_column_moniker;

            if v_no_more_rows then
                close v_subject_cursor;
                leave loop_subject_cursor;
            end if;
     
            SET @sql_text := '';
            SET @sql_text := concat('insert into tmp_ghsgt_subject_dtls (row_num, student_id, ayp_subject_id, school_year_id, ayp_score)'
                                , ' SELECT max(tmp.row_num) as row_num, tmp.student_id, sub.ayp_subject_id, tmp.school_year_id, dt.ayp_score '
                                , ' FROM v_pmi_ods_ga_ghsgt AS m '
                                , ' JOIN c_ayp_subject AS sub ON sub.ayp_subject_id = ', v_ayp_subject_id
                                , ' join tmp_ghsgt_stu_admin as tmp on m.row_num = tmp.row_num '
                                , ' join ( '
                                , '     SELECT  tmp2.student_id, tmp2.school_year_id,  max(m2.', v_column_moniker, ') as ayp_score'
                                , '     FROM v_pmi_ods_ga_ghsgt AS m2 '
                                , '     join tmp_ghsgt_stu_admin as tmp2 on m2.row_num = tmp2.row_num '
                                , '     WHERE m2.',v_column_moniker,' is not null '
                                , '     group by tmp2.student_id, tmp2.school_year_id) dt '
                                , ' on tmp.student_id = dt.student_id '
                                , ' and tmp.school_year_id = dt.school_year_id '
                                , ' and m.', v_column_moniker, ' = dt.ayp_score '
                                , ' group by tmp.student_id, sub.ayp_subject_id, tmp.school_year_id, dt.ayp_score '
                                , ' on duplicate key update row_num = values (row_num);');

            prepare stmt from @sql_text;
            execute stmt;
            deallocate prepare stmt;

            end loop loop_subject_cursor;
                                
        #######################
        ## Load Subject Data ##
        #######################

         
        insert into c_ayp_subject_student (
            student_id
            ,ayp_subject_id
            ,school_year_id
            ,month_id
            ,ayp_score
            ,last_user_id
            ,create_timestamp)            
        select student_id
            ,ayp_subject_id
            ,school_year_id
            ,0 as month_id
            ,ayp_score
            ,1234
            ,now()
        from tmp_ghsgt_subject_dtls
        on duplicate key update ayp_score = values(ayp_score);

        ######################
        ## Load Strand Data ##
        ######################
 
        set v_no_more_rows = false;
        
        open v_strand_cursor;
        loop_strand_cursor: loop
        FETCH v_strand_Cursor 
        INTO  v_ayp_subject_id, v_ayp_strand_id, v_column_moniker;

            if v_no_more_rows then
                close v_strand_cursor;
                leave loop_strand_cursor;
            end if;

            SET @sql_text := '';
            SET @sql_text := concat( 'insert c_ayp_strand_student (student_id, ayp_subject_id, ayp_strand_id, school_year_id, month_id, '
                            ,  ' ayp_score, last_user_id, create_timestamp)'
                            ,  ' SELECT  tmp.student_id, sub.ayp_subject_id, str.ayp_strand_id, tmp.school_year_id, 0 as test_month, ', v_column_moniker, ' ,1234, now() ' 
                            ,  ' FROM v_pmi_ods_ga_ghsgt AS m '
                            ,  ' JOIN c_ayp_subject AS sub ON sub.ayp_subject_id = ', v_ayp_subject_id
                            ,  ' JOIN c_ayp_strand AS str ON str.ayp_subject_id = sub.ayp_subject_id AND str.ayp_strand_id = ', v_ayp_strand_id
                            ,  ' join tmp_ghsgt_subject_dtls as tmp on tmp.row_num = m.row_num and tmp.ayp_subject_id = sub.ayp_subject_id'
                            ,  ' where ', v_column_moniker, ' is not null '
                            ,  ' ON DUPLICATE KEY UPDATE last_user_id = values(last_user_id), ayp_score = values(ayp_score);');


            prepare stmt from @sql_text;
            execute stmt;
            deallocate prepare stmt;

            end loop loop_strand_cursor;

    #########################
    ## clean-up tmp tables ##
    #########################
            
        drop table if exists `tmp_ghsgt_subject_list`;
        drop table if exists `tmp_ghsgt_strand_list`;
        drop table if exists `tmp_ghsgt_stu_admin`;
        drop table if exists `tmp_ghsgt_subject_dtls`;
        drop table if exists `tmp_ghsgt_date_conversion`;

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
