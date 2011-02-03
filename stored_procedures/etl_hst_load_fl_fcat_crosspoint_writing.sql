/*
$Rev: 8518 $ 
$Author: mike.torian $ 
$Date: 2010-05-06 08:02:08 -0400 (Thu, 06 May 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_hst_load_fl_fcat_crosspoint_writing.sql $
$Id: etl_hst_load_fl_fcat_crosspoint_writing.sql 8518 2010-05-06 12:02:08Z mike.torian $ 
*/

drop procedure if exists etl_hst_load_fl_fcat_crosspoint_writing//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_fl_fcat_crosspoint_writing()
contains sql
sql security invoker
comment '$Rev: 8518 $ $Date: 2010-05-06 08:02:08 -0400 (Thu, 06 May 2010) $'

proc: begin 

    Declare v_ods_table varchar(50);
    Declare v_ods_view varchar(50);
    Declare v_view_exists smallint(4);
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    set v_ods_table = 'pmi_ods_ltdb_crosspoint_writing';
    set v_ods_view = 'v_pmi_ods_ltdb_crosspoint_writing';
    

    select  count(*)
    into    v_view_exists
    from    information_schema.views as t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;
    

    if v_view_exists > 0 then

            ###########################
            ## Create Working Tables ##
            ###########################
    
        drop table if exists tmp_fcat_date_conversion;
        drop table if exists tmp_fcat_stu_admin;
        drop table if exists tmp_xref_ayp_subject;
        drop table if exists tmp_delete_key;
       
        CREATE TABLE `tmp_fcat_stu_admin` (
            `row_num` int(10) NOT NULL,
            `student_code` varchar(15) NOT NULL,
            `test_year` varchar(4) NOT NULL,
            `student_id` int(10) NOT NULL,
            `month_id` tinyint(2) NOT NULL,
            `school_year_id` smallint(4) NOT NULL,
            `subtest_code` varchar(25) not null,
            `points_possible` smallint(4) null,
            `test_date` datetime not null,
        PRIMARY KEY `uq_tmp_fcat_stu_admin` (`row_num`),
        KEY `ind_tmp_fcat_stu_admin_stu` (`student_id`),
        KEY `ind_tmp_fcat_stu_subtest_code` (`subtest_code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        CREATE TABLE  `tmp_fcat_date_conversion` (
            `test_date` datetime not null,
            `year_id` int(10) not null,
            `month_id` tinyint(2) not null,
            `day_id` tinyint(2) not null,
        unique key `tmp_fcat_date_conversion_test_date` (`test_date`),
        key `tmp_fcat_date_conversion_year_mm_dd` (`year_id`, `month_id`, `day_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
    
        create table `tmp_xref_ayp_subject` (
            `test_type_code` varchar(25) not null,
            `client_ayp_code` varchar(2) not null,
            `pmi_ayp_subject_code` varchar(25) not null,
        unique key `uq_tmp_xref_ayp_subject` (`test_type_code`, `client_ayp_code`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        CREATE TABLE `tmp_delete_key` (
            `student_id` int(10) NOT NULL,
            `ayp_subject_id` int(10) NOT NULL,
            `school_year_id` smallint(4) NOT NULL,
            `month_id` tinyint(2) not null,
          PRIMARY KEY  (`student_id`,`ayp_subject_id`, `school_year_id`, `month_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
    
            #########################
            ## Load Working Tables ##
            #########################
    
            insert into tmp_fcat_date_conversion (
                    test_date
                    ,year_id
                    ,month_id
                    ,day_id)
            select 
                    str_to_date(concat(mm,dd,yyyy),'%m%d%Y') as test_date
                    ,yyyy as year_id
                    ,mm as month_id_
                    ,dd as day_id
            from v_pmi_ods_ltdb_crosspoint_writing
            where   yyyy is not null 
                    and mm is not null
            group by str_to_date(concat(mm,dd,yyyy),'%m%d%Y');
 
            #########################################################################
            ## Uploader tables not created until a better understanding  
            ## of longterm crosspoint reporting code use
            ########################################################
 
            insert into tmp_xref_ayp_subject (
                        test_type_code
                        ,client_ayp_code
                        ,pmi_ayp_subject_code
                        ) values
            ('fcat','01', 'fcatwriting')
            ,('fcat', '02', 'fcatwriting')
            ,('fcat', '03', 'fcatwriting')
            ;


            insert  tmp_fcat_stu_admin (
                    row_num
                    ,student_code
                    ,test_year
                    ,student_id
                    ,month_id
                    ,school_year_id
                    ,subtest_code
                    ,test_date
                    )
                
            select  ods.row_num
                    ,ods.student_id as student_code
                    ,c_date.year_id
                    ,s.student_id
                    ,c_date.month_id as month_id
                    ,sy.school_year_id
                    ,ods.subtest_id
                    ,c_date.test_date
            from    v_pmi_ods_ltdb_crosspoint_writing as ods
            join    c_student as s
                    on s.student_code = ods.student_id
                    and s.student_code is not null
            join tmp_fcat_date_conversion as c_date
                    on ods.yyyy = c_date.year_id
                    and ods.mm = c_date.month_id
                    and ods.dd = c_date.day_id
            join c_school_year as sy 
                    on c_date.test_date BETWEEN sy.begin_date AND sy.end_date
            join c_student_year as sty 
                    on sty.student_id = s.student_id 
                    and sty.school_year_id = sy.school_year_id
            join tmp_xref_ayp_subject as xref
                    on ods.subtest_id = xref.client_ayp_code
                    and ods.score_01 REGEXP '^[0-9]' > 0
            where   ods.student_id is not null
            union all
            select  ods.row_num
                    ,ods.student_id as student_code
                    ,c_date.year_id
                    ,s.student_id
                    ,c_date.month_id as month_id
                    ,sy.school_year_id
                    ,ods.subtest_id
                    ,c_date.test_date
            from    v_pmi_ods_ltdb_crosspoint_writing as ods
            join    c_student as s
                    on s.student_state_code = ods.student_id
                    and s.student_state_code is not null
            JOIN tmp_fcat_date_conversion as c_date
                    on ods.yyyy = c_date.year_id
                    and ods.mm = c_date.month_id
                    and ods.dd = c_date.day_id
            JOIN c_school_year as sy 
                    on c_date.test_date BETWEEN sy.begin_date AND sy.end_date
            JOIN c_student_year as sty 
                    on sty.student_id = s.student_id 
                    and sty.school_year_id = sy.school_year_id
            join tmp_xref_ayp_subject as xref
                    on ods.subtest_id = xref.client_ayp_code
                    and ods.score_01 REGEXP '^[0-9]' > 0
            where   ods.student_id is not null
          union all
            select  ods.row_num
                    ,ods.student_id as student_code
                    ,c_date.year_id
                    ,s.student_id
                    ,c_date.month_id as month_id
                    ,sy.school_year_id
                    ,ods.subtest_id
                    ,c_date.test_date
            from    v_pmi_ods_ltdb_crosspoint_writing as ods
            join    c_student as s
                    on s.fid_code = ods.student_id
                    and s.fid_code is not null
            JOIN tmp_fcat_date_conversion as c_date
                    on ods.yyyy = c_date.year_id
                    and ods.mm = c_date.month_id
                    and ods.dd = c_date.day_id
            JOIN c_school_year as sy 
                    on c_date.test_date BETWEEN sy.begin_date AND sy.end_date
            JOIN c_student_year as sty 
                    on sty.student_id = s.student_id 
                    and sty.school_year_id = sy.school_year_id
            join tmp_xref_ayp_subject as xref
                    on ods.subtest_id = xref.client_ayp_code
                    and ods.score_01 REGEXP '^[0-9]' > 0
            where   ods.student_id is not null
                
            on duplicate key update row_num = values(row_num);

            ########################
            ## Load Target Tables ##
            ########################
    
            insert c_ayp_subject_student (
                    student_id
                    ,ayp_subject_id
                    ,school_year_id
                    ,month_id
                    ,ayp_score
                    ,last_user_id
                    ,create_timestamp
                    )
            select 
                    stu.student_id
                    ,sub.ayp_subject_id
                    ,sy.school_year_id
                    ,sadmin.month_id as month_id
                    ,max(ods.score_01) as ayp_score
                    ,1234 as last_user_id
                    ,now() as create_timestamp
            from v_pmi_ods_ltdb_crosspoint_writing as ods
            join tmp_fcat_stu_admin as sadmin
                    on ods.row_num = sadmin.row_num
            join tmp_fcat_date_conversion as cdate
                    on sadmin.test_date = cdate.test_date
            join c_student as stu
                    on sadmin.student_id = stu.student_id
            join c_school_year as sy
                    on sadmin.test_date between sy.begin_date and sy.end_date
            join c_student_year as styr
                    on stu.student_id = styr.student_id
                    and sy.school_year_id = styr.school_year_id
            join tmp_xref_ayp_subject as xref
                    on ods.subtest_id = xref.client_ayp_code
            join c_ayp_subject as sub
                    on xref.pmi_ayp_subject_code = sub.ayp_subject_code
            join c_ayp_test_type as ttype
                    on sub.ayp_test_type_id = ttype.ayp_test_type_id
                    and ttype.moniker = 'fcat'
            group by    stu.student_id
                        ,sub.ayp_subject_id
                        ,sy.school_year_id
                        ,sadmin.month_id
            on duplicate key update
                             ayp_score = values(ayp_score)
            ;


        #######################################################################################
        ## Delete records that exist in source with 0 month_id (single admin) 
        ## based on student, school_year and subject
        #######################################################################################

        insert into tmp_delete_key 
                (student_id
                ,ayp_subject_id
                ,school_year_id
                ,month_id
        )
        select  stu2.student_id
                ,stu2.ayp_subject_id
                ,stu2.school_year_id
                ,stu2.month_id
        from c_ayp_subject_student as stu
        join c_ayp_subject_student as stu2
                on stu.student_id = stu2.student_id
                and stu.ayp_subject_id = stu2.ayp_subject_id
                and stu.school_year_id = stu2.school_year_id
                and stu2.month_id = 0
                and stu.month_id <> 0
        group by stu2.student_id
                ,stu2.ayp_subject_id
                ,stu2.school_year_id
                ,stu2.month_id
        ;

        delete ayp_sub
        from c_ayp_subject_student as ayp_sub
        join tmp_delete_key as del
            on ayp_sub.student_id = del.student_id
            and ayp_sub.ayp_subject_id = del.ayp_subject_id
            and ayp_sub.school_year_id = del.school_year_id
            and ayp_sub.month_id = del.month_id
        join c_ayp_subject as sub
            on ayp_sub.ayp_subject_id = sub.ayp_subject_id
        join c_ayp_test_type as typ
            on sub.ayp_test_type_id = typ.ayp_test_type_id
            and typ.moniker = 'fcat'
         ;

    #########################
    ## clean-up tmp tables ##
    #########################
            
        drop table if exists tmp_fcat_date_conversion;
        drop table if exists tmp_fcat_stu_admin;
        drop table if exists tmp_xref_ayp_subject;
        drop table if exists tmp_delete_key;
        
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
