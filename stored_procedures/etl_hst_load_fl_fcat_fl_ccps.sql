/*
$Rev: 8518 $ 
$Author: mike.torian $ 
$Date: 2010-05-06 08:02:08 -0400 (Thu, 06 May 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_hst_load_fl_fcat_fl_ccps.sql $
$Id: etl_hst_load_fl_fcat_fl_ccps.sql 8518 2010-05-06 12:02:08Z mike.torian $ 
*/

drop procedure if exists etl_hst_load_fl_fcat_fl_ccps//

create definer=`dbadmin`@`localhost` procedure etl_hst_load_fl_fcat_fl_ccps()
contains sql
sql security invoker
comment '$Rev: 8518 $ $Date: 2010-05-06 08:02:08 -0400 (Thu, 06 May 2010) $'

proc: begin 

    Declare v_ods_table varchar(50);
    Declare v_ods_view varchar(50);
    Declare v_view_exists smallint(4);
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    set v_ods_table := 'pmi_ods_fl_ccps_cpmtest';
    set v_ods_view := 'v_pmi_ods_fl_ccps_cpmtest';
    
    
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
        drop table if exists tmp_xref_ayp_strand;
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
        KEY `ind_tmp_fcat_stu_subtest_code` (`subtest_code`),
        KEY `ind_tmp_fcat_stu_ayp_subject` (`student_id`,`school_year_id`,`month_id`)
             ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;
            
        create table `tmp_fcat_date_conversion` (
            `school_year` varchar(10) not null,
            `test_date` date not null,
            `year_id` int(10) not null,
            `month_id` tinyint(2) not null,
            unique key `uq_tmp_fcat_date_conv_school_year` (`school_year`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;
            
        create table `tmp_xref_ayp_subject` (
            `test_type_code` varchar(25) not null,
            `client_ayp_code` varchar(2) not null,
            `pmi_ayp_subject_code` varchar(25) not null,
            unique key `uq_tmp_xref_ayp_subject_codes` (`test_type_code`, `client_ayp_code`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            ;
    
        create table `tmp_xref_ayp_strand` (
            `test_type_code` varchar(25) not null,
            `client_ayp_code` varchar(2) not null,
            `pmi_ayp_subject_code` varchar(25) not null,
            `pmi_ayp_strand_code` varchar(25) not null,            
            unique key `uq_tmp_xref_ayp_strand_codes` (`test_type_code`, `client_ayp_code`)
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
    
            ####################################
            ## Load Conversion Working Tables ##
            ####################################
    
            insert tmp_fcat_date_conversion (
                        school_year
                        ,test_date
                        ,year_id
                        ,month_id)
            select 
                    school_year
                   ,str_to_date(src.school_year, '%m%d%Y') as test_date
                   ,year(str_to_date(src.school_year, '%m%d%Y')) as year_id
                   ,month(str_to_date(src.school_year, '%m%d%Y')) as month_id
            from v_pmi_ods_fl_ccps_cpmtest as src
            group by school_year;
           
        
            #########################################################################
            ## Uploader tables not created until a better understanding  
            ## of longterm ccps reporting code use
            ########################################################

            insert into tmp_xref_ayp_subject (
                        test_type_code
                        ,client_ayp_code
                        ,pmi_ayp_subject_code
                        ) values
            ('fcat','1','fcatReading')
            ,('fcat','4','fcatMath')
            ,('fcat','42','fcatScience')
            ;

            insert into tmp_xref_ayp_strand (
                        test_type_code
                        ,client_ayp_code
                        ,pmi_ayp_subject_code
                        ,pmi_ayp_strand_code
                        ) values
            ('fcat','5','fcatMath', 'fcatMathNSCO')
            ,('fcat','6','fcatMath', 'fcatMathMeas')
            ,('fcat','7','fcatMath', 'fcatMathGSS')
            ,('fcat','8','fcatMath', 'fcatMathAT')
            ,('fcat','9','fcatMath', 'fcatMathDAP')
            ,('fcat','38','fcatReading', 'fcatReadingWPC')
            ,('fcat','39','fcatReading', 'fcatReadingMIPP')
            ,('fcat','40','fcatReading', 'fcatReadingCCE')
            ,('fcat','41','fcatReading', 'fcatReadingRAR')
            ,('fcat','43','fcatScience', 'fcatSciencePC')
            ,('fcat','44','fcatScience', 'fcatScienceES')
            ,('fcat','45','fcatScience', 'fcatScienceLE')
            ,('fcat','46','fcatScience', 'fcatScienceST')
            ;

            ##########################################################
            ## Points Possible included in tmp_fcat_stu_admin
            ## for performance reasons.  
            ##########################################################


            insert  tmp_fcat_stu_admin (
                    row_num
                    ,student_code
                    ,test_year
                    ,student_id
                    ,month_id
                    ,school_year_id
                    ,subtest_code
                    ,points_possible
                    ,test_date
                    )
                
            select  ods.row_num
                    ,ods.student_id as student_code
                    ,c_date.year_id
                    ,s.student_id
                    ,c_date.month_id as month_id
                    ,sy.school_year_id
                    ,ods.subtest_code
                    ,pp.pp as points_possible
                    ,c_date.test_date
            from    v_pmi_ods_fl_ccps_cpmtest as ods
            join    c_student as s
                    on s.student_code = ods.student_id
                    and s.student_code is not null
                    and ods.test_code = 'FCA'
            join tmp_fcat_date_conversion as c_date
                    on ods.school_year = c_date.school_year
            join c_school_year as sy 
                    on c_date.test_date between sy.begin_date and sy.end_date
            join c_student_year as sty 
                    on sty.student_id = s.student_id 
                    and sty.school_year_id = sy.school_year_id
            join c_grade_level as gr
                    on gr.grade_level_id = sty.grade_level_id
            left join tmp_xref_ayp_subject as xref_sub
                    on ods.subtest_code = xref_sub.client_ayp_code
                    and ods.test_ss  REGEXP '^[0-9]' > 0
            left join tmp_xref_ayp_strand as xref
                    on ods.subtest_code = xref.client_ayp_code
                    and ods.test_rs REGEXP '^[0-9]' > 0
            left join v_pmi_ods_ayp_strand_points_possible as pp
                    on xref.pmi_ayp_subject_code = pp.ayp_subject_code
                    and xref.pmi_ayp_strand_code = pp.ayp_strand_code
                    and sy.school_year_id between pp.begin_year and pp.end_year
                    and gr.grade_code between pp.begin_grade_sequence and pp.end_grade_sequence
            where   ods.student_id is not null
            union all
            select  ods.row_num
                    ,ods.student_id as student_code
                    ,c_date.year_id
                    ,s.student_id
                    ,c_date.month_id as month_id
                    ,sy.school_year_id
                    ,ods.subtest_code
                    ,pp.pp as points_possible
                    ,c_date.test_date
            from    v_pmi_ods_fl_ccps_cpmtest as ods
            join    c_student as s
                    on s.student_state_code = ods.student_id
                    and s.student_state_code is not null
                    and ods.test_code = 'FCA'
            JOIN tmp_fcat_date_conversion as c_date
                    on ods.school_year = c_date.school_year
            JOIN c_school_year as sy 
                    on c_date.test_date BETWEEN sy.begin_date AND sy.end_date
            JOIN c_student_year as sty 
                    on sty.student_id = s.student_id 
                    and sty.school_year_id = sy.school_year_id
            join c_grade_level as gr
                    on gr.grade_level_id = sty.grade_level_id
            left join tmp_xref_ayp_subject as xref_sub
                    on ods.subtest_code = xref_sub.client_ayp_code
                    and ods.test_ss  REGEXP '^[0-9]' > 0
            left join tmp_xref_ayp_strand as xref
                    on ods.subtest_code = xref.client_ayp_code
                    and ods.test_rs REGEXP '^[0-9]' > 0
            left join v_pmi_ods_ayp_strand_points_possible as pp
                    on xref.pmi_ayp_subject_code = pp.ayp_subject_code
                    and xref.pmi_ayp_strand_code = pp.ayp_strand_code
                    and sy.school_year_id between pp.begin_year and pp.end_year
                    and gr.grade_code between pp.begin_grade_sequence and pp.end_grade_sequence
            where   ods.student_id is not null
            union all
            select  ods.row_num
                    ,ods.student_id as student_code
                    ,c_date.year_id
                    ,s.student_id
                    ,c_date.month_id as month_id
                    ,sy.school_year_id
                    ,ods.subtest_code
                    ,pp.pp as points_possible
                    ,c_date.test_date
            from    v_pmi_ods_fl_ccps_cpmtest as ods
            join    c_student as s
                    on      s.fid_code = ods.student_id
                    and s.fid_code is not null
                    and ods.test_code = 'FCA'
            JOIN tmp_fcat_date_conversion as c_date
                    on ods.school_year = c_date.school_year
            JOIN c_school_year as sy 
                    on c_date.test_date BETWEEN sy.begin_date AND sy.end_date
            JOIN c_student_year as sty 
                    on sty.student_id = s.student_id 
                    and sty.school_year_id = sy.school_year_id
            join c_grade_level as gr
                    on gr.grade_level_id = sty.grade_level_id
            left join tmp_xref_ayp_subject as xref_sub
                    on ods.subtest_code = xref_sub.client_ayp_code
                    and ods.test_ss  REGEXP '^[0-9]' > 0
            left join tmp_xref_ayp_strand as xref
                    on ods.subtest_code = xref.client_ayp_code
                    and ods.test_rs REGEXP '^[0-9]' > 0
            left join v_pmi_ods_ayp_strand_points_possible as pp
                    on xref.pmi_ayp_subject_code = pp.ayp_subject_code
                    and xref.pmi_ayp_strand_code = pp.ayp_strand_code
                    and sy.school_year_id between pp.begin_year and pp.end_year
                    and gr.grade_code between pp.begin_grade_sequence and pp.end_grade_sequence
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
                    ,alt_ayp_score
                    ,last_user_id
                    ,create_timestamp
                    )
            select 
                    stu.student_id
                    ,sub.ayp_subject_id
                    ,sy.school_year_id
                    ,sadmin.month_id
                    ,ods.test_ss as ayp_score
                    ,ods.test_dss as alt_ayp_score
                    ,1234 as last_user_id
                    ,now() as create_timestamp
            from v_pmi_ods_fl_ccps_cpmtest as ods
            join tmp_fcat_stu_admin as sadmin
                    on sadmin.row_num = ods.row_num
            join c_student as stu
                    on sadmin.student_id = stu.student_id
            join tmp_fcat_date_conversion as cdate
                    on ods.school_year = cdate.school_year
            join c_school_year as sy
                    on cdate.test_date between sy.begin_date and sy.end_date
            join c_student_year as styr
                    on stu.student_id = styr.student_id
                    and sy.school_year_id = styr.school_year_id
            join tmp_xref_ayp_subject as xref
                    on ods.subtest_code = xref.client_ayp_code
            join c_ayp_subject as sub
                    on xref.pmi_ayp_subject_code = sub.ayp_subject_code
            join c_ayp_test_type as ttype
                    on sub.ayp_test_type_id = ttype.ayp_test_type_id
                    and ttype.moniker = 'fcat'
            where ods.test_ss  REGEXP '^[0-9]' > 0
            on duplicate key update
                    ayp_score = values(ayp_score)
                    ,alt_ayp_score = values(alt_ayp_score)
            ;
            
            insert c_ayp_strand_student (
                    ayp_subject_id
                    ,ayp_strand_id
                    ,student_id
                    ,school_year_id
                    ,month_id
                    ,ayp_score
                    ,points_earned
                    ,points_possible
                    ,last_user_id
                    ,create_timestamp)
            select 
                    str.ayp_subject_id
                    ,str.ayp_strand_id
                    ,stu.student_id
                    ,sy.school_year_id
                    ,sadmin.month_id
                    ,ods.test_rs / sadmin.points_possible * 100 as ayp_score
                    ,ods.test_rs as points_earned
                    ,sadmin.points_possible as points_possible
                    ,1234 as last_user_id
                    ,now() as create_timestamp
            from v_pmi_ods_fl_ccps_cpmtest as ods
            join tmp_fcat_stu_admin as sadmin
                    on sadmin.row_num = ods.row_num
            join c_student as stu
                    on sadmin.student_id = stu.student_id
            join tmp_fcat_date_conversion as cdate
                    on ods.school_year = cdate.school_year
            join c_school_year as sy
                    on cdate.test_date between sy.begin_date and sy.end_date
            join c_student_year as styr
                    on stu.student_id = styr.student_id
                    and sy.school_year_id = styr.school_year_id
            join c_grade_level as gr
                    on styr.grade_level_id = gr.grade_level_id
            join tmp_xref_ayp_strand as xref
                    on ods.subtest_code = xref.client_ayp_code
            join c_ayp_subject as sub
                    on xref.pmi_ayp_subject_code = sub.ayp_subject_code
            join c_ayp_strand as str
                    on sub.ayp_subject_id = str.ayp_subject_id
                    and xref.pmi_ayp_strand_code = str.ayp_strand_code
            join c_ayp_test_type as ttype
                    on sub.ayp_test_type_id = ttype.ayp_test_type_id
                    and ttype.moniker = 'fcat'
            join c_ayp_subject_student as substu
                    on stu.student_id = substu.student_id
                    and sub.ayp_subject_id = substu.ayp_subject_id
                    and sy.school_year_id = substu.school_year_id
                    and sadmin.month_id = substu.month_id
            where   ods.test_rs REGEXP '^[0-9]' > 0
                    and sadmin.points_possible REGEXP '^[0-9]' > 0
            on duplicate key update
                    ayp_score = values(ayp_score)
                    ,points_earned = values(points_earned)
                    ,points_possible = values(points_possible)
                    ,last_user_id = values(last_user_id)
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

        delete ayp_str
        from c_ayp_strand_student as ayp_str
        join tmp_delete_key as del
            on ayp_str.student_id = del.student_id
            and ayp_str.ayp_subject_id = del.ayp_subject_id
            and ayp_str.school_year_id = del.school_year_id
            and ayp_str.month_id = del.month_id
        join c_ayp_subject as sub
            on ayp_str.ayp_subject_id = sub.ayp_subject_id
        join c_ayp_test_type as typ
            on sub.ayp_test_type_id = typ.ayp_test_type_id
            and typ.moniker = 'fcat'
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
        drop table if exists tmp_xref_ayp_strand;
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
