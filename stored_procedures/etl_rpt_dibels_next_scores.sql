/*
$Rev$ 
$Author$ 
$Date$
$HeadURL$
$Id$ 
*/

drop procedure if exists etl_rpt_dibels_next_scores//

create definer=`dbadmin`@`localhost` procedure etl_rpt_dibels_next_scores()
contains sql
sql security invoker
comment '$Rev$ $Date$'


proc: begin 

    declare v_ods_table         varchar(64);
    declare v_ods_view          varchar(64);
    declare v_view_exists       tinyint(1);
    declare v_assess_freq       char(1);
    declare v_assess_freq_id    int(10);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_dibels_next';
    set v_ods_view = concat('v_', v_ods_table);

    select  count(*)
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;
    

    if v_view_exists > 0 then

        drop table if exists `tmp_stu_list`;
        drop table if exists `tmp_dibels_year_xref`;
        drop table if exists `tmp_dibels_stu_grade_school_year`;
        drop table if exists `tmp_dibels_measure_xref`;
        drop table if exists `tmp_dibels_period_xref`;
        drop table if exists `tmp_dibels_measure_period_grade`;
        

        create table `tmp_dibels_year_xref` (
          `dibels_year_code` varchar(4) not null,
          `school_year_id` int(11) not null,
          `last_user_id` int(11) not null,
          primary key  (`school_year_id`),
          unique key `uq_tmp_dibels_year_xref` (`dibels_year_code`)
        ) engine=innodb default charset=latin1
        ;

        create table `tmp_stu_list` (
          `ods_student_code` varchar(15) not null,
          `student_id` int(10) not null,
          `last_user_id` int(11) not null,
          unique key `uq_tmp_stu_list` (`ods_student_code`),
          key `ind_tmp_stu_list` (`student_id`)
        ) engine=innodb default charset=latin1
        ;

        create table `tmp_dibels_stu_grade_school_year` (
          `student_id` int(10) not null,
          `school_year_id` int(11) not null,
          `grade_level_id` int(10) not null,
          `backfill_needed_flag` tinyint(1) not null,
          `dibels_year_code` varchar(4) not null,
          `ods_student_code` varchar(15) not null,
          `dibels_grade` varchar(5) not null,
          `last_user_id` int(11) not null,
          primary key  (`student_id`,`school_year_id`),
          key `ind_tmp_dibels_stu_grade_school_year` (`dibels_year_code`,`ods_student_code`,`dibels_grade`)
        ) engine=innodb default charset=latin1
        ;

        create table `tmp_dibels_measure_xref` (
          `ods_measure_code` varchar(20) not null,
          `measure_code` varchar(20) not null,
          `last_user_id` int(11) not null,
          unique key `uq_tmp_dibels_measure_xref` (`ods_measure_code`)
        ) engine=innodb default charset=latin1
        ;

        create table `tmp_dibels_period_xref` (
          `ods_period_code` varchar(20) not null,
          `period_code` varchar(20) not null,
          `last_user_id` int(11) not null,
          unique key `uq_tmp_dibels_period_xref` (`ods_period_code`)
        ) engine=innodb default charset=latin1
        ;

        create table `tmp_dibels_measure_period_grade` (
          `measure_id` int(10) NOT NULL,
          `freq_id` int(10) NOT NULL,
          `period_id` int(10) NOT NULL,
          `grade_level_id` int(10) NOT NULL,
          `ods_period_code` varchar(20) not null,
          `ods_measure_code` varchar(20) not null,
          `dibels_grade` varchar(5) not null,
          `last_user_id` int(11) not null,
          primary key  (`measure_id`,`freq_id`,`period_id`,`grade_level_id`),
          unique key `uq_tmp_dibels_measure_period_grade` (`ods_period_code`,`ods_measure_code`,`dibels_grade`)
        ) engine=innodb default charset=latin1
        ;


        set v_assess_freq = 3;
        
        select  freq_id
        into    v_assess_freq_id
        from    pm_dibels_assess_freq
        where   freq_code = v_assess_freq
        ;

        insert tmp_dibels_period_xref (
            ods_period_code
            ,period_code
            ,last_user_id
        )
        
        values ('beginning', '1', 1234)
                ,('middle', '2', 1234)
                ,('end', '3', 1234)
        ;

        insert tmp_dibels_measure_xref ( 
            ods_measure_code
            ,measure_code
            ,last_user_id
        )
        
        values ('LNF', 'dnLNF', 1234)
                ,('NWF-CLS', 'dnNWFCLS', 1234)
                ,('NWF-WWR', 'dnNWFWWR', 1234)
                ,('PSF', 'dnPSF', 1234)
                ,('Composite', 'dnComposite', 1234)
                ,('DORF-Retell', 'dnDORFRetell', 1234)
                ,('DORF-Retell Quality', 'dnDORFRetellQual', 1234)
                ,('DORF-Words Correct', 'dnDORFWC', 1234)
                ,('DORF-Accuracy', 'dnDORFAcc', 1234)
                ,('Daze-Correct', 'dnDAZECorrect', 1234)
                ,('Daze-Incorrect', 'dnDAZEIncorrect', 1234)
                ,('Daze-Adjusted Score', 'dnDAZEAdj', 1234)
                ,('FSF', 'dnFSF', 1234)
        ;

        insert tmp_dibels_year_xref (
            dibels_year_code
            ,school_year_id
            ,last_user_id
        )
        
        select  sydt.begin_year
            ,sydt.school_year_id
            ,1234
        
        from    (   select  begin_year
                    from    v_pmi_ods_dibels_next
                    group by begin_year
                ) as odsdt
        join    (   select  substring_index(sy.academic_year, '-', 1) as begin_year, sy.school_year_id
                    from    c_school_year as sy
                ) as sydt
                on      odsdt.begin_year = sydt.begin_year
        ;

        insert tmp_stu_list (
            ods_student_code
            ,student_id
            ,last_user_id
        )
        
        select  ods.sis_student_code
            ,max(s.student_id) as student_id
            ,1234 as last_user_id
            
        from    v_pmi_ods_dibels_next as ods
        join    c_student as s
                on      ods.sis_student_code = s.student_code
        group by ods.sis_student_code
        union all
        select  ods2.sis_student_code
            ,max(s2.student_id) as student_id
            ,1234 as last_user_id

        from    v_pmi_ods_dibels_next as ods2
        join    c_student as s2
                on      ods2.sis_student_code = s2.student_state_code
        group by ods2.sis_student_code
        on duplicate key update student_id = values(student_id)
        ;

        insert tmp_dibels_stu_grade_school_year (
            student_id
            ,school_year_id
            ,grade_level_id
            ,backfill_needed_flag
            ,dibels_year_code
            ,ods_student_code
            ,dibels_grade
            ,last_user_id
        )
        
        select  tmps.student_id
            ,tmpsy.school_year_id
            ,gl.grade_level_id
            ,case when sty.school_year_id is null then 1 else 0 end as backfill_needed_flag
            ,dt.begin_year
            ,dt.sis_student_code
            ,dt.grade
            ,1234
            
        from    (
                    select  ods.begin_year, ods.sis_student_code, ods.grade
                    from    v_pmi_ods_dibels_next as ods
                    group by ods.begin_year, ods.sis_student_code, ods.grade
                ) as dt
        join    tmp_stu_list as tmps
                on      dt.sis_student_code = tmps.ods_student_code
        join    tmp_dibels_year_xref as tmpsy
                on      dt.begin_year = tmpsy.dibels_year_code
        join    v_pmi_xref_grade_level as xgl
                on      dt.grade = xgl.client_grade_code
        join    c_grade_level as gl
                on      xgl.pmi_grade_code = gl.grade_code
        left join   c_student_year as sty
                on      tmps.student_id = sty.student_id
                and     tmpsy.school_year_id = sty.school_year_id
        ;

        insert tmp_dibels_measure_period_grade( 
            measure_id
            ,freq_id
            ,period_id
            ,grade_level_id
            ,ods_period_code
            ,ods_measure_code
            ,dibels_grade
            ,last_user_id
        )
        
        select  m.measure_id
            ,afp.freq_id
            ,afp.period_id
            ,gl.grade_level_id
            ,dt.assessment_period_name
            ,dt.measure_code
            ,dt.grade
            ,1234
        
        from    (
                    select  ods.assessment_period_name, ods.measure_code, ods.grade
                    from    v_pmi_ods_dibels_next as ods
                    group by ods.assessment_period_name, ods.measure_code, ods.grade
                ) as dt
        join    tmp_dibels_period_xref as pxref
                on      dt.assessment_period_name = pxref.ods_period_code
        join    pm_dibels_assess_freq_period as afp
                on      afp.freq_id = v_assess_freq_id
                and     afp.period_code = pxref.period_code
        join    tmp_dibels_measure_xref as mxref
                on      dt.measure_code = mxref.ods_measure_code
        join    pm_dibels_measure as m
                on      m.measure_code = mxref.measure_code
        join    v_pmi_xref_grade_level as xgl
                on      dt.grade = xgl.client_grade_code
        join    c_grade_level as gl
                on      xgl.pmi_grade_code = gl.grade_code
        ;

        insert rpt_dibels_scores (
            measure_period_id
            ,student_id
            ,school_year_id
            ,score
            ,prof_status
            ,last_user_id
            ,create_timestamp
        )

        select  mp.measure_period_id
            ,tmpsgsy.student_id
            ,tmpsgsy.school_year_id
            ,cast(substring_index(ods.score, '%', 1) as signed) as score
            ,ods.nfs
            ,1234
            ,now()

        from    v_pmi_ods_dibels_next as ods
        join    tmp_dibels_stu_grade_school_year as tmpsgsy
                on      ods.begin_year = tmpsgsy.dibels_year_code
                and     ods.sis_student_code = tmpsgsy.ods_student_code
                and     ods.grade = tmpsgsy.dibels_grade
        join    tmp_dibels_measure_period_grade as tmpmpg
                on      ods.assessment_period_name = tmpmpg.ods_period_code
                and     ods.measure_code = tmpmpg.ods_measure_code
                and     ods.grade = tmpmpg.dibels_grade
        join    pm_dibels_measure_period as mp
                on      tmpmpg.measure_id = mp.measure_id
                and     tmpmpg.freq_id = mp.freq_id
                and     tmpmpg.period_id = mp.period_id
                and     tmpmpg.grade_level_id = mp.grade_level_id
        on duplicate key update score = values(score)
            ,last_user_id = values(last_user_id)
        ;

        #################
        ## Update Log
        #################
        set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', v_ods_table, '\', \'P\', \'ETL Load Successful\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;

        drop table if exists `tmp_stu_list`;
        drop table if exists `tmp_dibels_year_xref`;
        drop table if exists `tmp_dibels_stu_grade_school_year`;
        drop table if exists `tmp_dibels_measure_xref`;
        drop table if exists `tmp_dibels_period_xref`;
        drop table if exists `tmp_dibels_measure_period_grade`;

    end if;

end proc;
//
