/*
$Rev$ 
$Author$ 
$Date$
$HeadURL$
$Id$ 
*/

drop procedure if exists etl_pm_smi_quantile_scores//

create definer=`dbadmin`@`localhost` procedure etl_pm_smi_quantile_scores()
contains sql
sql security invoker
comment '$Rev$ $Date$'


proc: begin 

    declare v_ods_table varchar(64);
    declare v_ods_view varchar(64);
    declare v_view_exists tinyint(1);
    declare v_date_format_mask varchar(15) default '%m%d%Y';

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_smi_quantile';
    set v_ods_view = concat('v_', v_ods_table);

    select  count(*)
    into    v_view_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_view;
    

    if v_view_exists > 0 then

        drop table if exists `tmp_stu_list`;

        create table `tmp_stu_list` (
          `ods_student_code` varchar(15) not null,
          `student_id` int(10) not null,
          `last_user_id` int(11) not null,
          unique key `uq_tmp_stu_list` (`ods_student_code`),
          key `ind_tmp_stu_list` (`student_id`)
        ) engine=innodb default charset=latin1
        ;

        set @smi_date_format_mask := pmi_f_get_etl_setting('smiQuantileDateFormatMask');
    
        if @smi_date_format_mask is not null then
            set v_date_format_mask = @smi_date_format_mask;
        end if;

        insert tmp_stu_list (
            ods_student_code
            ,student_id
            ,last_user_id
        )
        
        select  ods.sis_student_code
            ,max(s.student_id) as student_id
            ,1234 as last_user_id
            
        from    v_pmi_ods_smi_quantile as ods
        join    c_student as s
                on      ods.sis_student_code = s.student_code
        group by ods.sis_student_code
        union all
        select  ods2.sis_student_code
            ,max(s2.student_id) as student_id
            ,1234 as last_user_id

        from    v_pmi_ods_smi_quantile as ods2
        join    c_student as s2
                on      ods2.sis_student_code = s2.student_state_code
        group by ods2.sis_student_code
        union all
        select  ods3.sis_student_code
            ,max(s3.student_id) as student_id
            ,1234 as last_user_id

        from    v_pmi_ods_smi_quantile as ods3
        join    c_student as s3
                on      ods3.sis_student_code = s3.fid_code
        group by ods3.sis_student_code
        on duplicate key update student_id = values(student_id)
        ;

        insert into pm_smi_quantile_scores (
            student_id
            ,test_date
            ,test_moniker
            ,school_year_id
            ,score
            ,last_user_id
            ,create_timestamp
        )
        select tmps.student_id
            ,coalesce(str_to_date(substring_index(ods.test_date, ' ', 1), v_date_format_mask), '2000-01-01')
            ,coalesce(ods.moniker, date_format(str_to_date(substring_index(ods.test_date, ' ', 1), v_date_format_mask), '%m_%y'))
            ,sty.school_year_id
            ,ods.score
            ,1234
            ,now()

        from    v_pmi_ods_smi_quantile as ods
        join    tmp_stu_list as tmps
                on      ods.sis_student_code = ods_student_code
        join    c_student_year as sty
                on      sty.student_id = tmps.student_id
                and     sty.school_year_id = ods.school_year
        where   coalesce(ods.test_date, ods.moniker) is not null
        and     ods.score is not null
        on duplicate key update
            score = values(score)
            ,last_user_id = values(last_user_id)
        ;

        drop table if exists `tmp_stu_list`;

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
