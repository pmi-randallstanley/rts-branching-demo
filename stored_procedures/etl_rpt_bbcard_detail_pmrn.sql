/*
$Rev: 9954 $
$Author: ryan.riordan $
$Date: 2011-01-28 09:58:52 -0500 (Fri, 28 Jan 2011) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bbcard_detail_pmrn.sql $
$Id: etl_rpt_bbcard_detail_pmrn.sql 9954 2011-01-28 14:58:52Z ryan.riordan $
*/

drop procedure if exists etl_rpt_bbcard_detail_pmrn //

create definer=`dbadmin`@`localhost` procedure etl_rpt_bbcard_detail_pmrn()
contains sql
sql security invoker
comment '$Rev: 9954 $ $Date: 2011-01-28 09:58:52 -0500 (Fri, 28 Jan 2011) $'

proc: begin

    declare v_ods_table varchar(64);
    declare v_date_format_mask varchar(15) default '%y%y';

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    set v_ods_table = 'pmi_ods_fl_pmrn';

    set @etl_client_settings_mask := pmi_f_get_etl_setting('bbcardPmrnDateFormatMask');
    if @etl_client_settings_mask is not null then
        set v_date_format_mask = @etl_client_settings_mask;
    end if
    ;

    select  bb_group_id
    into    @bb_group_id
    from    pm_bbcard_group
    where   bb_group_code = 'pmrn'
    ;

    drop table if exists `tmp_stu_admin`;
    drop table if exists `tmp_date_conversion`;

    create table `tmp_stu_admin` (
      `student_code` varchar(15) NOT NULL,
      `row_num` int(10) NOT NULL,
      `student_id` int(10) NOT NULL,
      `school_year_id` smallint(4) NOT NULL,
      `grade_code` varchar(15) default null,
      `school_code` varchar(15) default null,
      `backfill_needed_flag` tinyint(1),
      primary key (`student_id`, `school_year_id`)
    ) engine=innodb default charset=latin1
    ;

    create table `tmp_date_conversion` (
      `school_year` varchar(4) DEFAULT NULL,
      `school_year_id` int unsigned NOT NULL,
      primary key (`school_year`),
      key (`school_year_id`)
    ) engine=innodb default charset=latin1
    ;

    insert tmp_date_conversion (
            school_year
           ,school_year_id
    )
    select school_year
          ,year(str_to_date(school_year, v_date_format_mask))
    from v_pmi_ods_fl_pmrn
    where school_year is not null
    group by school_year
    ;

    # student admin data
    insert  tmp_stu_admin (
            row_num
           ,student_code
           ,student_id
           ,school_year_id
           ,backfill_needed_flag
    )
    select  max(ods.row_num)
           ,ods.student_eid
           ,s.student_id
           ,tdc.school_year_id
           ,case when sty.school_year_id is null then 1 end as backfill_needed_flag
    from    v_pmi_ods_fl_pmrn as ods
    join    tmp_date_conversion tdc
            on ods.school_year = tdc.school_year
    join    c_student as s
            on    s.student_state_code = ods.student_eid
    left join c_student_year as sty
            on    sty.student_id = s.student_id
            and   sty.school_year_id = tdc.school_year_id
    where   ods.student_eid is not null
    group by ods.student_eid
    union all
    select  max(ods.row_num)
           ,ods.student_fid
           ,s.student_id
           ,tdc.school_year_id
           ,case when sty.school_year_id is null then 1 end as backfill_needed_flag
    from    v_pmi_ods_fl_pmrn as ods
    join    tmp_date_conversion tdc
            on ods.school_year = tdc.school_year
    join    c_student as s
            on    s.fid_code = ods.student_fid
    left join c_student_year as sty
            on    sty.student_id = s.student_id
            and   sty.school_year_id = tdc.school_year_id
    where   ods.student_fid is not null
    group by ods.student_fid
    order by 1
    on duplicate key update row_num = values(row_num)
    ;

    insert rpt_bbcard_detail_pmrn (
         bb_group_id
        ,bb_measure_id
        ,bb_measure_item_id
        ,student_id
        ,school_year_id
        ,score
        ,score_type
        ,score_color
        ,last_user_id
        ,create_timestamp
    )
    select mi.bb_group_id
          ,mi.bb_measure_id
          ,mi.bb_measure_item_id
          ,s.student_id
          ,s.school_year_id
          ,case CONCAT(mi.bb_measure_item_code, m.bb_measure_code)
             when 'ap1success'              then ods.PRS_FSP_ap1
             when 'ap1kReadFluency'         then ods.k2_rc_fluency_ap1
             when 'ap1kReadAccuracy'        then ods.k2_rc_accuracy_ap1
             when 'ap1kReadGrade'           then ods.k2_rc_grade_ap1
             when 'ap1kReadPassNum'         then ods.k2_rc_number_ap1
             when 'ap1kReadTarPassNum'      then ods.k2_rc_target_ap1
             when 'ap1compExplicit'         then ods.c_explict_ap1
             when 'ap1compImplicit'         then ods.c_implicit_ap1
             when 'ap1vocRawScore'          then ods.v_rs_ap1
             when 'ap1vocPercent'           then ods.v_percent_ap1
             when 'ap1spellRawSpell'        then ods.s_rs_ap1
             when 'ap1spellPercent'         then ods.s_percent_ap1
             when 'ap1readCompPercent'      then ods.312_rc_percent_ap1
             when 'ap1readCompScale'        then ods.312_rc_ss_ap1
             when 'ap1readCompAbility'      then ods.312_rc_as_ap1
             when 'ap1readCompLexile'       then ods.312_rc_lexile_ap1
             when 'ap1mazePercent'          then ods.m_percent_ap1
             when 'ap1mazeStandardScore'    then ods.m_ss_ap1
             when 'ap1mazeAdjScore'         then ods.m_as_ap1
             when 'ap1wordAnalysisPercent'  then ods.wa_percent_ap1
             when 'ap1wordAnalysisStandard' then ods.wa_ss_ap1
             when 'ap1wordAnalysisAbility'  then ods.wa_as_ap1
             when 'ap1boxScore'             then CAST(case
                                                        when coalesce(ods.PRS_FSP_ap1
                                                                     ,ods.m_percent_ap1
                                                                     ,ods.wa_percent_ap1) IS NULL                then NULL
                                                        when ods.PRS_FSP_ap1    > 84                             then '1'
                                                        when ods.m_percent_ap1  > 30 and ods.wa_percent_ap1 > 30 then '2+4'
                                                        when ods.m_percent_ap1  > 30                             then '2+5'
                                                        when ods.wa_percent_ap1 > 30                             then '3+4'
                                                        else                                                          '3+5'
                                                      end as CHAR)
             when 'ap2success'              then ods.PRS_FSP_ap2
             when 'ap2kReadFluency'         then ods.k2_rc_fluency_ap2
             when 'ap2kReadAccuracy'        then ods.k2_rc_accuracy_ap2
             when 'ap2kReadGrade'           then ods.k2_rc_grade_ap2
             when 'ap2kReadPassNum'         then ods.k2_rc_number_ap2
             when 'ap2kReadTarPassNum'      then ods.k2_rc_target_ap2
             when 'ap2compExplicit'         then ods.c_explict_ap2
             when 'ap2compImplicit'         then ods.c_implicit_ap2
             when 'ap2vocRawScore'          then ods.v_rs_ap2
             when 'ap2vocPercent'           then ods.v_percent_ap2
             when 'ap2spellRawSpell'        then ods.s_rs_ap2
             when 'ap2spellPercent'         then ods.s_percent_ap2
             when 'ap2readCompPercent'      then ods.312_rc_percent_ap2
             when 'ap2readCompScale'        then ods.312_rc_ss_ap2
             when 'ap2readCompAbility'      then ods.312_rc_as_ap2
             when 'ap2readCompLexile'       then ods.312_rc_lexile_ap2
             when 'ap2mazePercent'          then ods.m_percent_ap2
             when 'ap2mazeStandardScore'    then ods.m_ss_ap2
             when 'ap2mazeAdjScore'         then ods.m_as_ap2
             when 'ap2wordAnalysisPercent'  then ods.wa_percent_ap2
             when 'ap2wordAnalysisStandard' then ods.wa_ss_ap2
             when 'ap2wordAnalysisAbility'  then ods.wa_as_ap2
             when 'ap2boxScore'             then CAST(case
                                                        when coalesce(ods.PRS_FSP_ap2
                                                                     ,ods.m_percent_ap2
                                                                     ,ods.wa_percent_ap2) IS NULL                then NULL
                                                        when ods.PRS_FSP_ap2    > 84                             then '1'
                                                        when ods.m_percent_ap2  > 30 and ods.wa_percent_ap2 > 30 then '2+4'
                                                        when ods.m_percent_ap2  > 30                             then '2+5'
                                                        when ods.wa_percent_ap2 > 30                             then '3+4'
                                                        else                                                          '3+5'
                                                      end as CHAR)
             when 'ap3success'              then ods.PRS_FSP_ap3
             when 'ap3kReadFluency'         then ods.k2_rc_fluency_ap3
             when 'ap3kReadAccuracy'        then ods.k2_rc_accuracy_ap3
             when 'ap3kReadGrade'           then ods.k2_rc_grade_ap3
             when 'ap3kReadPassNum'         then ods.k2_rc_number_ap3
             when 'ap3kReadTarPassNum'      then ods.k2_rc_target_ap3
             when 'ap3compExplicit'         then ods.c_explict_ap3
             when 'ap3compImplicit'         then ods.c_implicit_ap3
             when 'ap3vocRawScore'          then ods.v_rs_ap3
             when 'ap3vocPercent'           then ods.v_percent_ap3
             when 'ap3spellRawSpell'        then ods.s_rs_ap3
             when 'ap3spellPercent'         then ods.s_percent_ap3
             when 'ap3readCompPercent'      then ods.312_rc_percent_ap3
             when 'ap3readCompScale'        then ods.312_rc_ss_ap3
             when 'ap3readCompAbility'      then ods.312_rc_as_ap3
             when 'ap3readCompLexile'       then ods.312_rc_lexile_ap3
             when 'ap3mazePercent'          then ods.m_percent_ap3
             when 'ap3mazeStandardScore'    then ods.m_ss_ap3
             when 'ap3mazeAdjScore'         then ods.m_as_ap3
             when 'ap3wordAnalysisPercent'  then ods.wa_percent_ap3
             when 'ap3wordAnalysisStandard' then ods.wa_ss_ap3
             when 'ap3wordAnalysisAbility'  then ods.wa_as_ap3
             when 'ap3boxScore'             then CAST(case
                                                        when coalesce(ods.PRS_FSP_ap3
                                                                     ,ods.m_percent_ap3
                                                                     ,ods.wa_percent_ap3) IS NULL                then NULL
                                                        when ods.PRS_FSP_ap3    > 84                             then '1'
                                                        when ods.m_percent_ap3  > 30 and ods.wa_percent_ap3 > 30 then '2+4'
                                                        when ods.m_percent_ap3  > 30                             then '2+5'
                                                        when ods.wa_percent_ap3 > 30                             then '3+4'
                                                        else                                                          '3+5'
                                                      end as CHAR)
           end as score
          ,IF ( m.bb_measure_code = 'boxScore', 'a', 'n' ) as score_type
          ,null
          ,1234
          ,now()
    from   v_pmi_ods_fl_pmrn ods
    join   tmp_stu_admin s
           on ods.row_num = s.row_num
    join   pm_bbcard_group b
    join   pm_bbcard_measure as m
           on      m.bb_group_id = b.bb_group_id
    join   pm_bbcard_measure_item as mi
           on      mi.bb_group_id = m.bb_group_id
           and     mi.bb_measure_id = m.bb_measure_id
    where  b.bb_group_id = @bb_group_id
    having score is not null
    on duplicate key update score = values(score)
    ;
    
    # Update score color
    update rpt_bbcard_detail_pmrn as rpt
    join    c_student_year sy
            on    rpt.student_id = sy.student_id
            and   rpt.school_year_id = sy.school_year_id
    join    c_grade_level gl
            on    sy.grade_level_id = gl.grade_level_id
    join    pm_bbcard_color_pmrn as cs
            on    rpt.bb_group_id = cs.bb_group_id
            and   rpt.bb_measure_id = cs.bb_measure_id
            and   rpt.bb_measure_item_id = cs.bb_measure_item_id 
            and   gl.grade_level_id = cs.grade_level_id
            and   rpt.score between cs.min_score and cs.max_score
    join    pmi_color as c
            on    c.color_id = cs.color_id
    set rpt.score_color = c.moniker
    ;

    #################
    ## Update Log
    #################
    set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', v_ods_table, '\', \'P\', \'ETL Load Successful\')');

    prepare sql_scan_log from @sql_scan_log;
    execute sql_scan_log;
    deallocate prepare sql_scan_log;

    drop table if exists `tmp_stu_admin`;
    drop table if exists `tmp_date_conversion`;
    
end proc;
//
