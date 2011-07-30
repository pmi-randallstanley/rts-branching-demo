/*
$Rev: 9639 $ 
$Author: randall.stanley $ 
$Date: 2010-11-05 22:26:46 -0400 (Fri, 05 Nov 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_student.sql $
$Id: etl_imp_student.sql 9639 2010-11-06 02:26:46Z randall.stanley $ 
 */


drop procedure if exists etl_imp_student//

create definer=`dbadmin`@`localhost` procedure etl_imp_student()
contains sql
sql security invoker
comment '$Rev: 9639 $ $Date: 2010-11-05 22:26:46 -0400 (Fri, 05 Nov 2010) $'
begin

    declare v_useincrementalschedule    char(1) default 'y';
    declare v_dob_date_format_mask      varchar(15) default '%Y-%m-%d';
    declare v_iep_date_format_mask      varchar(15) default '%Y-%m-%d';
    declare v_ods_table                 varchar(64);
    declare v_ods_view                  varchar(64);
    declare v_ods_ethn_view             varchar(64) default 'v_pmi_xref_ethnicity';
    declare v_view_ethn_exists          tinyint(1);
    declare v_hisp_ethn_value_exists    tinyint(1) default '0';
    declare v_pmi_std_hisp_ethn_code    varchar(1) default 'h';
    declare v_unknown_race_id           int(10);

    drop table if exists `tmp_id_assign`;
    drop table if exists `tmp_ethnicity_race_code_list`;

    create table `tmp_id_assign` (
      `new_id` int(11) not null,
      `base_code` varchar(20) not null,
      primary key  (`new_id`),
      unique key `uq_tmp_id_assign` (`base_code`)
    );

    create table `tmp_ethnicity_race_code_list` (
      `client_ethn_race_code` varchar(50) not null,
      `pmi_ethn_race_code` varchar(20) not null,
      unique key `uq_tmp_ethnicity_race_code_list` (`client_ethn_race_code`)
    );

    set @useincrementalschedule := pmi_f_get_etl_setting('useincrementalschedule');
    select count(*) into @rowcnt from v_pmi_ods_student;

    set @dob_date_format_mask := pmi_f_get_etl_setting('stuDOBDateFormatMask');
    set @iep_date_format_mask := pmi_f_get_etl_setting('stuIEPDateFormatMask');

    set v_ods_table = 'pmi_ods_student';
    set v_ods_view = concat('v_', v_ods_table);

    if @dob_date_format_mask is not null then
        set v_dob_date_format_mask = @dob_date_format_mask;
    end if;
    
    if @iep_date_format_mask is not null then
        set v_iep_date_format_mask = @iep_date_format_mask;
    end if;
    
    if @useincrementalschedule = 'y' then 
        set v_useincrementalschedule = 'n';
    end if;
    
    if v_useincrementalschedule = 'n' and @rowcnt > 1000 then 
        update c_student_school_list set
        enrolled_school_flag = 0, active_flag = 0;
        update c_student set
        active_flag = 0;
        update c_student_year set
        active_flag = 0;
    end if;
    
    select  school_year_id
    into    @curr_sy_id
    from    c_school_year
    where   active_flag = 1
    ;

    select  ethnicity_id
    into    v_unknown_race_id
    from    c_ethnicity
    where   ethnicity_code = 'u'
    ;

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  count(*)
    into    v_view_ethn_exists
    from    information_schema.views t
    where   t.table_schema = database()
    and     t.table_name = v_ods_ethn_view;
    

    insert tmp_ethnicity_race_code_list (
        client_ethn_race_code
        ,pmi_ethn_race_code
    )
    
    select  client_race_code
        ,pmi_race_code
    from    v_pmi_xref_race
    ;

    # only proceed if this dataset is implemented for site
    if v_view_ethn_exists > 0 then
    
        select  count(*)
        into    v_hisp_ethn_value_exists
        from    v_pmi_xref_ethnicity
        where   pmi_ethnicity_code = v_pmi_std_hisp_ethn_code
        ;
    
    end if;

    if v_hisp_ethn_value_exists > 0 then

        delete from tmp_ethnicity_race_code_list
        where   pmi_ethn_race_code = v_pmi_std_hisp_ethn_code
        ;

        insert tmp_ethnicity_race_code_list (
            client_ethn_race_code
            ,pmi_ethn_race_code
        )
        
        select  client_ethnicity_code
            ,pmi_ethnicity_code
        from    v_pmi_xref_ethnicity
        where   pmi_ethnicity_code = v_pmi_std_hisp_ethn_code
        on duplicate key update pmi_ethn_race_code = values(pmi_ethn_race_code)
        ;

    end if;

    # Obtain a new id only for records that are not already in the target table.
    insert tmp_id_assign (new_id, base_code)
    select  pmi_f_get_next_sequence_app_db('c_student', 1), ods.student_id
    from    v_pmi_ods_student as ods
    left join   c_student as tar
            on      ods.student_id = tar.student_code
    where   ods.student_id is not null
    and     tar.student_id is null
    group by ods.student_id
    ;      
    
    insert into c_student (
        student_id
        ,student_code
        ,student_state_code
        ,fid_code
        ,last_name
        ,first_name
        ,long_name
        ,gender_flag
        ,ethnicity_id
        ,date_of_birth
        ,iep_date
        ,504_accom
        ,last_user_id
        ,client_id
        ,active_flag
        ,create_timestamp
        ) 
    select   coalesce(tmpid.new_id, tar.student_id)
            ,dt.student_id
            ,dt.student_eid
            ,dt.student_ssn
            ,dt.last_name
            ,dt.first_name
            ,concat(dt.last_name, ', ',dt.first_name)
            ,g.gender_flag
            ,coalesce(e.ethnicity_id, v_unknown_race_id)
            ,str_to_date(dt.date_of_birth, v_dob_date_format_mask)
            ,str_to_date(dt.iep_date, v_iep_date_format_mask)
            ,dt.504_accom
            ,1234
            ,@client_id
            ,1
            ,now()
    from     (
                select     o.student_id
                        ,max(o.student_eid) as student_eid
                        ,max(o.student_ssn) as student_ssn
                        ,max(o.last_name) as last_name
                        ,max(o.first_name) as first_name
                        ,max(o.gender_code) as gender_code
                        ,max(case when xe.pmi_ethnicity_code = v_pmi_std_hisp_ethn_code then o.ethnicity_code else o.race_code end) as race_code
                        ,max(o.date_of_birth) as date_of_birth
                        ,max(o.iep_date) as iep_date
                        ,max(504_accom) as 504_accom
                from     v_pmi_ods_student as o
                left join   v_pmi_xref_ethnicity as xe
                        on      o.ethnicity_code = xe.client_ethnicity_code
                where    o.student_id is not null 
                group by o.student_id
                    ) as dt
    join    v_pmi_xref_gender as g
            on      g.client_gender_code = dt.gender_code
    left join   tmp_ethnicity_race_code_list as xe
            on      xe.client_ethn_race_code = dt.race_code
    left join   c_ethnicity as e
            on      e.ethnicity_code = xe.pmi_ethn_race_code
    left join   tmp_id_assign as tmpid
            on      dt.student_id = tmpid.base_code
    left join   c_student as tar
            on      dt.student_id = tar.student_code
    on duplicate key update last_user_id = 1234
        ,student_state_code = values(student_state_code)
        ,fid_code = values(fid_code)
        ,gender_flag = values(gender_flag)
        ,ethnicity_id = values(ethnicity_id)
        ,active_flag = values(active_flag)
        ,last_name = values(last_name)
        ,first_name = values(first_name)
        ,long_name = values(long_name)
        ,date_of_birth = values(date_of_birth)
        ,iep_date = values(iep_date)
        ,504_accom = values(504_accom)
    ;
    
    insert into c_student_year (
        student_id
        ,school_year_id
        ,school_id
        ,grade_level_id
        ,migrant_flag
        ,active_flag
        ,cumulative_gpa
        ,last_user_id
        ,client_id
        ,create_timestamp
        ) 
        
    select  s.student_id
        ,@curr_sy_id
        ,max(sch.school_id) as school_id
        ,max(gl.grade_level_id) as grade_level_id
        ,max(coalesce(m.migrant_flag,0)) as migrant_flag
        ,1
        ,max(cumulative_gpa)
        ,1234
        ,@client_id
        ,now()
        
    from    c_student as s
    join    v_pmi_ods_student as o
            on      o.student_id = s.student_code
            and     o.student_id is not null
    join    c_school as sch
            on      sch.school_code = o.school_id
    join    v_pmi_xref_grade_level as x
            on      x.client_grade_code = o.grade_level
    join    c_grade_level as gl
            on      gl.grade_code = x.pmi_grade_code
    left join   v_pmi_xref_migrant m
            on      m.client_migrant_code = o.migrant_code
    where   s.active_flag = 1
    group by s.student_id
    on duplicate key update last_user_id = 1234
            ,school_id = values(school_id)
            ,grade_level_id = values(grade_level_id)
            ,migrant_flag = values(migrant_flag)
            ,active_flag = values(active_flag)
            ,cumulative_gpa = values(cumulative_gpa)
    ;

    # Set NCLB flag and codes
    update  c_student_year as sty
    join    c_student as st
            on      st.student_id = sty.student_id
    join    v_pmi_ods_student as ods
            on      ods.student_id = st.student_code
    left join   v_pmi_xref_swd as xswd
            on      ods.swd_code = xswd.client_swd_code
    left join   c_swd_type as cswd
            on      xswd.pmi_swd_code = cswd.swd_code
    left join   v_pmi_xref_lep as xlep
            on      ods.lep_code = xlep.client_lep_code
    left join   c_lep_type as clep
            on      xlep.pmi_lep_code = clep.lep_code
    left join   v_pmi_xref_econ_disadv as xed
            on      ods.ed_code = xed.client_econ_disadv_code
    left join   c_ed_type as ced
            on      xed.pmi_econ_disadv_code = ced.ed_code
    left join   v_pmi_xref_gifted as xgft
            on      ods.gifted_code = xgft.client_gifted_code
    left join   v_pmi_xref_title1 as xttl1
            on      ods.title1_code = xttl1.client_title1_code
    set     sty.swd_flag = coalesce(xswd.swd_flag, 0)
            ,sty.swd_id = cswd.swd_id
            ,sty.lep_flag = coalesce(xlep.lep_flag, 0)
            ,sty.lep_id = clep.lep_id
            ,sty.econ_disadv_flag = coalesce(xed.econ_disadv_flag, 0)
            ,sty.ed_id = ced.ed_id
            ,sty.gifted_flag = coalesce(xgft.gifted_flag, 0)
            ,sty.title1_flag = coalesce(xttl1.title1_flag, 0)
    where   sty.school_year_id = @curr_sy_id
    and     sty.active_flag = 1
    ;


    insert into c_student_school_list (
        student_id, 
        school_year_id, 
        school_id, 
        enrolled_school_flag, 
        active_flag, 
        last_user_id,
        create_timestamp
        )
    select   st.student_id,
            sy.school_year_id,
            sc.school_id,
            case  when  sc.school_id = sty.school_id then 1 else 0 end as enrolled_school_flag,
            1,
            1234,
            current_timestamp
    from c_student st
    join     v_pmi_ods_student o
                        on    o.student_id = st.student_code
    join     c_school_year as sy
                        on    sy.active_flag = 1
    join     c_student_year as sty
                        on    sty.student_id = st.student_id
                        and   sty.school_year_id = sy.school_year_id
    join     c_school as sc
                        on    sc.school_id = sty.school_id
    on duplicate key update last_user_id = 1234
            ,enrolled_school_flag = values(enrolled_school_flag)
            ,active_flag = 1
    ;

                    

    if v_useincrementalschedule = 'y' then 
    
        update  c_student_school_list sl
        join    c_school_year sy
                on      sy.school_year_id = sl.school_year_id
                and     sy.active_flag = 0
        set     sl.active_flag = 0, sl.enrolled_school_flag = 0
        ;
 
        update  c_student_school_list sl
        join    c_student_year sy
                on      sl.student_id = sy.student_id
                and     sl.school_year_id = sy.school_year_id
                and     sl.school_id = sy.school_id
                and     sy.active_flag = 1
        join    c_school_year yr
                on      sy.school_year_id = yr.school_year_id
                and     yr.active_flag = 1 
        set     sl.enrolled_school_flag = 1, sl.active_flag = 1
        ;    
    
        update  c_student s
        left join   v_pmi_ods_student stu
                on      stu.student_id = s.student_code
        set     s.active_flag = 
                case when stu.student_id is not null then 1
                    when stu.student_id is null then 0
                end
        ;
        
    end if;

    # if current student year record is inactive after above process,
    # need to deactivate the student record
    update  c_student as st
    join    c_student_year as sty
            on      st.student_id = sty.student_id
            and     sty.school_year_id = @curr_sy_id
    set     st.active_flag = sty.active_flag
    ;
    
    /* this is a temp fix, function needs to be added uploader proc's */    
    update c_student set
    first_name = replace(first_name, '"', '`'),
    last_name = replace(last_name, '"', '`')
    where (first_name like '%"%' or last_name like '%"%');

    update c_student set
    first_name = replace(first_name, '\'',''),
    last_name = replace(last_name, '\'','')
    where first_name like '%\'%'
    or last_name like '%\'%'; 

    # Grad Report update
    update  c_student as st
    join    c_student_year as sty
            on      st.student_id = sty.student_id
            and     sty.school_year_id = @curr_sy_id
    join    c_school as sch
            on      sty.school_id = sch.school_id
    join    c_school_type as scht
            on      sch.school_type_id = scht.school_type_id
            and     scht.school_type_code = 'hs'
    set     st.grad_eligible_flag = 1
    ;

    update  c_student as st
    join    v_pmi_ods_student as o
            on      o.student_id = st.student_code
            and     o.student_id is not null
    join    v_pmi_xref_grad_exempt as ge
            on      ge.client_grad_exempt_code = o.grad_exempt_code
    set     st.grad_exempt_flag = coalesce(ge.grad_exempt_flag, 0)
            ,st.grad_reqs_met_flag = case when coalesce(ge.grad_exempt_flag, 0) = 1 then 1 else st.grad_reqs_met_flag end
    ;

    # insert into c_student_contact_info
    insert c_student_contact_info 
    (student_id,  guardian_1_full_name, guardian_2_full_name, address_1, address_2, phone_1, phone_2, city, zipcode, state_id )
    select c.student_id,
    case
        when ods.guardian_1_full_name is null then concat(ods.guardian_1_last_name,', ',ods.guardian_1_first_name)
        else ods.guardian_1_full_name 
    end as g1_full_name,
    case
       when ods.guardian_2_full_name is null then concat(ods.guardian_2_last_name,', ',ods.guardian_2_first_name)
      else ods.guardian_2_full_name 
    end as g2_full_name,
    ods.address_1,
    ods.address_2,
    ods.phone_1,
    ods.phone_2,
    ods.city,
    ods.zipcode,
    state.state_id
    from v_pmi_ods_student as ods
    join c_student as c
      on ods.student_id = c.student_code
    join pmi_state_info as state
      on state.state_abbr = ods.state_abbr
    on duplicate key update guardian_1_full_name = values(guardian_1_full_name)
      ,guardian_2_full_name = values(guardian_2_full_name)
      ,address_1 = values(address_1)
      ,address_2 = values(address_2)
      ,phone_1 = values(phone_1)
      ,phone_2 = values(phone_2)
      ,city = values(city)
      ,zipcode = values(zipcode)
      ,state_id = values(state_id);

    # Cleanup
    drop table if exists `tmp_id_assign`;
    drop table if exists `tmp_ethnicity_race_code_list`;

    #################
    ## Update Log
    #################
    set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', v_ods_table, '\', \'P\', \'ETL Load Successful\')');
    
    prepare sql_scan_log from @sql_scan_log;
    execute sql_scan_log;
    deallocate prepare sql_scan_log;

end;
//
