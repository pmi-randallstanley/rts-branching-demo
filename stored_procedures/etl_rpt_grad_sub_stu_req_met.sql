DROP PROCEDURE IF EXISTS etl_rpt_grad_sub_stu_req_met //

create definer=`dbadmin`@`localhost` procedure etl_rpt_grad_sub_stu_req_met()
contains sql
sql security invoker
comment '$Rebuild etl_rpt_grad_sub_stu_req_met $'

proc: begin

    declare v_ayp_subject_id_reading int(10);
    declare v_ayp_subject_id_writing int(10);
    declare v_ayp_subject_id_math int(10);
    declare v_fail_color varchar(50);
    declare v_pass_color varchar(50);
    
    declare v_is_fl_client tinyint(1);
    declare v_is_ga_client tinyint(1);
    declare v_is_mn_client tinyint(1);
    declare v_is_ky_client tinyint(1);
    declare v_is_nc_client tinyint(1);
    declare v_is_ca_client tinyint(1);
    declare v_is_nj_client tinyint(1);
    declare v_is_il_client tinyint(1);
    declare v_is_oh_client tinyint(1);
    declare v_is_md_client tinyint(1);
    declare v_is_al_client tinyint(1);
    declare v_is_tx_client tinyint(1);
    declare v_is_wy_client tinyint(1);
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    SELECT  count(case when st.state_abbr = 'fl' then st.state_abbr end)
            ,count(case when st.state_abbr = 'ga' then st.state_abbr end)
            ,count(case when st.state_abbr = 'mn' then st.state_abbr end)
            ,count(case when st.state_abbr = 'ky' then st.state_abbr end)
            ,count(case when st.state_abbr = 'nc' then st.state_abbr end)
            ,count(case when st.state_abbr = 'ca' then st.state_abbr end)
            ,count(case when st.state_abbr = 'nj' then st.state_abbr end)
            ,count(case when st.state_abbr = 'il' then st.state_abbr end)
            ,count(case when st.state_abbr = 'oh' then st.state_abbr end)
            ,count(case when st.state_abbr = 'md' then st.state_abbr end)
            ,count(case when st.state_abbr = 'al' then st.state_abbr end)
            ,count(case when st.state_abbr = 'tx' then st.state_abbr end)
            ,count(case when st.state_abbr = 'wy' then st.state_abbr end)
    INTO    v_is_fl_client
            ,v_is_ga_client
            ,v_is_mn_client
            ,v_is_ky_client
            ,v_is_nc_client
            ,v_is_ca_client
            ,v_is_nj_client
            ,v_is_il_client
            ,v_is_oh_client
            ,v_is_md_client
            ,v_is_al_client
            ,v_is_tx_client
            ,v_is_wy_client
    FROM    pmi_admin.pmi_state AS st
    WHERE   st.state_id = @state_id
    AND     st.state_abbr in ('fl','ga','mn','ky','nc','ca','nj','il','oh','md','al','tx','wy');
 
    IF v_is_mn_client > 0 then

        # Get AYP Subject ID's
        select  ayp_subject_id into v_ayp_subject_id_reading
        from    c_ayp_subject sub
        where   sub.ayp_subject_code = 'gradRead'
        ;
        
        select  ayp_subject_id into v_ayp_subject_id_math
        from    c_ayp_subject sub
        where   sub.ayp_subject_code = 'gradMath'
        ;
        
        select  ayp_subject_id into v_ayp_subject_id_writing
        from    c_ayp_subject sub
        where   sub.ayp_subject_code = 'gradWriting'
        ;
        
        # Get Colors
        select clr.moniker 
        into   v_fail_color
        from c_color_swatch sw
        join  c_color_swatch_list swl
              on    swl.swatch_id = sw.swatch_id
              and   sw.swatch_code = 'gradReqMet'
        join  pmi_color clr
              on    swl.color_id = clr.color_id
        where swl.sort_order = 1
        ;
        
        select clr.moniker 
        into   v_pass_color
        from c_color_swatch sw
        join  c_color_swatch_list swl
              on    swl.swatch_id = sw.swatch_id
              and   sw.swatch_code = 'gradReqMet'
        join  pmi_color clr
              on    swl.color_id = clr.color_id
        where swl.sort_order = 2
        ;
        
        ## Delete existing records with 0 subject ID - we will be recalculating these
        delete from rpt_grad_sub_stu_req_met where ayp_subject_id = 0;
        
        insert into rpt_grad_sub_stu_req_met (
              ayp_subject_id
              ,student_id
              ,req_met_flag
              ,req_met_color
              ,last_user_id
              ,create_timestamp)
        select 0 as ayp_subject_id
              , csty.student_id
              , case
                  when gl.grade_code = 10 and dt_pivot.writing_req_met = 1 then 1
                  when gl.grade_code = 11 and dt_pivot.writing_req_met = 1 and dt_pivot.read_req_met = 1 then 1
                  when gl.grade_code = 12 and dt_pivot.writing_req_met = 1 and dt_pivot.read_req_met = 1 and dt_pivot.math_req_met = 1  then 1
                  else 0
                end as req_met_flag
              , case
                  when gl.grade_code = 10 and dt_pivot.writing_req_met = 1 then v_pass_color
                  when gl.grade_code = 11 and dt_pivot.writing_req_met = 1 and dt_pivot.read_req_met = 1 then v_pass_color
                  when gl.grade_code = 12 and dt_pivot.writing_req_met = 1 and dt_pivot.read_req_met = 1 and dt_pivot.math_req_met = 1  then v_pass_color
                  else v_fail_color
                end as req_met_color
              , 1234
              , now()
        from c_student_year csty
        join c_grade_level gl
              on csty.grade_level_id = gl.grade_level_id
              and   gl.grade_sequence >= 10
        left join (
                select dt.student_id
                        , sum(coalesce(dt.math_req_met,0)) as math_req_met
                        , sum(coalesce(dt.read_req_met,0)) as read_req_met
                        , sum(coalesce(dt.writing_req_met,0)) as writing_req_met
                from (
                        select student_id
                              , case when ayp_subject_id = v_ayp_subject_id_math then req_met_flag end as math_req_met
                              , case when ayp_subject_id = v_ayp_subject_id_reading then req_met_flag end as read_req_met
                              , case when ayp_subject_id = v_ayp_subject_id_writing then req_met_flag end as writing_req_met
                        from rpt_grad_sub_stu_req_met
                    ) dt
                group by dt.student_id
              ) dt_pivot
              on csty.student_id = dt_pivot.student_id
        where csty.active_flag = 1
        ;
    end if; ##v_is_mn_client

    
end proc;
//
