drop procedure if exists etl_rpt_profile_bm_subgroup_class//

create definer=`dbadmin`@`localhost` procedure etl_rpt_profile_bm_subgroup_class()
contains sql
sql security invoker
comment '$Rev$ $Date$'


proc: begin 

    declare no_more_rows                boolean; 
    declare v_ayp_subject_id            int(11) default '0';
    declare v_ayp_strand_id             int(11) default '0';
    declare v_last_subject_id           int(11) default '0';
    declare v_curr_school_year_id       int(11);

    declare cur_ayp_sub cursor for 
        select  ayp_subject_id, ayp_strand_id
        from    rpt_profile_bm_stu
        group by ayp_subject_id, ayp_strand_id
        ;

    declare continue handler for not found 
    set no_more_rows = true;

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    select  school_year_id
    into    v_curr_school_year_id
    from    c_school_year
    where   active_flag = 1
    ;

    truncate table `rpt_profile_bm_subgroup_class`;
#     optimize table `rpt_profile_bm_subgroup_class`;
    

    # Populate table - looping by subject
    open cur_ayp_sub;
    loop_cur_ayp_sub: loop
    
        set v_last_subject_id = v_ayp_subject_id;

        fetch  cur_ayp_sub 
        into   v_ayp_subject_id, v_ayp_strand_id;
               
        if no_more_rows then
            close cur_ayp_sub;
            leave loop_cur_ayp_sub;
        end if;


        insert rpt_profile_bm_subgroup_class (
            ayp_curriculum_id
            ,class_id
            ,ayp_group_id
            ,ayp_subject_id
            ,ayp_strand_id
            ,bm_01_pe
            ,bm_01_pp
            ,bm_02_pe
            ,bm_02_pp
            ,bm_03_pe
            ,bm_03_pp
            ,bm_04_pe
            ,bm_04_pp
            ,bm_05_pe
            ,bm_05_pp
            ,bm_06_pe
            ,bm_06_pp
            ,bm_07_pe
            ,bm_07_pp
            ,bm_08_pe
            ,bm_08_pp
            ,total_pe
            ,total_pp
            ,last_user_id
        )
        
        select  rpbs.curriculum_id
            ,cle.class_id
            ,rsg.ayp_group_id
            ,v_ayp_subject_id
            ,v_ayp_strand_id
            ,sum(rpbs.bm_01_pe) as bm_01_pe
            ,sum(rpbs.bm_01_pp) as bm_01_pp
            ,sum(rpbs.bm_02_pe) as bm_02_pe
            ,sum(rpbs.bm_02_pp) as bm_02_pp
            ,sum(rpbs.bm_03_pe) as bm_03_pe
            ,sum(rpbs.bm_03_pp) as bm_03_pp
            ,sum(rpbs.bm_04_pe) as bm_04_pe
            ,sum(rpbs.bm_04_pp) as bm_04_pp
            ,sum(rpbs.bm_05_pe) as bm_05_pe
            ,sum(rpbs.bm_05_pp) as bm_05_pp
            ,sum(rpbs.bm_06_pe) as bm_06_pe
            ,sum(rpbs.bm_06_pp) as bm_06_pp
            ,sum(rpbs.bm_07_pe) as bm_07_pe
            ,sum(rpbs.bm_07_pp) as bm_07_pp
            ,sum(rpbs.bm_08_pe) as bm_08_pe
            ,sum(rpbs.bm_08_pp) as bm_08_pp
            ,sum(rpbs.total_pe) as total_pe
            ,sum(rpbs.total_pp) as total_pp
            ,1234
        
        from    rpt_profile_bm_stu as rpbs
        join    c_student_year as sty
                on      sty.student_id = rpbs.student_id
                and     sty.school_year_id = v_curr_school_year_id
                and     sty.active_flag = 1
        join    rpt_student_group as rsg
                on      rpbs.student_id = rsg.student_id
                and     rsg.school_year_id = sty.school_year_id
        join    c_ayp_group as ag
                on      rsg.ayp_group_id = ag.ayp_group_id
                and     ag.ayp_group_code = 'all'
                and     ag.ayp_accel_flag = 1
        join    c_class_enrollment as cle
                on      rpbs.student_id = cle.student_id
        where   rpbs.ayp_subject_id = v_ayp_subject_id
        and     rpbs.ayp_strand_id = v_ayp_strand_id
        group by rpbs.curriculum_id, cle.class_id, rsg.ayp_group_id
        ;

        # add strand level aggs

        insert rpt_profile_bm_subgroup_class (
            ayp_curriculum_id
            ,class_id
            ,ayp_group_id
            ,ayp_subject_id
            ,ayp_strand_id
            ,bm_01_pe
            ,bm_01_pp
            ,bm_02_pe
            ,bm_02_pp
            ,bm_03_pe
            ,bm_03_pp
            ,bm_04_pe
            ,bm_04_pp
            ,bm_05_pe
            ,bm_05_pp
            ,bm_06_pe
            ,bm_06_pp
            ,bm_07_pe
            ,bm_07_pp
            ,bm_08_pe
            ,bm_08_pp
            ,total_pe
            ,total_pp
            ,last_user_id
        )
        
        select  v_ayp_strand_id
            ,class_id
            ,ayp_group_id
            ,v_ayp_subject_id
            ,v_ayp_strand_id
            ,sum(rpbsc.bm_01_pe) as bm_01_pe
            ,sum(rpbsc.bm_01_pp) as bm_01_pp
            ,sum(rpbsc.bm_02_pe) as bm_02_pe
            ,sum(rpbsc.bm_02_pp) as bm_02_pp
            ,sum(rpbsc.bm_03_pe) as bm_03_pe
            ,sum(rpbsc.bm_03_pp) as bm_03_pp
            ,sum(rpbsc.bm_04_pe) as bm_04_pe
            ,sum(rpbsc.bm_04_pp) as bm_04_pp
            ,sum(rpbsc.bm_05_pe) as bm_05_pe
            ,sum(rpbsc.bm_05_pp) as bm_05_pp
            ,sum(rpbsc.bm_06_pe) as bm_06_pe
            ,sum(rpbsc.bm_06_pp) as bm_06_pp
            ,sum(rpbsc.bm_07_pe) as bm_07_pe
            ,sum(rpbsc.bm_07_pp) as bm_07_pp
            ,sum(rpbsc.bm_08_pe) as bm_08_pe
            ,sum(rpbsc.bm_08_pp) as bm_08_pp
            ,sum(rpbsc.total_pe) as total_pe
            ,sum(rpbsc.total_pp) as total_pp
            ,1234
        
        from    rpt_profile_bm_subgroup_class as rpbsc
        where   rpbsc.ayp_subject_id = v_ayp_subject_id
        and     rpbsc.ayp_strand_id = v_ayp_strand_id
        and     rpbsc.ayp_curriculum_id != v_ayp_strand_id
        group by rpbsc.class_id, rpbsc.ayp_group_id
        on duplicate key update bm_01_pe = bm_01_pe + values(bm_01_pe)
            ,bm_01_pp = bm_01_pp + values(bm_01_pp)
            ,bm_02_pe = bm_02_pe + values(bm_02_pe)
            ,bm_02_pp = bm_02_pp + values(bm_02_pp)
            ,bm_03_pe = bm_03_pe + values(bm_03_pe)
            ,bm_03_pp = bm_03_pp + values(bm_03_pp)
            ,bm_04_pe = bm_04_pe + values(bm_04_pe)
            ,bm_04_pp = bm_04_pp + values(bm_04_pp)
            ,bm_05_pe = bm_05_pe + values(bm_05_pe)
            ,bm_05_pp = bm_05_pp + values(bm_05_pp)
            ,bm_06_pe = bm_06_pe + values(bm_06_pe)
            ,bm_06_pp = bm_06_pp + values(bm_06_pp)
            ,bm_07_pe = bm_07_pe + values(bm_07_pe)
            ,bm_07_pp = bm_07_pp + values(bm_07_pp)
            ,bm_08_pe = bm_08_pe + values(bm_08_pe)
            ,bm_08_pp = bm_08_pp + values(bm_08_pp)
            ,total_pe = total_pe + values(total_pe)
            ,total_pp = total_pp + values(total_pp)
        ;


        # add subject level aggs
        if v_ayp_subject_id != v_last_subject_id and v_last_subject_id != 0 then

            insert rpt_profile_bm_subgroup_class (
                ayp_curriculum_id
                ,class_id
                ,ayp_group_id
                ,ayp_subject_id
                ,ayp_strand_id
                ,bm_01_pe
                ,bm_01_pp
                ,bm_02_pe
                ,bm_02_pp
                ,bm_03_pe
                ,bm_03_pp
                ,bm_04_pe
                ,bm_04_pp
                ,bm_05_pe
                ,bm_05_pp
                ,bm_06_pe
                ,bm_06_pp
                ,bm_07_pe
                ,bm_07_pp
                ,bm_08_pe
                ,bm_08_pp
                ,total_pe
                ,total_pp
                ,last_user_id
            )
            
            select  v_last_subject_id
                ,class_id
                ,ayp_group_id
                ,v_last_subject_id
                ,0
                ,sum(rpbsc.bm_01_pe) as bm_01_pe
                ,sum(rpbsc.bm_01_pp) as bm_01_pp
                ,sum(rpbsc.bm_02_pe) as bm_02_pe
                ,sum(rpbsc.bm_02_pp) as bm_02_pp
                ,sum(rpbsc.bm_03_pe) as bm_03_pe
                ,sum(rpbsc.bm_03_pp) as bm_03_pp
                ,sum(rpbsc.bm_04_pe) as bm_04_pe
                ,sum(rpbsc.bm_04_pp) as bm_04_pp
                ,sum(rpbsc.bm_05_pe) as bm_05_pe
                ,sum(rpbsc.bm_05_pp) as bm_05_pp
                ,sum(rpbsc.bm_06_pe) as bm_06_pe
                ,sum(rpbsc.bm_06_pp) as bm_06_pp
                ,sum(rpbsc.bm_07_pe) as bm_07_pe
                ,sum(rpbsc.bm_07_pp) as bm_07_pp
                ,sum(rpbsc.bm_08_pe) as bm_08_pe
                ,sum(rpbsc.bm_08_pp) as bm_08_pp
                ,sum(rpbsc.total_pe) as total_pe
                ,sum(rpbsc.total_pp) as total_pp
                ,1234
            
            from    rpt_profile_bm_subgroup_class as rpbsc
            where   rpbsc.ayp_subject_id = v_last_subject_id
            and     rpbsc.ayp_curriculum_id = rpbsc.ayp_strand_id
            group by rpbsc.class_id, rpbsc.ayp_group_id
            on duplicate key update bm_01_pe = bm_01_pe + values(bm_01_pe)
                ,bm_01_pp = bm_01_pp + values(bm_01_pp)
                ,bm_02_pe = bm_02_pe + values(bm_02_pe)
                ,bm_02_pp = bm_02_pp + values(bm_02_pp)
                ,bm_03_pe = bm_03_pe + values(bm_03_pe)
                ,bm_03_pp = bm_03_pp + values(bm_03_pp)
                ,bm_04_pe = bm_04_pe + values(bm_04_pe)
                ,bm_04_pp = bm_04_pp + values(bm_04_pp)
                ,bm_05_pe = bm_05_pe + values(bm_05_pe)
                ,bm_05_pp = bm_05_pp + values(bm_05_pp)
                ,bm_06_pe = bm_06_pe + values(bm_06_pe)
                ,bm_06_pp = bm_06_pp + values(bm_06_pp)
                ,bm_07_pe = bm_07_pe + values(bm_07_pe)
                ,bm_07_pp = bm_07_pp + values(bm_07_pp)
                ,bm_08_pe = bm_08_pe + values(bm_08_pe)
                ,bm_08_pp = bm_08_pp + values(bm_08_pp)
                ,total_pe = total_pe + values(total_pe)
                ,total_pp = total_pp + values(total_pp)
            ;

            update  rpt_profile_bm_subgroup_class
            set     bm_01_pct = round((bm_01_pe / bm_01_pp), 3) * 100
                    ,bm_02_pct = round((bm_02_pe / bm_02_pp), 3) * 100
                    ,bm_03_pct = round((bm_03_pe / bm_03_pp), 3) * 100
                    ,bm_04_pct = round((bm_04_pe / bm_04_pp), 3) * 100
                    ,bm_05_pct = round((bm_05_pe / bm_05_pp), 3) * 100
                    ,bm_06_pct = round((bm_06_pe / bm_06_pp), 3) * 100
                    ,bm_07_pct = round((bm_07_pe / bm_07_pp), 3) * 100
                    ,bm_08_pct = round((bm_08_pe / bm_08_pp), 3) * 100
                    ,total_pct = round((total_pe / total_pp), 3) * 100
            where   ayp_subject_id = v_last_subject_id
            ;

        end if;

    end loop loop_cur_ayp_sub;

    insert rpt_profile_bm_subgroup_class (
        ayp_curriculum_id
        ,class_id
        ,ayp_group_id
        ,ayp_subject_id
        ,ayp_strand_id
        ,bm_01_pe
        ,bm_01_pp
        ,bm_02_pe
        ,bm_02_pp
        ,bm_03_pe
        ,bm_03_pp
        ,bm_04_pe
        ,bm_04_pp
        ,bm_05_pe
        ,bm_05_pp
        ,bm_06_pe
        ,bm_06_pp
        ,bm_07_pe
        ,bm_07_pp
        ,bm_08_pe
        ,bm_08_pp
        ,total_pe
        ,total_pp
        ,last_user_id
    )
    
    select  v_last_subject_id
        ,class_id
        ,ayp_group_id
        ,v_last_subject_id
        ,0
        ,sum(rpbsc.bm_01_pe) as bm_01_pe
        ,sum(rpbsc.bm_01_pp) as bm_01_pp
        ,sum(rpbsc.bm_02_pe) as bm_02_pe
        ,sum(rpbsc.bm_02_pp) as bm_02_pp
        ,sum(rpbsc.bm_03_pe) as bm_03_pe
        ,sum(rpbsc.bm_03_pp) as bm_03_pp
        ,sum(rpbsc.bm_04_pe) as bm_04_pe
        ,sum(rpbsc.bm_04_pp) as bm_04_pp
        ,sum(rpbsc.bm_05_pe) as bm_05_pe
        ,sum(rpbsc.bm_05_pp) as bm_05_pp
        ,sum(rpbsc.bm_06_pe) as bm_06_pe
        ,sum(rpbsc.bm_06_pp) as bm_06_pp
        ,sum(rpbsc.bm_07_pe) as bm_07_pe
        ,sum(rpbsc.bm_07_pp) as bm_07_pp
        ,sum(rpbsc.bm_08_pe) as bm_08_pe
        ,sum(rpbsc.bm_08_pp) as bm_08_pp
        ,sum(rpbsc.total_pe) as total_pe
        ,sum(rpbsc.total_pp) as total_pp
        ,1234
    
    from    rpt_profile_bm_subgroup_class as rpbsc
    where   rpbsc.ayp_subject_id = v_last_subject_id
    and     rpbsc.ayp_curriculum_id = rpbsc.ayp_strand_id
    group by rpbsc.class_id, rpbsc.ayp_group_id
    on duplicate key update bm_01_pe = bm_01_pe + values(bm_01_pe)
        ,bm_01_pp = bm_01_pp + values(bm_01_pp)
        ,bm_02_pe = bm_02_pe + values(bm_02_pe)
        ,bm_02_pp = bm_02_pp + values(bm_02_pp)
        ,bm_03_pe = bm_03_pe + values(bm_03_pe)
        ,bm_03_pp = bm_03_pp + values(bm_03_pp)
        ,bm_04_pe = bm_04_pe + values(bm_04_pe)
        ,bm_04_pp = bm_04_pp + values(bm_04_pp)
        ,bm_05_pe = bm_05_pe + values(bm_05_pe)
        ,bm_05_pp = bm_05_pp + values(bm_05_pp)
        ,bm_06_pe = bm_06_pe + values(bm_06_pe)
        ,bm_06_pp = bm_06_pp + values(bm_06_pp)
        ,bm_07_pe = bm_07_pe + values(bm_07_pe)
        ,bm_07_pp = bm_07_pp + values(bm_07_pp)
        ,bm_08_pe = bm_08_pe + values(bm_08_pe)
        ,bm_08_pp = bm_08_pp + values(bm_08_pp)
        ,total_pe = total_pe + values(total_pe)
        ,total_pp = total_pp + values(total_pp)
    ;

    update  rpt_profile_bm_subgroup_class
    set     bm_01_pct = round((bm_01_pe / bm_01_pp), 3) * 100
            ,bm_02_pct = round((bm_02_pe / bm_02_pp), 3) * 100
            ,bm_03_pct = round((bm_03_pe / bm_03_pp), 3) * 100
            ,bm_04_pct = round((bm_04_pe / bm_04_pp), 3) * 100
            ,bm_05_pct = round((bm_05_pe / bm_05_pp), 3) * 100
            ,bm_06_pct = round((bm_06_pe / bm_06_pp), 3) * 100
            ,bm_07_pct = round((bm_07_pe / bm_07_pp), 3) * 100
            ,bm_08_pct = round((bm_08_pe / bm_08_pp), 3) * 100
            ,total_pct = round((total_pe / total_pp), 3) * 100
    where   ayp_subject_id = v_last_subject_id
    ;


    update   rpt_profile_bm_subgroup_class as  rpbsc
    join     c_color_ayp_benchmark cb
             on     cb.client_id = @client_id
             and    rpbsc.ayp_subject_id = cb.ayp_subject_id
             and    rpbsc.bm_01_pct between cb.min_score and cb.max_score
    join     pmi_color c
             on    c.color_id = cb.color_id
    set     rpbsc.bm_01_color = c.moniker
    ;

    update   rpt_profile_bm_subgroup_class as  rpbsc
    join     c_color_ayp_benchmark cb
             on     cb.client_id = @client_id
             and    rpbsc.ayp_subject_id = cb.ayp_subject_id
             and   rpbsc.bm_02_pct between cb.min_score and cb.max_score
    join     pmi_color c
             on    c.color_id = cb.color_id
    set     rpbsc.bm_02_color = c.moniker
    ;

    update   rpt_profile_bm_subgroup_class as  rpbsc
    join     c_color_ayp_benchmark cb
             on     cb.client_id = @client_id
             and    rpbsc.ayp_subject_id = cb.ayp_subject_id
             and   rpbsc.bm_03_pct between cb.min_score and cb.max_score
    join     pmi_color c
             on    c.color_id = cb.color_id
    set     rpbsc.bm_03_color = c.moniker
    ;

    update   rpt_profile_bm_subgroup_class as  rpbsc
    join     c_color_ayp_benchmark cb
             on     cb.client_id = @client_id
             and    rpbsc.ayp_subject_id = cb.ayp_subject_id
             and   rpbsc.bm_04_pct between cb.min_score and cb.max_score
    join     pmi_color c
             on    c.color_id = cb.color_id
    set     rpbsc.bm_04_color = c.moniker
    ;

    update   rpt_profile_bm_subgroup_class as  rpbsc
    join     c_color_ayp_benchmark cb
             on     cb.client_id = @client_id
             and    rpbsc.ayp_subject_id = cb.ayp_subject_id
             and   rpbsc.bm_05_pct between cb.min_score and cb.max_score
    join     pmi_color c
             on    c.color_id = cb.color_id
    set     rpbsc.bm_05_color = c.moniker
    ;

    update   rpt_profile_bm_subgroup_class as  rpbsc
    join     c_color_ayp_benchmark cb
             on     cb.client_id = @client_id
             and    rpbsc.ayp_subject_id = cb.ayp_subject_id
             and   rpbsc.bm_06_pct between cb.min_score and cb.max_score
    join     pmi_color c
             on    c.color_id = cb.color_id
    set     rpbsc.bm_06_color = c.moniker
    ;

    update   rpt_profile_bm_subgroup_class as  rpbsc
    join     c_color_ayp_benchmark cb
             on     cb.client_id = @client_id
             and    rpbsc.ayp_subject_id = cb.ayp_subject_id
             and   rpbsc.bm_07_pct between cb.min_score and cb.max_score
    join     pmi_color c
             on    c.color_id = cb.color_id
    set     rpbsc.bm_07_color = c.moniker
    ;

    update   rpt_profile_bm_subgroup_class as  rpbsc
    join     c_color_ayp_benchmark cb
             on     cb.client_id = @client_id
             and    rpbsc.ayp_subject_id = cb.ayp_subject_id
             and   rpbsc.bm_08_pct between cb.min_score and cb.max_score
    join     pmi_color c
             on    c.color_id = cb.color_id
    set     rpbsc.bm_08_color = c.moniker
    ;

    update   rpt_profile_bm_subgroup_class as  rpbsc
    join     c_color_ayp_benchmark cb
             on     cb.client_id = @client_id
             and    rpbsc.ayp_subject_id = cb.ayp_subject_id
             and   rpbsc.total_pct between cb.min_score and cb.max_score
    join     pmi_color c
             on    c.color_id = cb.color_id
    set     rpbsc.bm_08_color = c.moniker
    ;


end proc;
//
