/*
$Rev: 9643 $ 
$Author: randall.stanley $ 
$Date: 2010-11-05 22:55:00 -0400 (Fri, 05 Nov 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bbcard_detail_lag_lead_hst_strand.sql $
$Id: etl_rpt_bbcard_detail_lag_lead_hst_strand.sql 9643 2010-11-06 02:55:00Z randall.stanley $ 
*/

drop procedure if exists etl_rpt_bbcard_detail_lag_lead_hst_strand//

create definer=`dbadmin`@`localhost` procedure etl_rpt_bbcard_detail_lag_lead_hst_strand()
contains sql
sql security invoker
comment '$Rev: 9643 $ $Date: 2010-11-05 22:55:00 -0400 (Fri, 05 Nov 2010) $'


proc: begin 

    declare v_curr_yr_id        int(11);
    declare v_num_round_digits  int(11) default '0';

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    drop table if exists `tmp_bb_measure_strand_xref`;
    create table `tmp_bb_measure_strand_xref` (
      `ayp_subject_id` int(10) not null,
      `ayp_strand_id` int(10) not null,
      `ayp_strand_code` varchar(40) not null,
      `bb_group_id` int(10) not null,
      `bb_measure_id` int(10) not null,
      primary key (`ayp_subject_id`,`ayp_strand_id`),
      unique key `uq_tmp_bb_measure_strand_xref` (`ayp_subject_id`,`ayp_strand_code`),
      key `ind_tmp_bb_measure_strand_xref` (`bb_group_id`,`bb_measure_id`)
    ) engine=innodb default charset=latin1
    ;

    select  school_year_id
    into    v_curr_yr_id
    from    c_school_year
    where   active_flag = 1
    ;

    select  number_round_digits
    into    v_num_round_digits
    from    pmi_admin.pmi_client
    where   client_id = @client_id
    ;

    delete from `rpt_bbcard_detail_lag_lead_hst_strand`;

    insert tmp_bb_measure_strand_xref (
        ayp_subject_id
        ,ayp_strand_id
        ,ayp_strand_code
        ,bb_group_id
        ,bb_measure_id
    )
    
    select  str.ayp_subject_id
        ,str.ayp_strand_id
        ,str.ayp_strand_code
        ,bg.bb_group_id
        ,bm.bb_measure_id

    from    c_ayp_strand as str
    join    pm_bbcard_group as bg
            on      bg.bb_group_code = 'lagLeadStrand'
    join    pm_bbcard_measure as bm
            on      bg.bb_group_id = bm.bb_group_id
            and     bm.bb_measure_code = str.ayp_strand_code
    ;


    ##############################################################
    # Insert Leading data
    ##############################################################

    insert into rpt_bbcard_detail_lag_lead_hst_strand (
        bb_group_id
        ,bb_measure_id
        ,bb_measure_item_id
        ,student_id
        ,school_year_id
        ,score
        ,score_color
        ,score_type
        ,last_user_id
        ,create_timestamp
    ) 
    # leading - bm
    select  dt.bb_group_id
        ,dt.bb_measure_id
        ,dt.bb_measure_item_id
        ,dt.student_id
        ,v_curr_yr_id
        ,dt.score
        ,dt.color
        ,'n'
        ,1234
        ,now()

    from    (
                select  bbg.bb_group_id
                    ,bbm.bb_measure_id
                    ,bbmi.bb_measure_item_id
                    ,rptlead.student_id
                    ,case when bbmi.bb_measure_item_code = 'bm01' then round(rptlead.bm_01_pe/ rptlead.bm_01_pp * 100, v_num_round_digits)
                            when bbmi.bb_measure_item_code = 'bm02' then round(rptlead.bm_02_pe/ rptlead.bm_02_pp * 100, v_num_round_digits)
                            when bbmi.bb_measure_item_code = 'bm03' then round(rptlead.bm_03_pe/ rptlead.bm_03_pp * 100, v_num_round_digits)
                            when bbmi.bb_measure_item_code = 'bm04' then round(rptlead.bm_04_pe/ rptlead.bm_04_pp * 100, v_num_round_digits)
                            when bbmi.bb_measure_item_code = 'bm05' then round(rptlead.bm_05_pe/ rptlead.bm_05_pp * 100, v_num_round_digits)
                            when bbmi.bb_measure_item_code = 'bm06' then round(rptlead.bm_06_pe/ rptlead.bm_06_pp * 100, v_num_round_digits)
                            when bbmi.bb_measure_item_code = 'bm07' then round(rptlead.bm_07_pe/ rptlead.bm_07_pp * 100, v_num_round_digits)
                            when bbmi.bb_measure_item_code = 'bm08' then round(rptlead.bm_08_pe/ rptlead.bm_08_pp * 100, v_num_round_digits)
                            when bbmi.bb_measure_item_code = 'wtavg' then round(rptlead.total_pe/ rptlead.total_pp * 100, v_num_round_digits)
                            else null end as score
                    ,case when bbmi.bb_measure_item_code = 'bm01' then rptlead.bm_01_color
                            when bbmi.bb_measure_item_code = 'bm02' then rptlead.bm_02_color
                            when bbmi.bb_measure_item_code = 'bm03' then rptlead.bm_03_color
                            when bbmi.bb_measure_item_code = 'bm04' then rptlead.bm_04_color
                            when bbmi.bb_measure_item_code = 'bm05' then rptlead.bm_05_color
                            when bbmi.bb_measure_item_code = 'bm06' then rptlead.bm_06_color
                            when bbmi.bb_measure_item_code = 'bm07' then rptlead.bm_07_color
                            when bbmi.bb_measure_item_code = 'bm08' then rptlead.bm_08_color
                            when bbmi.bb_measure_item_code = 'wtavg' then rptlead.wavg_color
                            else null end as color

                from    rpt_profile_leading_strand_stu as rptlead
                cross join  pm_bbcard_group as bbg
                        on      bbg.bb_group_code = 'lagLeadStrand'
                join    tmp_bb_measure_strand_xref as xref
                        on      rptlead.ayp_subject_id = xref.ayp_subject_id
                        and     rptlead.ayp_strand_id = xref.ayp_strand_id
                join    pm_bbcard_measure as bbm
                        ON      bbm.bb_group_id = xref.bb_group_id
                        AND     bbm.bb_measure_id = xref.bb_measure_id
                join    pm_bbcard_measure_item as bbmi
                        on      bbmi.bb_group_id =  bbg.bb_group_id
                        and     bbmi.bb_measure_id = bbm.bb_measure_id
                        and     bbmi.bb_measure_item_code in ('wtavg','bm01','bm02','bm03','bm04','bm05','bm06','bm07','bm08')
            ) as dt
    where score is not null
    ;
                    

    ##############################################################
    # Insert lagging data
    ##############################################################

        insert into rpt_bbcard_detail_lag_lead_hst_strand
        (
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
        select  bbg.bb_group_id
            ,bbm.bb_measure_id
            ,mi.bb_measure_item_id
            ,astrst.student_id
            ,astrst.school_year_id
            ,astrst.ayp_score
            ,astrst.score_type_code
            ,astrst.ayp_score_color
            ,1234
            ,now()
        from    c_ayp_strand_student as astrst
        join    c_student_year as sty
                on      sty.student_id = astrst.student_id
                and     sty.school_year_id = astrst.school_year_id
        join    c_grade_level as gl
                on      gl.grade_level_id = sty.grade_level_id
        join    pm_bbcard_group as bbg
                on      bbg.bb_group_code = 'lagLeadStrand'
        join    tmp_bb_measure_strand_xref as xref
                on      astrst.ayp_subject_id = xref.ayp_subject_id
                and     astrst.ayp_strand_id = xref.ayp_strand_id
        join    pm_bbcard_measure as bbm
                on      bbm.bb_group_id = xref.bb_group_id
                and     bbm.bb_measure_id = xref.bb_measure_id
        join    pm_bbcard_measure_item as mi
                on      mi.bb_group_id = bbm.bb_group_id
                and     mi.bb_measure_id = bbm.bb_measure_id
                and     mi.bb_measure_item_code = 'score'
        where   astrst.ayp_score is not null
        on duplicate key update last_user_id = 1234
        ;

    drop table if exists `tmp_bb_measure_strand_xref`;
        
end proc;
//
