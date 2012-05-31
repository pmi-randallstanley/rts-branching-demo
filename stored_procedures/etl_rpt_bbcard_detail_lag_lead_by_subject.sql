/*
$Rev: 9930 $ 
$Author: randall.stanley $ 
$Date: 2011-01-26 08:55:39 -0500 (Wed, 26 Jan 2011) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bbcard_detail_lag_lead_by_subject.sql $
$Id: etl_rpt_bbcard_detail_lag_lead_by_subject.sql 9930 2011-01-26 13:55:39Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_rpt_bbcard_detail_lag_lead_hst_subject //

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_bbcard_detail_lag_lead_hst_subject()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 9930 $ $Date: 2011-01-26 08:55:39 -0500 (Wed, 26 Jan 2011) $'

PROC: BEGIN 

    declare v_curr_yr_id        int(11);
    declare v_num_round_digits  int(11) default '0';
    

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);


    drop table if exists `tmp_bb_measure_subject_xref`;
    create table `tmp_bb_measure_subject_xref` (
      `ayp_subject_id` int(10) not null,
      `ayp_subject_code` varchar(25) not null,
      `bb_group_id` int(10) not null,
      `bb_measure_id` int(10) not null,
      primary key (`ayp_subject_id`),
      unique key `uq_tmp_bb_measure_subject_xref` (`ayp_subject_code`),
      key `ind_tmp_bb_measure_subject_xref` (`bb_group_id`,`bb_measure_id`)
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
    
    insert tmp_bb_measure_subject_xref (
        ayp_subject_id
        ,ayp_subject_code
        ,bb_group_id
        ,bb_measure_id
    )
    
    select  sub.ayp_subject_id
        ,sub.ayp_subject_code
        ,bg.bb_group_id
        ,bm.bb_measure_id

    from    c_ayp_subject as sub
    join    pm_bbcard_group as bg
            on      bg.bb_group_code = 'lagLeadSubject'
    join    pm_bbcard_measure as bm
            on      bg.bb_group_id = bm.bb_group_id
            and     bm.bb_measure_code = sub.ayp_subject_code
    ;
    

    #=========
#    DELETE FROM `rpt_bbcard_detail_lag_lead_hst_subject`
#    WHERE bb_group_id IN (SELECT bb_group_id
#                          FROM pm_baseball_group
#                          WHERE bb_group_code IN ('hst_subject'));
    #=========

#    flush table `rpt_bbcard_detail_lag_lead_hst_subject`;
#    truncate table `rpt_bbcard_detail_lag_lead_hst_subject`;

    delete from rpt_bbcard_detail_lag_lead_hst_subject;

    ##############################################################
    # Insert Leading data
    ##############################################################

    INSERT INTO rpt_bbcard_detail_lag_lead_hst_subject
        (
        bb_group_id
        , bb_measure_id
        , bb_measure_item_id
        , student_id
        , school_year_id
        , score
        , score_color
        , score_type
        , last_user_id
        , create_timestamp
        ) 
    # Leading - bm
    SELECT *
    FROM (
            SELECT   bbg.bb_group_id
                    ,bbm.bb_measure_id
                    ,bbmi.bb_measure_item_id
                    ,rptlead.student_id
                    ,v_curr_yr_id
                    ,CASE WHEN bbmi.bb_measure_item_code = 'BM01' THEN round(rptlead.bm_01_pe/ rptlead.bm_01_pp * 100, v_num_round_digits)
                            WHEN bbmi.bb_measure_item_code = 'BM02' THEN round(rptlead.bm_02_pe/ rptlead.bm_02_pp * 100, v_num_round_digits)
                            WHEN bbmi.bb_measure_item_code = 'BM03' THEN round(rptlead.bm_03_pe/ rptlead.bm_03_pp * 100, v_num_round_digits)
                            WHEN bbmi.bb_measure_item_code = 'BM04' THEN round(rptlead.bm_04_pe/ rptlead.bm_04_pp * 100, v_num_round_digits)
                            WHEN bbmi.bb_measure_item_code = 'BM05' THEN round(rptlead.bm_05_pe/ rptlead.bm_05_pp * 100, v_num_round_digits)
                            WHEN bbmi.bb_measure_item_code = 'BM06' THEN round(rptlead.bm_06_pe/ rptlead.bm_06_pp * 100, v_num_round_digits)
                            WHEN bbmi.bb_measure_item_code = 'BM07' THEN round(rptlead.bm_07_pe/ rptlead.bm_07_pp * 100, v_num_round_digits)
                            WHEN bbmi.bb_measure_item_code = 'BM08' THEN round(rptlead.bm_08_pe/ rptlead.bm_08_pp * 100, v_num_round_digits)
                            WHEN bbmi.bb_measure_item_code = 'wtavg' THEN round(rptlead.total_pe/ rptlead.total_pp * 100, v_num_round_digits)
                            ELSE NULL END AS score
                    ,CASE WHEN bbmi.bb_measure_item_code = 'BM01' THEN rptlead.bm_01_color
                            WHEN bbmi.bb_measure_item_code = 'BM02' THEN rptlead.bm_02_color
                            WHEN bbmi.bb_measure_item_code = 'BM03' THEN rptlead.bm_03_color
                            WHEN bbmi.bb_measure_item_code = 'BM04' THEN rptlead.bm_04_color
                            WHEN bbmi.bb_measure_item_code = 'BM05' THEN rptlead.bm_05_color
                            WHEN bbmi.bb_measure_item_code = 'BM06' THEN rptlead.bm_06_color
                            WHEN bbmi.bb_measure_item_code = 'BM07' THEN rptlead.bm_07_color
                            WHEN bbmi.bb_measure_item_code = 'BM08' THEN rptlead.bm_08_color
                            WHEN bbmi.bb_measure_item_code = 'wtavg' THEN rptlead.wavg_color
                            ELSE NULL END AS color
                    ,'n'
                    ,1234
                    ,CURRENT_TIMESTAMP
            FROM    rpt_profile_leading_subject_stu AS rptlead
            CROSS JOIN  pm_bbcard_group AS bbg
                    ON      bbg.bb_group_code = 'lagLeadSubject'
            JOIN    tmp_bb_measure_subject_xref as xref
                    ON      rptlead.ayp_subject_id = xref.ayp_subject_id
            JOIN    pm_bbcard_measure AS bbm
                    ON      bbm.bb_group_id = xref.bb_group_id
                    AND     bbm.bb_measure_id = xref.bb_measure_id
            JOIN    pm_bbcard_measure_item AS bbmi
                    ON      bbmi.bb_group_id =  bbm.bb_group_id
                    AND     bbmi.bb_measure_id = bbm.bb_measure_id
                    AND     bbmi.bb_measure_item_code in ('wtavg','bm01','bm02','bm03','bm04','bm05','bm06','bm07','bm08')

            ) dt
    WHERE score is not null
    ;
                    

    ##############################################################
    # Insert lagging data
    ##############################################################

        INSERT INTO rpt_bbcard_detail_lag_lead_hst_subject
        (
        bb_group_id
        , bb_measure_id
        , bb_measure_item_id
        , student_id
        , school_year_id
        , score
        , score_color
        , score_type
        , last_user_id
        , create_timestamp
        ) 
    SELECT * FROM (
        # Lagging - Scale Score
        SELECT bbg.bb_group_id
                ,bbm.bb_measure_id
                ,mi.bb_measure_item_id
                ,asubst.student_id
                ,asubst.school_year_id
                ,asubst.ayp_score
                ,asubst.ayp_score_color
                ,asubst.score_type_code
                ,1234
                ,CURRENT_TIMESTAMP
        FROM c_ayp_subject_student AS asubst
        JOIN c_student_year AS sty
            ON   sty.student_id = asubst.student_id
            AND  sty.school_year_id = asubst.school_year_id
        JOIN c_grade_level AS gl
            ON gl.grade_level_id = sty.grade_level_id
        JOIN pm_bbcard_group AS bbg
            ON bbg.bb_group_code = 'lagLeadSubject'
        JOIN    tmp_bb_measure_subject_xref as xref
                ON      asubst.ayp_subject_id = xref.ayp_subject_id
        JOIN pm_bbcard_measure AS bbm
                ON      bbm.bb_group_id = xref.bb_group_id
                AND     bbm.bb_measure_id = xref.bb_measure_id
        JOIN pm_bbcard_measure_item AS mi
            ON   mi.bb_group_id = bbg.bb_group_id
            AND  mi.bb_measure_id = bbm.bb_measure_id
            AND  mi.bb_measure_item_code = 'scaleScore'
        WHERE   asubst.ayp_score IS NOT NULL
        AND     asubst.score_record_flag = 1
    
        UNION 
        # Lagging - Dev Score
        SELECT bbg2.bb_group_id
                ,bbm2.bb_measure_id
                ,mi2.bb_measure_item_id
                ,asubst2.student_id
                ,asubst2.school_year_id
                ,asubst2.alt_ayp_score
                ,asubst2.alt_ayp_score_color
                ,asubst2.score_type_code
                ,1234
                ,CURRENT_TIMESTAMP
        FROM c_ayp_subject_student AS asubst2
        JOIN c_student_year AS sty2
            ON   sty2.student_id = asubst2.student_id
            AND  sty2.school_year_id = asubst2.school_year_id
        JOIN c_grade_level AS gl2
            ON gl2.grade_level_id = sty2.grade_level_id
        JOIN pm_bbcard_group AS bbg2
            ON bbg2.bb_group_code = 'lagLeadSubject'
        JOIN    tmp_bb_measure_subject_xref as xref2
                ON      asubst2.ayp_subject_id = xref2.ayp_subject_id
        JOIN pm_bbcard_measure AS bbm2
                ON      bbm2.bb_group_id = xref2.bb_group_id
                AND     bbm2.bb_measure_id = xref2.bb_measure_id
        JOIN pm_bbcard_measure_item AS mi2
            ON   mi2.bb_group_id = bbg2.bb_group_id
            AND  mi2.bb_measure_id = bbm2.bb_measure_id
            AND  mi2.bb_measure_item_code = 'altScore'
        WHERE   asubst2.alt_ayp_score IS NOT NULL
        AND     asubst2.score_record_flag = 1
            ) dt
        ON DUPLICATE key UPDATE last_user_id = 1234;
        
END PROC;
//
