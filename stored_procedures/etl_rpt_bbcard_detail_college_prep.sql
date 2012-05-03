/*
$Rev: 9643 $ 
$Author: randall.stanley $ 
$Date: 2010-11-05 22:55:00 -0400 (Fri, 05 Nov 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bbcard_detail_college_prep.sql $
$Id: etl_rpt_bbcard_detail_college_prep.sql 9643 2010-11-06 02:55:00Z randall.stanley $ 
*/

drop procedure if exists etl_rpt_bbcard_detail_college_prep//

create definer=`dbadmin`@`localhost` procedure etl_rpt_bbcard_detail_college_prep()
contains sql
sql security invoker
comment '$Rev: 9643 $ $Date: 2010-11-05 22:55:00 -0400 (Fri, 05 Nov 2010) $'


proc: begin 

    declare     v_bbcv2_use_month_season varchar(15) default 'n';
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    set @bbcv2_use_month_season := pmi_f_get_etl_setting('bbcv2UseMonthSeason');

    if @bbcv2_use_month_season is not null then
        set v_bbcv2_use_month_season = @bbcv2_use_month_season;
    end if;

    drop table if exists `tmp_natl_test_scores_bbc`;

    create table `tmp_natl_test_scores_bbc` (
      `student_id` int(10) not null,
      `test_type_id` int(10) not null,
      `subject_id` int(10) NOT NULL,
      `subject_code` varchar(50) NOT NULL,
      `test_month` tinyint(4) NOT NULL,
      `month_season` varchar(15) NOT NULL,
      `test_year` int(11) NOT NULL,
      `school_year_id` int(11) NOT NULL,
      `score` decimal(6,1) default NULL,
      `color_moniker` varchar(50) default NULL ,
      `best_overall_score_flag` tinyint(4) NOT NULL,
      `best_year_score_flag` tinyint(4) NOT NULL,
      `best_season_score_flag` tinyint(4) NOT NULL,
      primary key  (`student_id`,`test_type_id`,`subject_id`,`school_year_id`,`test_month`)
    ) engine=innodb default charset=latin1
    ;
    
    
    ### Insert Initial Data
    insert into tmp_natl_test_scores_bbc (
            student_id
            ,test_type_id
            ,subject_id
            ,subject_code
            ,test_month
            ,month_season
            ,test_year
            ,school_year_id
            ,score
            ,color_moniker
            ,best_overall_score_flag
            ,best_year_score_flag
            ,best_season_score_flag )
    select  ts.student_id
        ,tty.test_type_id
        ,tsub.subject_id
        ,case when tsub.subject_code like '%satmvwsum' then concat(tty.test_type_code,'total')
            else tsub.subject_code end as subject_code
        ,ts.test_month
        ,coalesce(sym.month_season,'NA') as month_season
        ,ts.test_year
        ,sy.school_year_id
        ,max(ts.score) as score
        ,null as color_desc
        ,0 as best_overall_score
        ,0 as best_year_score
        ,0 as best_season_score
    from    pm_natl_test_scores as ts
    join    pm_natl_test_subject as tsub
            on      tsub.subject_id = ts.subject_id
            and     tsub.test_type_id = ts.test_type_id
    join    pm_natl_test_type as tty
            on      tty.test_type_id = ts.test_type_id
            and     tty.test_type_code in ('sat','psat','act','explore','plan')
    join    c_school_year sy ### Data is stored by test year, Need to convert to school year for BBC report
            on      str_to_date(concat('15/', ts.test_month,'/',ts.test_year), '%d/%m/%Y') between sy.begin_date and sy.end_date
    left join   c_school_year_month sym
                on  ts.test_month = sym.month_id
    group by ts.student_id
        ,ts.test_type_id
        ,ts.subject_id
        ,ts.test_month
        ,coalesce(sym.month_season,'NA')
        ,ts.test_year
        ,sy.school_year_id
    ;
    
    ## Update Color
    update  tmp_natl_test_scores_bbc tmp
    join    pm_color_natl_test_subject as clrts
            on      clrts.subject_id = tmp.subject_id
            and     tmp.test_type_id = clrts.test_type_id
            and     tmp.test_year between clrts.begin_year and clrts.end_year  ## Using test year b/c that is how base data is colored.
            and     tmp.score between clrts.min_score and clrts.max_score
    join    pmi_color as cl
                on  cl.color_id = clrts.color_id
    set     tmp.color_moniker = cl.moniker
    ;
    
    ## Update Best Overall Score Flag
    update  tmp_natl_test_scores_bbc tmp
    join    (select student_id
                    , test_type_id
                    , subject_id
                    , max(score) as best_score
            from tmp_natl_test_scores_bbc
            group by student_id, test_type_id, subject_id
            ) as dt
            on  tmp.student_id = dt.student_id
            and tmp.test_type_id = dt.test_type_id
            and tmp.subject_id = dt.subject_id
            and tmp.score = dt.best_score
    set     tmp.best_overall_score_flag = 1
    ;
    
    
    ## Update Best Year Score Flag
    update  tmp_natl_test_scores_bbc tmp
    join    (select student_id
                    , test_type_id
                    , subject_id
                    , school_year_id
                    , max(score) as best_score
            from tmp_natl_test_scores_bbc
            group by student_id, test_type_id, subject_id, school_year_id
            ) as dt
            on  tmp.student_id = dt.student_id
            and tmp.test_type_id = dt.test_type_id
            and tmp.subject_id = dt.subject_id
            and tmp.school_year_id = dt.school_year_id
            and tmp.score = dt.best_score
    set     tmp.best_year_score_flag = 1
    ;
    
    
    
    ## Update Best Season Score Flag
    update  tmp_natl_test_scores_bbc tmp
    join    (select student_id
                    , test_type_id
                    , subject_id
                    , school_year_id
                    , month_season
                    , max(score) as best_score
            from tmp_natl_test_scores_bbc
            group by student_id, test_type_id, subject_id, school_year_id, month_season
            ) as dt
            on  tmp.student_id = dt.student_id
            and tmp.test_type_id = dt.test_type_id
            and tmp.subject_id = dt.subject_id
            and tmp.school_year_id = dt.school_year_id
            and tmp.month_season = dt.month_season
            and tmp.score = dt.best_score
    set     tmp.best_season_score_flag = 1
    ;
    
    
    IF v_bbcv2_use_month_season = 'n' THEN
        ## Insert Best Year Score

        insert into rpt_bbcard_detail_college_prep (
            bb_group_id
            , bb_measure_id
            , bb_measure_item_id
            , student_id
            , school_year_id
            , score
            , score_type
            , score_color
            , last_user_id
            , create_timestamp
        )
        select  bbm.bb_group_id as bb_group_id
            ,bbm.bb_measure_id as bb_measure_id
            ,bbmi.bb_measure_item_id as bb_measure_item_id
            ,tmp.student_id
            ,tmp.school_year_id
            ,tmp.score
            ,'n'
            ,tmp.color_moniker
            ,1234
            ,now()
        from    tmp_natl_test_scores_bbc tmp  
        join    pm_bbcard_measure as bbm
                on      bbm.bb_measure_code = tmp.subject_code
        join    pm_bbcard_measure_item as bbmi
            on      bbm.bb_group_id = bbmi.bb_group_id 
            and     bbm.bb_measure_id = bbmi.bb_measure_id
            and     bbmi.bb_measure_item_code = 'bestscore'
        where   tmp.best_year_score_flag = 1
        on duplicate key update last_user_id = 1234
            ,score = values(score)
            ,score_color = values(score_color)
        ;
    
    ELSE

        ## Insert Best Month Season Score
        insert into rpt_bbcard_detail_college_prep (
            bb_group_id
            , bb_measure_id
            , bb_measure_item_id
            , student_id
            , school_year_id
            , score
            , score_type
            , score_color
            , last_user_id
            , create_timestamp
        )
        select  bbm.bb_group_id as bb_group_id
            ,bbm.bb_measure_id as bb_measure_id
            ,bbmi.bb_measure_item_id as bb_measure_item_id
            ,tmp.student_id
            ,tmp.school_year_id
            ,tmp.score
            ,'n'
            ,tmp.color_moniker
            ,1234
            ,now()
        from    tmp_natl_test_scores_bbc tmp  
        join    pm_bbcard_measure as bbm
                on      bbm.bb_measure_code = tmp.subject_code
        join    pm_bbcard_measure_item as bbmi
                on      bbm.bb_group_id = bbmi.bb_group_id 
                and     bbm.bb_measure_id = bbmi.bb_measure_id
                and     bbmi.bb_measure_item_code = tmp.month_season
        where   tmp.best_season_score_flag = 1
        on duplicate key update last_user_id = 1234
            ,score = values(score)
            ,score_color = values(score_color)
        ;
    END IF;
    
    ## Insert Best Overall Season Score
    insert into rpt_bbcard_detail_college_prep (
        bb_group_id
        , bb_measure_id
        , bb_measure_item_id
        , student_id
        , school_year_id
        , score
        , score_type
        , score_color
        , last_user_id
        , create_timestamp
    )
    select  bbm.bb_group_id as bb_group_id
        ,bbm.bb_measure_id as bb_measure_id
        ,bbmi.bb_measure_item_id as bb_measure_item_id
        ,tmp.student_id
        ,0
        ,tmp.score
        ,'n'
        ,tmp.color_moniker
        ,1234
        ,now()
    from    tmp_natl_test_scores_bbc tmp  
    join    pm_bbcard_measure as bbm
            on      bbm.bb_measure_code = tmp.subject_code
    join    pm_bbcard_measure_item as bbmi
            on      bbm.bb_group_id = bbmi.bb_group_id 
            and     bbm.bb_measure_id = bbmi.bb_measure_id
            and     bbmi.bb_measure_item_code = 'bestscore'
    where   tmp.best_overall_score_flag = 1
    on duplicate key update last_user_id = 1234
        ,score = values(score)
        ,score_color = values(score_color)
    ;
    
    drop table if exists `tmp_natl_test_scores_bbc`;


end proc;
//
