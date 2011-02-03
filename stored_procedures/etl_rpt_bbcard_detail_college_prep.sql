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
    select  bbg.bb_group_id as bb_group_id
        ,bbm.bb_measure_id as bb_measure_id
        ,0
        ,dt.student_id
        ,dt.test_year
        ,dt.score
        ,'n'
        ,cl.moniker
        ,1234
        ,now()
    from    
        (
            select  ts.student_id
                ,tty.test_type_id
                ,tsub.subject_id
                ,case when tsub.subject_code like '%maxmvwsum' then concat(tty.test_type_code,'total')
                    else tsub.subject_code end as subject_code
                ,max(ts.test_year) as test_year
                ,max(ts.score) as score
            from    pm_natl_test_scores as ts
            join    pm_natl_test_subject as tsub
                    on      tsub.subject_id = ts.subject_id
                    and     tsub.test_type_id = ts.test_type_id
            join    pm_natl_test_type as tty
                    on      tty.test_type_id = ts.test_type_id
                    and     tty.test_type_code in ('sat','psat')
            group by ts.student_id
                ,ts.test_type_id
                ,ts.subject_id
        ) as dt
                    
    join    pm_natl_test_type as tty
            on      tty.test_type_id = dt.test_type_id
    join    pm_bbcard_group as bbg
            on      bbg.bb_group_code = tty.test_type_code
            and     bbg.bb_group_code in ('sat', 'psat')
    join    pm_bbcard_measure as bbm
            on      bbm.bb_measure_code = dt.subject_code
    left join   pm_color_natl_test_subject as clrts
            on      clrts.subject_id = dt.subject_id
            and     dt.test_type_id = clrts.test_type_id
            and     dt.test_year between clrts.begin_year and clrts.end_year
            and     dt.score between clrts.min_score and clrts.max_score
    left join   pmi_color as cl
            on  cl.color_id = clrts.color_id
    on duplicate key update last_user_id = 1234
        ,score = values(score)
        ,score_color = values(score_color)
    ;

end proc;
//
