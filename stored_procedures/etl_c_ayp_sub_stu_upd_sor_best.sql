/*
$Rev: 8019 $ 
$Author: randall.stanley $ 
$Date: 2009-12-14 15:18:55 -0500 (Mon, 14 Dec 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_c_ayp_sub_stu_upd_sor_best.sql $
$Id: etl_c_ayp_sub_stu_upd_sor_best.sql 8019 2009-12-14 20:18:55Z randall.stanley $ 
 */

drop procedure if exists etl_c_ayp_sub_stu_upd_sor_best//

create definer=`dbadmin`@`localhost` procedure etl_c_ayp_sub_stu_upd_sor_best(p_ayp_subject_id int(11))
contains sql
sql security invoker
comment '$Rev: 8019 $ $Date: 2009-12-14 15:18:55 -0500 (Mon, 14 Dec 2009) $'

proc: begin 

    drop table if exists `tmp_best_score_dups`;
    CREATE TABLE `tmp_best_score_dups` (
      `student_id` int(10) NOT NULL,
      `ayp_subject_id` int(10) NOT NULL,
      `school_year_id` smallint(4) NOT NULL,
      `ayp_score` decimal(9,3) NOT NULL,
      `test_month_id` tinyint(2) NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1
    ;

    # reset score_record_flag for this subject
    update  c_ayp_subject_student
    set     score_record_flag = 0
    where   ayp_subject_id = p_ayp_subject_id
    ;
    
    # get score of record candidate list for student/subject/year 
    # with multiple administrations
    insert tmp_best_score_dups (
        student_id
        ,ayp_subject_id 
        ,school_year_id
        ,ayp_score
    )
    
    select  student_id
        ,ayp_subject_id
        ,school_year_id
        ,max(coalesce(alt_ayp_score,ayp_score))  ### Needed to fix so it looks at alt score if that is there
    
    from    c_ayp_subject_student
    where   ayp_subject_id = p_ayp_subject_id
    group by student_id, ayp_subject_id, school_year_id
    having count(*) > 1
    ;
    
    # determine the month of the best score; in case of tie
    # select most recent month
    update  tmp_best_score_dups as upd
    join    (
                select  ss.student_id
                    ,ss.ayp_subject_id
                    ,ss.school_year_id
                    ,max(sym.academic_year_order) as max_academic_year_order
                from    tmp_best_score_dups as dups1
                join    c_ayp_subject_student as ss
                        on      dups1.student_id = ss.student_id
                        and     dups1.ayp_subject_id = ss.ayp_subject_id
                        and     dups1.school_year_id = ss.school_year_id
                        and     dups1.ayp_score = ss.ayp_score
                join    c_school_year_month  as sym
                        on      ss.month_id = sym.month_id
                group by ss.student_id, ss.ayp_subject_id, ss.school_year_id
            ) as dt
            on      upd.student_id = dt.student_id
            and     upd.ayp_subject_id = dt.ayp_subject_id
            and     upd.school_year_id = dt.school_year_id
    join    c_school_year_month as sym2
            on      sym2.academic_year_order = dt.max_academic_year_order
    set     upd.test_month_id = sym2.month_id
    ;
    
    # set score_record_flag where multiple admins exist
    update  c_ayp_subject_student as upd
    join    tmp_best_score_dups as dups
            on      upd.student_id = dups.student_id
            and     upd.ayp_subject_id = dups.ayp_subject_id    
            and     upd.school_year_id = dups.school_year_id
            and     upd.month_id = dups.test_month_id
    set     upd.score_record_flag = 1
    ;
    
    # set score_record_flag where multiple admins do not exist
    update  c_ayp_subject_student as upd
    left join    tmp_best_score_dups as dups
            on      upd.student_id = dups.student_id
            and     upd.ayp_subject_id = dups.ayp_subject_id
            and     upd.school_year_id = dups.school_year_id
    set     upd.score_record_flag = 1
    where   upd.ayp_subject_id = p_ayp_subject_id
    and     dups.student_id is null
    ;

    drop table if exists `tmp_best_score_dups`;

end proc;
//
