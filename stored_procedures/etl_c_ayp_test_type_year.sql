DROP PROCEDURE IF EXISTS etl_c_ayp_test_type_year //

CREATE definer=`dbadmin`@`localhost` procedure etl_c_ayp_test_type_year()
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
CONTAINS SQL
SQL SECURITY INVOKER

/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_c_ayp_test_type_year.sql $
$Id: etl_c_ayp_test_type_year.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

BEGIN

    INSERT INTO c_ayp_test_type_year (
        ayp_test_type_id
        , school_year_id
        , active_flag
        , last_user_id
        , create_timestamp
        ) 
    
    select  dt.ayp_test_type_id
        ,dt.school_year_id
        ,dt.active_flag
        ,1234
        ,now()
    
    from    (
                SELECT  tt.ayp_test_type_id
                    ,ss.school_year_id
                    ,case when ss.school_year_id between (sy.school_year_id - 4) and sy.school_year_id then 1 else 0 end as active_flag
                
                FROM    c_ayp_test_type AS tt
                JOIN    c_ayp_subject AS sub
                        ON      sub.ayp_test_type_id = tt.ayp_test_type_id
                JOIN    c_ayp_subject_student AS ss
                        ON      ss.ayp_subject_id = sub.ayp_subject_id
                join    c_school_year as sy
                        on      sy.active_flag = 1
                GROUP BY tt.ayp_test_type_id, ss.school_year_id
            ) as dt
    ON DUPLICATE KEY UPDATE last_user_id = 1234
        ,active_flag = dt.active_flag
    ;
    
END;
//
