DROP PROCEDURE IF EXISTS etl_pm_state_test_scores_nwea//

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_state_test_scores_nwea()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 8352 $ $Date: 2010-03-26 13:55:02 -0400 (Fri, 26 Mar 2010) $'

/*
$Rev: 8352 $ 
$Author: mike.torian $ 
$Date: 2010-03-26 13:55:02 -0400 (Fri, 26 Mar 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_state_test_scores_nwea.sql $
$Id: etl_pm_state_test_scores_nwea.sql 8352 2010-03-26 17:55:02Z mike.torian $ 
 */

PROC: BEGIN 

    declare v_date_format_mask varchar(15) default '%m/%d/%Y';
    declare v_nweaDateFormatMask varchar(15);
    declare v_view_exists int(10);
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);
    
    set v_nweaDateFormatMask := pmi_f_get_etl_setting('nweaDateFormatMask');
    
    if v_nweaDateFormatMask is not null then
        set v_date_format_mask = v_nweaDateFormatMask;
    end if;
    
    SELECT  count(*) 
    INTO    v_view_exists
    FROM    information_schema.tables t
    WHERE   t.table_schema = @db_name_core
    AND     t.table_name = 'v_pmi_ods_nwea';

    IF v_view_exists > 0 THEN

        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_date_conversion`;

        CREATE TABLE `tmp_stu_admin` (
          `row_num` int(10) NOT NULL,
          `student_code` varchar(15) NOT NULL,
          `student_id` int(10) NOT NULL,
          UNIQUE KEY `uq_tmp_stu_admin` (`row_num`, `student_code`),
          KEY `ind_tmp_stu_admin_row_num` (`row_num`),
          KEY `ind_tmp_stu_admin_stu` (`student_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
           CREATE TABLE `tmp_date_conversion` (
          `test_start_date` varchar(10) NOT NULL,
          `test_date` datetime NOT NULL,
          UNIQUE KEY `uq_tmp_test_date` (`test_date`),
          KEY `uq_tmp_test_start_date` (`test_start_date`)
          ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;

        
        insert into tmp_date_conversion (
                test_start_date
                ,test_date
        )
        select  m.test_start_date
                ,str_to_date(m.test_start_date, v_date_format_mask)
                from v_pmi_ods_nwea m
                group by m.test_start_date
                ,str_to_date(m.test_start_date, v_date_format_mask)
        ;        

        insert  tmp_stu_admin (
            row_num
            ,student_code
            ,student_id
        )
        select  ods.row_num
                ,ods.student_id as student_code
                ,s.student_id
        from    v_pmi_ods_nwea as ods
        join    c_student as s
                on      s.student_code = ods.student_id
         where   ods.student_id is not null
        union all
        select  ods.row_num
                ,ods.student_id as student_code
                ,s.student_id
        from    v_pmi_ods_nwea as ods
        join    c_student as s
                on      s.student_state_code = ods.student_id
         where   ods.student_id is not null
        union all
        select  ods.row_num
                ,ods.student_id as student_code
                ,s.student_id
        from    v_pmi_ods_nwea as ods
        join    c_student as s
                on      s.fid_code = ods.student_id
         where   ods.student_id is not null
        on duplicate key update row_num = values(row_num);


        INSERT pm_state_test_scores (
            student_id
            ,test_id
            ,subject_id
            ,test_year
            ,test_month
            ,nwea_rit_score
            ,nwea_std_err
            ,nwea_percentile
            ,nwea_lexile
            ,nwea_lexile_min
            ,nwea_lexile_max
            ,nwea_adjective
            ,last_user_id
            ,create_timestamp)
        SELECT  dt.student_id
                ,dt.test_id
                ,dt.subject_id
                ,year(dt.test_start_date)
                ,month(dt.test_start_date)
                ,dt.nwea_rit_score
                ,dt.nwea_std_err
                ,dt.nwea_percentile
                ,dt.nwea_lexile
                ,dt.nwea_lexile_min
                ,dt.nwea_lexile_max
                ,dt.nwea_adjective 
                ,1234
                ,now()
        FROM (
            SELECT  s.student_id
                    ,ts.test_id
                    ,ts.subject_id
                    ,cdate.test_date AS test_start_date
                    ,CASE WHEN ts.sort_order = 0 THEN m.test_rit_score
                          WHEN ts.sort_order = 1 THEN m.goal_rit_score1
                          WHEN ts.sort_order = 2 THEN m.goal_rit_score2
                          WHEN ts.sort_order = 3 THEN m.goal_rit_score3
                          WHEN ts.sort_order = 4 THEN m.goal_rit_score4
                          WHEN ts.sort_order = 5 THEN m.goal_rit_score5
                          WHEN ts.sort_order = 6 THEN m.goal_rit_score6
                          WHEN ts.sort_order = 7 THEN m.goal_rit_score7
                        END AS nwea_rit_score
                    ,CASE WHEN ts.sort_order = 0 THEN m.test_std_err
                        END AS nwea_std_err
                    ,CASE WHEN ts.sort_order = 0 THEN m.test_percentile
                        END AS nwea_percentile
                    ,CASE WHEN ts.sort_order = 0 THEN m.lexile_score
                        END AS nwea_lexile
                    ,CASE WHEN ts.sort_order = 0 THEN m.lexile_min
                        END AS nwea_lexile_min
                    ,CASE WHEN ts.sort_order = 0 THEN m.lexile_max
                        END AS nwea_lexile_max
                    ,CASE WHEN ts.sort_order = 1 THEN m.goal_adjective1
                          WHEN ts.sort_order = 2 THEN m.goal_adjective2
                          WHEN ts.sort_order = 3 THEN m.goal_adjective3
                          WHEN ts.sort_order = 4 THEN m.goal_adjective4
                          WHEN ts.sort_order = 5 THEN m.goal_adjective5
                          WHEN ts.sort_order = 6 THEN m.goal_adjective6
                          WHEN ts.sort_order = 7 THEN m.goal_adjective7
                        END AS nwea_adjective
            FROM    v_pmi_ods_nwea as m
            JOIN    pm_state_test as t
                ON   t.moniker = m.test_name
            JOIN    pm_state_test_subject AS ts
                ON   ts.test_id = t.test_id
            JOIN    tmp_stu_admin as tmp
                ON  m.row_num = tmp.row_num
            JOIN    c_student as s
                 ON  s.student_id = tmp.student_id
            JOIN    tmp_date_conversion as cdate
                ON  m.test_start_date = cdate.test_start_date
            JOIN    c_school_year as sy
                 ON  cdate.test_date BETWEEN sy.begin_date AND sy.end_date
            WHERE m.lexile_max != 'BR'
                AND m.lexile_min != 'BR'
                AND m.lexile_score != 'BR') as dt
        WHERE dt.nwea_rit_score is not null
        ON DUPLICATE KEY UPDATE last_user_id = 1234
            ,nwea_rit_score  =  values(nwea_rit_score)
            ,nwea_std_err    =  values(nwea_std_err)
            ,nwea_percentile =  values(nwea_percentile)
            ,nwea_lexile     =  values(nwea_lexile)
            ,nwea_lexile_min =  values(nwea_lexile_min)
            ,nwea_lexile_max =  values(nwea_lexile_max)
            ,nwea_adjective  =  values(nwea_adjective) ;
   
        ##########################
        ## working table cleanup
        ##########################
   
        drop table if exists `tmp_stu_admin`;
        drop table if exists `tmp_date_conversion`;

        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_nwea', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;  
        
    end if;

END PROC;
//
