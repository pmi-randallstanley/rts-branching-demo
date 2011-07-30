DROP PROCEDURE IF EXISTS etl_rpt_baseball_detail_ga_g8wa //

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_baseball_detail_ga_g8wa()
COMMENT '$Rev: 8352 $ $Date: 2010-03-26 13:55:02 -0400 (Fri, 26 Mar 2010) $'
CONTAINS SQL
SQL SECURITY INVOKER

/*
$Rev: 8352 $ 
$Author: mike.torian $ 
$Date: 2010-03-26 13:55:02 -0400 (Fri, 26 Mar 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_baseball_detail_ga_g8wa.sql $
$Id: etl_rpt_baseball_detail_ga_g8wa.sql 8352 2010-03-26 17:55:02Z mike.torian $ 
*/

BEGIN

    declare v_school_year_id int(11);
    declare v_view_exists int(10);

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    SELECT  count(*) 
    INTO    v_view_exists
    FROM    information_schema.tables t
    WHERE   t.table_schema = @db_name_core
    AND     t.table_name = 'v_pmi_ods_ga_g8wa';

    IF v_view_exists > 0 THEN

        SET v_school_year_id = (SELECT school_year_id FROM c_school_year sy WHERE sy.active_flag = 1);
  
        DELETE FROM rpt_baseball_detail_ga_writing  
        WHERE  school_year_id = v_school_year_id AND 
            bb_group_id IN (SELECT bb_group_id FROM pm_baseball_group WHERE bb_group_code = 'gag8wa');
       

####################################################################
# Insert GA Writing G8WA data into rpt tables for baseball report.
####################################################################

        drop table if exists `tmp_stu_admin`;

        CREATE TABLE `tmp_stu_admin` (
          `row_num` int(10) NOT NULL,
          `student_code` varchar(15) NOT NULL,
          `student_id` int(10) NOT NULL,
          UNIQUE KEY `uq_tmp_stu_admin` (`student_code`, `row_num`),
          KEY `ind_tmp_stu_admin_row_num` (`row_num`),
          KEY `ind_tmp_stu_admin_stu` (`student_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1
        ;
        
        
        insert  tmp_stu_admin (
                row_num
                ,student_code
                ,student_id
        )
        select  ods.row_num
                ,ods.stuid as student_code
                ,s.student_id
        from    v_pmi_ods_ga_g8wa as ods
        join    c_student as s
                on      s.student_code = ods.stuid
         where   ods.stuid is not null
        union all
        select  ods.row_num
                ,ods.stuid as student_code
                ,s.student_id
        from    v_pmi_ods_ga_g8wa as ods
        join    c_student as s
                on      s.student_state_code = ods.stuid
         where   ods.stuid is not null
        union all
        select  ods.row_num
                ,ods.stuid as student_code
                ,s.student_id
        from    v_pmi_ods_ga_g8wa as ods
        join    c_student as s
                on      s.fid_code = ods.stuid
         where   ods.stuid is not null
        on duplicate key update row_num = values(row_num);

        INSERT INTO rpt_baseball_detail_ga_writing
            (    bb_group_id
                , bb_measure_id
                , bb_measure_item_id
                , student_id
                , school_year_id
                , score
                , score_color
                , last_user_id
                , create_timestamp
            ) 
        SELECT bg.bb_group_id
               ,bm.bb_measure_id
               ,0 as bb_meaure_item_id
               ,s.student_id
               ,sy.school_year_id
               ,MAX(CASE 
                        WHEN bm.bb_measure_code = 'gag8waScaleScore'   THEN wrtss
                        WHEN bm.bb_measure_code = 'gag8waIdeas'        THEN dom1
                        WHEN bm.bb_measure_code = 'gag8waOrganization' THEN dom2
                        WHEN bm.bb_measure_code = 'gag8waStyle'        THEN dom3
                        WHEN bm.bb_measure_code = 'gag8waConventions'  THEN dom4
                   END) AS score
               ,MAX(CASE 
                        WHEN bm.bb_measure_code = 'gag8waScaleScore'   THEN clr.moniker 
                        ELSE NULL
                   END) AS score_color
               ,1234
               ,now()
        FROM  v_pmi_ods_ga_g8wa AS ts
        JOIN  tmp_stu_admin as tmp
                ON ts.stuid = tmp.student_code
        JOIN  c_student AS s
                ON s.student_id = tmp.student_id
        JOIN  pm_baseball_group AS bg 
                ON bg.bb_group_code = 'gag8wa'
        JOIN  pm_baseball_measure AS bm
                ON bm.bb_group_id = bg.bb_group_id 
        JOIN c_school_year AS sy
                ON sy.active_flag = 1  
        left JOIN  pm_color_ga_writing AS ga
                ON ts.wrtss BETWEEN ga.min_score  AND ga.max_score 
                AND sy.school_year_id BETWEEN ga.begin_year AND ga.end_year 
        left JOIN  pmi_color AS clr
                ON clr.color_id = ga.color_id               
        GROUP BY bg.bb_group_id
                 ,bm.bb_measure_id
                 ,s.student_id
                 ,sy.school_year_id
        ;
    
        ##############################
        ## clean up working tables
        ##############################
    
        drop table if exists `tmp_stu_admin`;

        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_ga_g8wa', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;  
        
    END IF;    

END;
//
