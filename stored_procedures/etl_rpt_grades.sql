/*
$Rev: 7380 $
$Author: randall.stanley $
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_grades.sql $
$Id: etl_rpt_grades.sql 7380 2009-07-16 14:23:58Z randall.stanley $
 */


DROP PROCEDURE IF EXISTS etl_rpt_grades//

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_grades()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7380 $ $Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $'

PROC: BEGIN 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_grades';

    if @view_exists > 0 then

        select  school_year_id
        into    @curr_sy_id
        from    c_school_year
        where   active_flag = 1
        ;

        delete from rpt_grades
        where   school_year_id < @curr_sy_id
        ;
        
        ##############################################################
        # Insert Grade Records  
        ##############################################################
        INSERT INTO rpt_grades (
            student_id
            ,school_year_id
            ,course_code
            ,course_title
            ,mp_code
            ,mp_order
            ,mp_title
            ,mark
            ,last_user_id
            ,create_timestamp
            )
            SELECT 
                st.student_id
                ,sty.school_year_id
                ,odsimp.course_id
                ,replace(odsimp.course_title, '&', ' ')
                ,COALESCE(
                    CASE WHEN odsimp.marking_period_code rlike '[[:digit:]]' THEN CONCAT('x_', odsimp.marking_period_code) ELSE odsimp.marking_period_code end,
                    CASE WHEN odsimp.marking_period_title rlike '[[:digit:]]' THEN CONCAT('x_', odsimp.marking_period_title) ELSE odsimp.marking_period_title end) AS mp_code
                ,odsimp.marking_period_order
                ,COALESCE(odsimp.marking_period_title, odsimp.marking_period_code) AS mp_title
                ,odsimp.mark
                ,1234
                ,now()
            FROM   v_pmi_ods_grades AS odsimp
            JOIN   c_student AS st
                ON   st.student_code = odsimp.student_id
          JOIN   c_student_year AS sty
                 ON   sty.student_id = st.student_id
                 AND  sty.school_year_id = odsimp.school_year
          WHERE COALESCE(odsimp.marking_period_title, odsimp.marking_period_code) IS NOT NULL
        ON DUPLICATE KEY UPDATE
            course_title = values(course_title)
            ,mp_order = values(mp_order)
            ,mp_code = values(mp_code)
            ,mp_title = values(mp_title)
            ,mark     = values(mark)
            ,last_user_id = 1234
        ;

        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_grades', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;  

    end if;

END PROC;
//
