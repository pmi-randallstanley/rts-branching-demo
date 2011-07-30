/*
$Rev: 7380 $
$Author: randall.stanley $
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_discipline.sql $
$Id: etl_rpt_discipline.sql 7380 2009-07-16 14:23:58Z randall.stanley $
 */


DROP PROCEDURE IF EXISTS etl_rpt_discipline//

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_discipline()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7380 $ $Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $'

PROC: BEGIN 

    declare v_date_format_mask varchar(15) default '%m%d%Y';
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_discipline';

    if @view_exists > 0 then

        set @discDateFormatMask := pmi_f_get_etl_setting('discDateFormatMask');
    
         if @discDateFormatMask is not null then
            set v_date_format_mask = @discDateFormatMask;
         end if;
    
        ##############################################################
        # Insert Discipline Records  
        ##############################################################
        INSERT INTO rpt_discipline (
            student_id
            ,school_year_id
            ,disc_date
            ,`code`
            ,title
            ,abbrev
            ,`type`
            ,last_user_id
            ,create_timestamp
        )
        SELECT 
            st.student_id
            ,sty.school_year_id
            ,STR_TO_DATE(odsimp.discipline_date, v_date_format_mask) AS date_str
            ,odsimp.discipline_code
            ,odsimp.discipline_title
            ,odsimp.discipline_abbrev
            ,odsimp.discipline_type
            ,1234
            ,now()
        FROM    v_pmi_ods_discipline AS odsimp
        JOIN    c_student AS st
                ON      st.student_code = odsimp.student_id
        JOIN    c_school_year AS sy
                ON      STR_TO_DATE(odsimp.discipline_date, v_date_format_mask) BETWEEN sy.begin_date AND sy.end_date 
        JOIN    c_student_year AS sty
                ON      sty.student_id = st.student_id
                AND     sty.school_year_id = sy.school_year_id
        WHERE   odsimp.discipline_title IS NOT NULL
        ON DUPLICATE KEY UPDATE
            title   = odsimp.discipline_title
            ,abbrev = odsimp.discipline_abbrev
            ,`type`   = odsimp.discipline_type
            ,last_user_id = 1234
        ;

        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_discipline', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;  

    end if;

END PROC;
//
