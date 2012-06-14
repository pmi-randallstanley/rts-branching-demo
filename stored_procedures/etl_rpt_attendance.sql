/*
$Rev: 7380 $
$Author: randall.stanley $
$Date: 2009-07-16 10:23:58 -0400 (Thu, 16 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_attendance.sql $
$Id: etl_rpt_attendance.sql 7380 2009-07-16 14:23:58Z randall.stanley $
 */


DROP PROCEDURE IF EXISTS etl_rpt_attendance//

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_attendance()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7380 $'

PROC: BEGIN 

    declare v_date_format_mask varchar(15) default '%m%d%Y';
    declare v_attendTruncateBeforeLoading varchar(15) default 'n';
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_attendance';

    if @view_exists > 0 then

        set @attendDateFormatMask := pmi_f_get_etl_setting('attendDateFormatMask');
    
         if @attendDateFormatMask is not null then
            set v_date_format_mask = @attendDateFormatMask;
         end if;
         
        set @attendTruncateBeforeLoading := pmi_f_get_etl_setting('attendTruncateBeforeLoading');
    
         if @attendTruncateBeforeLoading is not null then
            set v_attendTruncateBeforeLoading = @attendTruncateBeforeLoading;
         end if;
         
        if v_attendTruncateBeforeLoading = 'y' then
            truncate table rpt_attendance;
        end if;
        
        ##############################################################
        # Insert Attendance Records  
        ##############################################################
        INSERT INTO rpt_attendance (
            student_id
            ,school_year_id
            ,att_date
            ,code
            ,title
            ,abbrev
            ,type
            ,last_user_id
            ,create_timestamp
        )
        SELECT 
            st.student_id
            ,sty.school_year_id
            ,str_to_date(odsimp.att_date, v_date_format_mask) AS date_str
            ,odsimp.att_code
            ,odsimp.att_title
            ,odsimp.att_abbrev
            ,odsimp.att_type
            ,1234
            ,now()
        FROM    v_pmi_ods_attendance AS odsimp
        JOIN    c_student AS st
                ON      st.student_code = odsimp.student_id
        JOIN    c_school_year AS sy
                ON      str_to_date(odsimp.att_date, v_date_format_mask)  BETWEEN sy.begin_date AND sy.end_date
        JOIN    c_student_year AS sty
                ON      sty.student_id = st.student_id
                AND     sty.school_year_id = sy.school_year_id
        WHERE    odsimp.att_title IS NOT NULL
        ON DUPLICATE KEY UPDATE
            title   = odsimp.att_title
            ,abbrev = odsimp.att_abbrev
            ,type   = odsimp.att_type
            ,last_user_id = 1234
        ;
        
        ### Optimize the rpt_attendance table
        optimize table rpt_attendance;

        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := '';
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_attendance', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;  

    end if;

END PROC;
//
