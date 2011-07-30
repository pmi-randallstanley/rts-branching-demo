/*
$Rev: 7701 $ 
$Author: randall.stanley $ 
$Date: 2009-09-28 13:05:32 -0400 (Mon, 28 Sep 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_lexile_scores.sql $
$Id: etl_pm_lexile_scores.sql 7701 2009-09-28 17:05:32Z randall.stanley $ 
 */

####################################################################
# Insert scores into pm_lexile_scores.
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_pm_lexile_scores//

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_lexile_scores()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7701 $'

PROC: BEGIN 

    declare v_date_format_mask varchar(15) default '%Y-%m-%d';
    declare v_use_stu_fid_code char(1) default 'n';    
    
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    set @use_stu_fid_code := pmi_f_get_etl_setting('lexileUseFIDCode');
    if @use_stu_fid_code is not null then
        set v_use_stu_fid_code = @use_stu_fid_code;
    end if;

    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_lexile';

    if @view_exists > 0 then

        set @lexileDateFormatMask := pmi_f_get_etl_setting('lexileDateFormatMask');
    
         if @lexileDateFormatMask is not null then
            set v_date_format_mask = @lexileDateFormatMask;
         end if;

        # load using c_student.student_code as default method
        # use of fid_code is the exception
        if v_use_stu_fid_code = 'y' then
            ##############################
            # Using student fid_code...  #
            ##############################
            INSERT INTO pm_lexile_scores (
                student_id
                ,test_date
                ,test_moniker
                ,school_year_id
                ,lexile_score
                ,last_user_id
                ,create_timestamp
            )
            SELECT 
                st.student_id
                ,COALESCE(STR_TO_DATE(vods.test_date, v_date_format_mask), '2000-01-01')
                ,COALESCE(vods.moniker, CONCAT(vods.test_mm, '_', vods.test_year), DATE_FORMAT(STR_TO_DATE(vods.test_date, v_date_format_mask), '%m_%Y'))
                ,sty.school_year_id
                ,vods.lexile_score
                ,1234
                ,now()
            FROM   v_pmi_ods_lexile AS vods
            JOIN   c_student AS st
                ON   st.fid_code = vods.student_id
            JOIN   c_student_year AS sty
                ON   sty.student_id = st.student_id
                AND  sty.school_year_id = vods.school_year
            WHERE    coalesce(vods.test_date, vods.moniker) IS NOT NULL
            AND     vods.lexile_score is not null
            ON DUPLICATE KEY UPDATE
                lexile_score = vods.lexile_score,
                last_user_id = 1234;
        else
            ##############################
            # Using student_code...      #
            ##############################
            INSERT INTO pm_lexile_scores (
                student_id
                ,test_date
                ,test_moniker
                ,school_year_id
                ,lexile_score
                ,last_user_id
                ,create_timestamp
            )
            SELECT 
                st.student_id
                ,COALESCE(STR_TO_DATE(vods.test_date, v_date_format_mask), '2000-01-01')
                ,COALESCE(vods.moniker, CONCAT(vods.test_mm, '_', vods.test_year), DATE_FORMAT(STR_TO_DATE(vods.test_date, v_date_format_mask), '%m_%Y'))
                ,sty.school_year_id
                ,vods.lexile_score
                ,1234
                ,now()
            FROM   v_pmi_ods_lexile AS vods
            JOIN   c_student AS st
                ON   st.student_code = vods.student_id
            JOIN   c_student_year AS sty
                ON   sty.student_id = st.student_id
                AND  sty.school_year_id = vods.school_year
            WHERE    coalesce(vods.test_date, vods.moniker) IS NOT NULL
            AND     vods.lexile_score is not null
            ON DUPLICATE KEY UPDATE
                lexile_score = values(lexile_score),
                last_user_id = values(last_user_id);

        end if;

        #################
        ## Update Log
        #################
            
        SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_lexile', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');
    
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;  

    end if;

END PROC;
//
