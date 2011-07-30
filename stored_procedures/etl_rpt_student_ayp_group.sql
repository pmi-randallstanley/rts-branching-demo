DROP PROCEDURE IF EXISTS etl_rpt_student_ayp_group //

CREATE definer=`dbadmin`@`localhost` procedure etl_rpt_student_ayp_group()
COMMENT '$Rev: 9375 $ $Date: 2010-10-07 08:57:01 -0400 (Thu, 07 Oct 2010) $'
CONTAINS SQL
SQL SECURITY INVOKER

/*
$Rev: 9375 $ 
$Author: randall.stanley $ 
$Date: 2010-10-07 08:57:01 -0400 (Thu, 07 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_student_ayp_group.sql $
$Id: etl_rpt_student_ayp_group.sql 9375 2010-10-07 12:57:01Z randall.stanley $ 
*/

BEGIN

    DECLARE  v_ayp_group_code, v_join_text varchar(200);
    DECLARE  v_not_found BOOLEAN default 0;
    
    DECLARE cur_ayp_group_inserts CURSOR FOR
        SELECT ayp_group_code
            , CASE WHEN ayp_group_code like 'ethn%' AND ayp_group_code <> 'all'
                    THEN concat('AND e.ethnicity_code = ', (CASE 
                                            WHEN ayp_group_code like '%Asian'    THEN  '''a'''
                                            WHEN ayp_group_code like '%Black'    THEN  '''b'''
                                            WHEN ayp_group_code like '%Hispanic' THEN  '''h'''
                                            WHEN ayp_group_code like '%Indian'   THEN  '''i'''
                                            WHEN ayp_group_code like '%Multi'    THEN  '''m'''
                                            WHEN ayp_group_code like '%Hawaiian' THEN  '''p'''
                                            WHEN ayp_group_code like '%White'    THEN  '''w'''
                                            ELSE  '' END))
                    WHEN ayp_group_code not like 'ethn%' AND ayp_group_code <> 'all'
                    THEN concat('AND sty.', (CASE 
                                            WHEN ayp_group_code like 'swd%' THEN  'ayp_swd_flag'
                                            WHEN ayp_group_code like 'lep%' THEN  'ayp_lep_flag'
                                            WHEN ayp_group_code like 'ed%'  THEN  'ayp_ed_flag'
                                            WHEN ayp_group_code like 'econDisadv'  THEN  'ayp_ed_flag'
                                            ELSE  NULL END), ' = ', COALESCE(matrix_use_value, 1)) 
                    ELSE '' END as join_text
        FROM c_ayp_group
        where   ayp_group_code in ('all','ethnAsian','ethnBlack','ethnHispanic','ethnIndian','ethnMulti','ethnWhite','ethnHawaiian'
                                    ,'swd','lep','econDisadv','swdNo','swdYes','lepNo','lepYes','edNo','edYes')
        ;

    DECLARE CONTINUE HANDLER FOR NOT FOUND 
        SET v_not_found = TRUE;


    TRUNCATE TABLE rpt_student_ayp_group;
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    OPEN cur_ayp_group_inserts;
    
    loop_ayp_group_inserts: LOOP
        FETCH cur_ayp_group_inserts 
        INTO  v_ayp_group_code,
              v_join_text;
           
        IF v_not_found THEN
            CLOSE cur_ayp_group_inserts;
            LEAVE loop_ayp_group_inserts;
        END IF;
        
        SET @sql_text := concat('
            INSERT INTO rpt_student_ayp_group (
               accessor_id, 
               student_id,
               ayp_group_id, 
               grade_level_id,
               school_year_id, 
               last_user_id,
               last_edit_timestamp
            ) 
            
            SELECT DISTINCT   
            coalesce(sty.ayp_school_id,', @client_id, ') as accessor_id,  
                sty.student_id,
                ag.ayp_group_id,
                gl.grade_level_id,
                sty.school_year_id,
                1234,
                now()
            
            FROM   c_student st
            JOIN   c_student_year sty
                ON   sty.student_id = st.student_id
                AND  sty.active_flag = 1
            JOIN   c_school_year as sy
                ON   sty.school_year_id = sy.school_year_id 
                AND sy.active_flag = 1
            JOIN   c_ethnicity AS e
                ON   e.ethnicity_id = coalesce(sty.ayp_ethnicity_id, st.ethnicity_id)
            JOIN   c_grade_level gl
                ON   gl.grade_level_id = sty.grade_level_id
            JOIN   c_ayp_group ag
                ON   ag.ayp_group_code = ''', v_ayp_group_code,'''
            WHERE    st.active_flag = 1 and sty.ayp_enrollment_flag = 1
            ',  v_join_text);
        
        
        prepare stmt from @sql_text;
        execute stmt;
        deallocate prepare stmt;
        
    END LOOP loop_ayp_group_inserts;

     
END;
//
