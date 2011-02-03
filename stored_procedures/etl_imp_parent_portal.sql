/*
$Rev: 8472 $ 
$Author: randall.stanley $ 
$Date: 2010-04-29 16:01:54 -0400 (Thu, 29 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_parent_portal.sql $
$Id: etl_imp_parent_portal.sql 8472 2010-04-29 20:01:54Z randall.stanley $ 
 */


DROP PROCEDURE IF EXISTS etl_imp_parent_portal//

CREATE definer=`dbadmin`@`localhost` procedure etl_imp_parent_portal()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 8472 $ $Date: 2010-04-29 16:01:54 -0400 (Thu, 29 Apr 2010) $'

PROC: BEGIN 
   SELECT COUNT(*)
        ,SUM(CASE WHEN v.TABLE_NAME = 'v_pmi_ods_parent_portal_users'    THEN 1 ELSE 0 END)
    INTO
        @tot_count
        ,@v_pmi_ods_parent_portal_users
    FROM information_schema.VIEWS v
    WHERE v.TABLE_SCHEMA = database();

        IF @v_pmi_ods_parent_portal_users = 1          
            THEN    
            
                UPDATE pp_user 
                SET active_flag = 0;
            
                SELECT  c.client_id
                INTO    @client_id
                FROM    pmi_admin.pmi_dsn AS db
                JOIN    pmi_admin.pmi_client AS c
                        ON      c.dsn_core_id = db.dsn_id
                        AND NOT EXISTS  (   SELECT  *
                                            FROM    pmi_admin.pmi_client AS c2
                                            WHERE   c2.dsn_core_id = db.dsn_id
                                            AND     c2.client_id > c.client_id
                                        )
                WHERE   db.db_name = database();
                
                INSERT pp_user (pp_user_id, login, password, active_flag, force_reset_pwd_flag, client_id, last_user_id)
                SELECT DISTINCT pmi_f_get_next_sequence_app_db('pp_user', 1),
                        dt.pp_user_id, 
                        dt.pp_pwd,
                        1,
                        0,
                        @client_id,
                        1234
                FROM (SELECT DISTINCT ou.pp_user_id, ou.pp_pwd FROM v_pmi_ods_parent_portal_users ou) AS dt
                ON DUPLICATE Key UPDATE password = dt.pp_pwd;
                
                                    
                UPDATE pp_user p
                    JOIN v_pmi_ods_parent_portal_users ou
                        ON p.login = ou.pp_user_id
                SET active_flag = 1; 
                
                truncate TABLE pp_user_student_list;
                
                INSERT pp_user_student_list (pp_user_id, student_id, active_flag, last_user_id)
                SELECT  DISTINCT p.pp_user_id,
                        s.student_id,
                        1,
                        1234
                FROM pp_user p
                    JOIN v_pmi_ods_parent_portal_users ou
                        ON p.login = ou.pp_user_id
                    JOIN c_student s
                        ON (ou.student_id * 1) = s.student_code
                WHERE p.active_flag = 1;
                
    
            END IF;
END PROC;
//
