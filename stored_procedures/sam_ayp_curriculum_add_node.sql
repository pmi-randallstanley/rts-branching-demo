DROP PROCEDURE IF EXISTS sam_ayp_curriculum_add_node//

CREATE definer=`dbadmin`@`localhost` procedure sam_ayp_curriculum_add_node 
   (
    p_ayp_curriculum_id int(10)
    ,p_parent_id int(10)
    ,p_ayp_curriculum_code varchar(50)
    ,p_level varchar(15)
    ,p_desc varchar(1500)
    ,p_client_id int(10)
   )

CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'

/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/sam_ayp_curriculum_add_node.sql $
$Id: sam_ayp_curriculum_add_node.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

proc: BEGIN


   DECLARE  v_insert_right int;
   DECLARE  v_msg_id       int;
   DECLARE  v_not_found BOOLEAN default 0;

   DECLARE CONTINUE HANDLER FOR NOT FOUND 
      SET v_not_found = TRUE;

   SELECT   rgt INTO v_insert_right
   FROM     sam_ayp_curriculum
   WHERE    ayp_curriculum_id = p_parent_id;

   IF !v_not_found THEN

        UPDATE  sam_ayp_curriculum
        SET     lft =   CASE    WHEN    lft > v_insert_right THEN lft + 2
                                ELSE lft
                        END,
                    rgt =   CASE    WHEN    rgt >= v_insert_right THEN rgt + 2
                                ELSE rgt
                        END 
        WHERE   rgt >= v_insert_right;
        
        INSERT INTO sam_ayp_curriculum (
            ayp_curriculum_id
            ,ayp_curriculum_code
            ,lft
            ,rgt
            ,parent_id
            ,`level`
            ,client_id
            ,last_user_id
            ,create_timestamp
            ,description
            ,suppress_code_flag
            ) 
        
        
        VALUES( p_ayp_curriculum_id
            ,p_ayp_curriculum_code
            ,v_insert_right
            ,v_insert_right + 1
            ,p_parent_id
            ,p_level
            ,p_client_id
            ,1234
            ,now()
            ,p_desc
            ,case when p_level in ('subject', 'strand')
                    then 1
                 else   0
            end);
    END IF;

END proc;
//
