/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/pmi_dropdown_menu_add_node.sql $
$Id: pmi_dropdown_menu_add_node.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

DROP PROCEDURE IF EXISTS pmi_dropdown_menu_add_node //

CREATE definer=`dbadmin`@`localhost` procedure pmi_dropdown_menu_add_node 
   (
      p_parent_id int,
      p_menu_code varchar(25),
      p_display_text varchar(50),
      p_display_order decimal(9,3),
      p_tab_width smallint,
      p_panel_width smallint,
      p_image_name tinytext,
      p_menu_href tinytext,
      p_target_action varchar(30),
      p_description text
   )

contains sql
sql security invoker
comment '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'

proc: BEGIN


   DECLARE  v_insert_right int;
   DECLARE  v_msg_id       int;
   
   SELECT   rgt INTO v_insert_right
   FROM     pmi_dropdown_menu
   WHERE        menu_id = p_parent_id;
   
   SELECT   p.msg_id INTO v_msg_id
   FROM     pmi_sys_message AS p
   WHERE    p.msg_moniker = CONCAT('ddm_',p_menu_code);
   
   IF v_msg_id IS NULL THEN
   
         SET @last_sequence_id := pmi_admin.pmi_f_get_next_sequence('message', 1);
   
         insert pmi_sys_message (
            msg_id,
            msg_moniker, 
            msg_text, 
            last_user_id,
            create_timestamp
         )
         
         VALUES  (@last_sequence_id,
            CONCAT('ddm_',p_menu_code),
            p_display_text,
            1234,
            current_timestamp);
   
         SET v_msg_id = @last_sequence_id;

   END IF;
   
   
    load_data: BEGIN
       DECLARE EXIT handler FOR SQLEXCEPTION rollback;
       start transaction;            
    
   
       UPDATE   pmi_dropdown_menu
       SET      lft =   CASE    WHEN    lft > v_insert_right THEN lft + 2
                            ELSE lft
                      END,
                rgt =   CASE    WHEN    rgt >= v_insert_right THEN rgt + 2
                            ELSE rgt
                      END 
       WHERE    rgt >= v_insert_right;
       
    
       
       INSERT INTO pmi_dropdown_menu (
          menu_id, 
          menu_code, 
          lft, 
          rgt, 
          parent_id, 
          msg_id, 
          display_order, 
          tab_width, 
          panel_width, 
          image_name, 
          menu_href, 
          target_action, 
          last_user_id, 
          create_timestamp, 
          description
          ) 
    
       VALUES (
          pmi_admin.pmi_f_get_next_sequence('menu_item', 1), 
          p_menu_code, 
          v_insert_right,
          v_insert_right + 1,
          p_parent_id,
          v_msg_id,
          p_display_order, 
          p_tab_width, 
          p_panel_width, 
          p_image_name, 
          p_menu_href, 
          p_target_action, 
          1234, 
          current_timestamp, 
          p_description
       
       );
       commit;
    END load_data;

-- SELECT v_insert_right, v_msg_id;

END proc;
//
