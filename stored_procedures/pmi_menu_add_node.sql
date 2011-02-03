/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/pmi_menu_add_node.sql $
$Id: pmi_menu_add_node.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

drop procedure if exists pmi_menu_add_node //

create definer=`dbadmin`@`localhost` procedure pmi_menu_add_node 
   (
      p_parent_id int,
      p_menu_moniker varchar(50),
      p_menu_text varchar(50),
      p_page_id int,
      p_menu_type_code char(1),
      p_menu_msg_flag tinyint # 1 = add msg; 0 = do not add msg.
   )

CONTAINS SQL
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
SQL SECURITY INVOKER


proc: begin


   DECLARE  v_insert_right int;
   DECLARE  v_msg_id       int;
   DECLARE  v_not_found BOOLEAN default 0;
   DECLARE  v_add_menu_message_flag int default 0;

   DECLARE CONTINUE HANDLER FOR NOT FOUND 
      SET v_not_found = TRUE;
   
   IF p_menu_msg_flag IS NOT NULL THEN
        SET v_add_menu_message_flag = p_menu_msg_flag;
   END IF;
   
   select   rgt into v_insert_right
   from     pmi_menu
   where        menu_id = p_parent_id;
   
   IF !v_not_found THEN
         
         IF  v_add_menu_message_flag = 1 THEN
             insert pmi_sys_message (
                msg_id,
                msg_moniker, 
                msg_text, 
                last_user_id,
                create_timestamp      
             )
             
             values  (pmi_admin.pmi_f_get_next_sequence('pmi_message', 1),
                concat('mnu',p_menu_moniker),
                p_menu_text,
                0,
                current_timestamp )
                ON DUPLICATE KEY UPDATE last_user_id = 1234;
       
            select   p.msg_id into v_msg_id
            from     pmi_sys_message p
            where    p.msg_moniker = concat('mnu',p_menu_moniker);
        END IF;
   
   
        load_data: begin
           declare exit handler for sqlexception rollback;
           start transaction;            
        
       
       update   pmi_menu
       set      lft =   case    when    lft > v_insert_right then lft + 2
                            else lft
                      end,
                rgt =   case    when    rgt >= v_insert_right then rgt + 2
                            else rgt
                      end 
       where    rgt >= v_insert_right;
       
       
       insert pmi_menu (
          menu_id,
          moniker,
          lft,
          rgt,
          parent_id,
          menu_type_code,
          page_id,
          #menu_path,
          msg_id,
          last_user_id,
          create_timestamp
       )
       
       values( pmi_admin.pmi_f_get_next_sequence('pmi_menu', 1),
          p_menu_moniker,
          v_insert_right,
          v_insert_right + 1,
          p_parent_id,
          p_menu_type_code,
          p_page_id,
          #p_menu_path,
          v_msg_id,
          1234,
          current_timestamp);
    
           commit;
        end load_data;

    END IF;

end proc;
//
