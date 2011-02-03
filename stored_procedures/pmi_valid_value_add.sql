/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/pmi_valid_value_add.sql $
$Id: pmi_valid_value_add.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

drop procedure if exists pmi_valid_value_add //

create definer=`dbadmin`@`localhost` procedure pmi_valid_value_add 
   (
      p_vv_group_id int,
      p_vv_code varchar(25),
      p_vv_item_name varchar(25),
      p_vv_text varchar (50)
   )

CONTAINS SQL
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
SQL SECURITY INVOKER


proc: begin


   declare  v_msg_id          int;
   declare  v_vv_group_name   varchar(25);
      
   select   vvg.group_name
   into     v_vv_group_name
   
   from     pmi_sys_valid_value_group vvg
   where    vvg.valid_value_group_id = p_vv_group_id;
   
     insert pmi_sys_message (
        msg_id,
        msg_moniker, 
        msg_text, 
        last_user_id,
        create_timestamp      
     )
     
     values  (pmi_admin.pmi_f_get_next_sequence('message', 1),
        concat('vv_', v_vv_group_name, '_', p_vv_item_name),
        p_vv_text,
        1234,
        current_timestamp )
        ON DUPLICATE KEY UPDATE last_user_id = 1234;
 
    select   p.msg_id into v_msg_id
    from     pmi_sys_message p
    where    p.msg_moniker = concat('vv_', v_vv_group_name, '_', p_vv_item_name);
      
   
   INSERT INTO pmi_sys_valid_value (
      valid_value_group_id, 
      valid_value_id, 
      value_code, 
      msg_id,
      last_user_id,
      create_timestamp
   ) 
   VALUES (
      p_vv_group_id, 
      pmi_admin.pmi_f_get_next_sequence('valid_value', 1), 
      p_vv_code, 
      v_msg_id,
      1234,
      current_timestamp
   );
      
   
      
end proc;
//
