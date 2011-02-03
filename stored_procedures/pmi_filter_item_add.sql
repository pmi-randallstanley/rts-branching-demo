/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/pmi_filter_item_add.sql $
$Id: pmi_filter_item_add.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

drop procedure if exists pmi_filter_item_add //

create definer=`dbadmin`@`localhost` procedure pmi_filter_item_add 
   (
      p_filter_group_id int,
      p_filter_item_code varchar(25),
      p_filter_item_text varchar (50)
   )

CONTAINS SQL
SQL SECURITY INVOKER
comment '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'

proc: begin


   declare  v_msg_id       int;
      
   select   p.msg_id into v_msg_id
   from     pmi_sys_message p
   where    p.msg_moniker = concat('fitm_', p_filter_item_code);

   set @item_id := pmi_admin.pmi_f_get_next_sequence('filter_item', 1);

   
   if v_msg_id is null then
   
         set @message_id := pmi_admin.pmi_f_get_next_sequence('message', 1);
   
         insert pmi_sys_message (
            msg_id,
            msg_moniker, 
            msg_text, 
            last_user_id      
         )
         
         values  (@message_id,
            concat('fitm_', p_filter_item_code),
            p_filter_item_text,
            0 );
   
         set v_msg_id = @message_id;

   end if;
   
   INSERT INTO pmi_filter_group_item (
      filter_group_id, 
      filter_item_id, 
      filter_item_code, 
      msg_id, 
      last_user_id
      ) 
   
   Values ( p_filter_group_id,
      @item_id,
      p_filter_item_code,
      @message_id,
      101 );     
      
end proc;
//
