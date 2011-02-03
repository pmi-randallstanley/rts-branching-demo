/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/pmi_filter_group_add.sql $
$Id: pmi_filter_group_add.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

drop procedure if exists pmi_filter_group_add //

create definer=`dbadmin`@`localhost` procedure pmi_filter_group_add 
   (
      p_filter_group_code varchar(20),
      p_filter_group_text varchar (50)
   )

CONTAINS SQL
SQL SECURITY INVOKER
comment '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'


proc: begin


   declare  v_msg_id       int;
      
   select   p.msg_id into v_msg_id
   from     pmi_sys_message p
   where    p.msg_moniker = concat('fgrp_',p_filter_group_code);

   set @group_id := pmi_admin.pmi_f_get_next_pmi_seed_id('filter_group', 1);

   
   if v_msg_id is null then
   
         set @message_id := pmi_admin.pmi_f_get_next_pmi_seed_id('message', 1);
   
         insert pmi_sys_message (
            msg_id,
            msg_moniker, 
            msg_text, 
            last_user_id      
         )
         
         values  (@message_id,
            concat('fgrp_', p_filter_group_code),
            p_filter_group_text,
            0 );
   
         set v_msg_id = @message_id;

   end if;
   
   
      
   
   INSERT INTO black_box_dev.pmi_filter_group (
      filter_group_id, 
      filter_group_code, 
      msg_id, 
      last_user_id
      )
      
   VALUES (@group_id, p_filter_group_code, v_msg_id, 101);


end proc;
//
