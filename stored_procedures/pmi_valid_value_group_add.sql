
/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/pmi_valid_value_group_add.sql $
$Id: pmi_valid_value_group_add.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

drop procedure if exists pmi_valid_value_group_add //

create definer=`dbadmin`@`localhost` procedure pmi_valid_value_group_add 
   (
      p_vv_group_name varchar(25)
   )

CONTAINS SQL
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
SQL SECURITY INVOKER


proc: begin



   set @group_id := pmi_admin.pmi_f_get_next_sequence('valid_value_group', 1);

   INSERT INTO pmi_sys_valid_value_group (
      valid_value_group_id, group_name, create_timestamp
   ) 
   VALUES (@group_id, p_vv_group_name, current_timestamp);


end proc;
//
