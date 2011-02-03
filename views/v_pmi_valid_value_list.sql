DROP VIEW IF EXISTS v_pmi_valid_value_list //
CREATE definer=`dbadmin`@`localhost`
SQL SECURITY INVOKER
VIEW v_pmi_valid_value_list 
AS

/*
$Rev: 7378 $ 
$Author: randall.stanley $ 
$Date: 2009-07-15 16:05:14 -0400 (Wed, 15 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/views/v_pmi_valid_value_list.sql $
$Id: v_pmi_valid_value_list.sql 7378 2009-07-15 20:05:14Z randall.stanley $ 
*/

  select   vvg.group_name as moniker,
   vv.value_code,
   vv.sort_order,
   coalesce(smc.msg_text, sm.msg_text) as display_text,
   vv.active_flag

from     pmi_sys_valid_value_group vvg
join     pmi_sys_valid_value vv
         on    vv.valid_value_group_id = vvg.valid_value_group_id
join     pmi_sys_message sm
         on    sm.msg_id = vv.msg_id
left join pmi_sys_message_client smc
         on    smc.msg_id = vv.msg_id;
         
//
