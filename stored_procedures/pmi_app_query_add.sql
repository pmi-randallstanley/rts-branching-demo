/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/pmi_app_query_add.sql $
$Id: pmi_app_query_add.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/


drop procedure if exists pmi_app_query_add //

create definer=`dbadmin`@`localhost` procedure pmi_app_query_add 
   (
      p_query_name varchar(25),
      p_user_id int
   )
CONTAINS SQL
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
SQL SECURITY INVOKER

proc: begin

   set @id := pmi_admin.pmi_f_get_next_sequence('app_query', 1);

   INSERT INTO black_box_dev.pmi_app_query (
      query_id, 
      query_code,
      last_user_id
   ) 
   
   VALUES (
      @id, 
      p_query_name,
      p_user_id
      );

end proc;
//
