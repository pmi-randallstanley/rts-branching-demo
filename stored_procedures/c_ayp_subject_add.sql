/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/c_ayp_subject_add.sql $
$Id: c_ayp_subject_add.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/
DROP PROCEDURE IF EXISTS c_ayp_subject_add //
CREATE definer=`dbadmin`@`localhost` procedure c_ayp_subject_add 
   (
      p_ayp_subject_code varchar(25),
      p_moniker varchar(50),
      p_ayp_test_type_id int
   )
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
proc: BEGIN

   INSERT INTO c_ayp_subject (
      ayp_subject_id, 
      ayp_subject_code,
      moniker, 
      ayp_test_type_id, 
      last_user_id,
      create_timestamp
   ) 
   VALUES (
      pmi_admin.pmi_f_get_next_sequence('curriculum_ayp_sub_strand', 1),
      p_ayp_subject_code,
      p_moniker,
      p_ayp_test_type_id,
      1234,
      current_timestamp
   )
   ON DUPLICATE KEY UPDATE last_user_id = 1234;   
END proc;
//
