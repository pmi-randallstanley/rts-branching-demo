DROP TRIGGER IF EXISTS `c_student_da`//
CREATE definer=`dbadmin`@`localhost` trigger `c_student_da` AFTER DELETE ON `c_student`
  FOR EACH ROW

/*
$Rev: 7374 $ 
$Author: randall.stanley $ 
$Date: 2009-07-15 13:26:40 -0400 (Wed, 15 Jul 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/triggers/c_student_da.sql $
$Id: c_student_da.sql 7374 2009-07-15 17:26:40Z randall.stanley $ 
*/

BEGIN

    delete from pmi_user.ola_student_login
    where   client_id = old.client_id
    and     student_id = old.student_id
    ;
    
END;

//
