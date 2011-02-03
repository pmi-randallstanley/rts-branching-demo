DROP TRIGGER IF EXISTS `pp_user_da` //
CREATE definer=`dbadmin`@`localhost` trigger `pp_user_da` AFTER DELETE ON `pp_user`
  FOR EACH ROW

/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/triggers/pp_user_da.sql $
$Id: pp_user_da.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

BEGIN

    DELETE FROM pmi_user.pp_user_login
    WHERE   pp_user_id = old.pp_user_id;
        
END;
//
