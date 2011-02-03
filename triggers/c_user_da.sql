DROP TRIGGER IF EXISTS `c_user_da` //
CREATE definer=`dbadmin`@`localhost` trigger `c_user_da` AFTER DELETE ON `c_user`
  FOR EACH ROW

/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/triggers/c_user_da.sql $
$Id: c_user_da.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

BEGIN
    # Do not delete PMI users
    IF old.user_id > 1000000 THEN

        DELETE  
        FROM    pmi_user.user_login
        WHERE   pmi_user_id = old.user_id
        AND     client_id = old.client_id
        ;

        DELETE FROM c_data_accessor
        WHERE   accessor_id = old.user_id;
        
    END IF;
END;
//
