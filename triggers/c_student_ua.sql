DROP TRIGGER IF EXISTS `c_student_ua`//
CREATE definer=`dbadmin`@`localhost` trigger `c_student_ua` AFTER UPDATE ON `c_student`
  FOR EACH ROW

/*
$Rev: 8068 $ 
$Author: randall.stanley $ 
$Date: 2010-01-04 14:18:42 -0500 (Mon, 04 Jan 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/triggers/c_student_ua.sql $
$Id: c_student_ua.sql 8068 2010-01-04 19:18:42Z randall.stanley $ 
*/

BEGIN

    if new.active_flag = 0  
         or ( coalesce(old.login, '') != coalesce(new.login, '') and new.login is null) then

        delete from pmi_user.ola_student_login
        where   client_id = old.client_id
        and     student_id = old.student_id
        ;

    end if;
    
    if new.login is not null and new.active_flag = 1 then
    
        select  client_code
        into    @client_code
        from    pmi_admin.pmi_client
        where   client_id = new.client_id
        ;

        insert  pmi_user.ola_student_login

        set client_id = new.client_id
            ,student_id = new.student_id
            ,client_code = @client_code
            ,login = new.login
            ,`password` = new.`password`
            ,active_flag = new.active_flag
            ,create_timestamp = new.create_timestamp
            
        on duplicate key update login = new.login
            ,`password` = new.`password`
            ,active_flag = new.active_flag
        ;

    end if;
    
END;

//
