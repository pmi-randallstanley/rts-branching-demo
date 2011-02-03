drop trigger if exists `sam_test_event_ua` //

create definer=`dbadmin`@`localhost` trigger `sam_test_event_ua` after update on `sam_test_event` 
for each row

/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/triggers/sam_test_event_ua.sql $
$Id: sam_test_event_ua.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

begin

    # Eval if Test Event purge_flag has changed and if so,
    # update sam_test_student records to correspond
    if  old.purge_flag != new.purge_flag then

        update  sam_test_student
        set     purge_responses_flag = new.purge_flag
        where   test_id = new.test_id
        and     test_event_id = new.test_event_id
        ;

    end if;

end;
//
