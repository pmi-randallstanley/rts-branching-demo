drop trigger if exists `sam_test_event_school_list_da` //

create definer=`dbadmin`@`localhost` trigger `sam_test_event_school_list_da` after delete on `sam_test_event_school_list` 
for each row

/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/triggers/sam_test_event_school_list_da.sql $
$Id: sam_test_event_school_list_da.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

begin


    # 1 or 100% means all schools released; above zero but not 1 means partial; zero means none
    select  case when (count(case when release_flag = 1 then release_flag end) / count(*)) >= 1 then 'c'
                when (count(case when release_flag = 1 then release_flag end) / count(*)) > 0  then 'p'
                else 'n'
         end as event_release_status
    into @event_release_code
    
    from    sam_test_event_school_list 
    where   test_id = old.test_id 
    and     test_event_id = old.test_event_id
    ;

    # update event release_status_code based on percentage of schools released
    update sam_test_event
    set release_status_code = @event_release_code
    where   test_id = old.test_id
    and     test_event_id = old.test_event_id
    ;


end;
//
