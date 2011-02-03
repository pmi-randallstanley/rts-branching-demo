drop trigger if exists `sam_print_job_queue_ua` //
create definer=`dbadmin`@`localhost` trigger `sam_print_job_queue_ua` after update on `sam_print_job_queue`
  for each row

/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/triggers/sam_print_job_queue_ua.sql $
$Id: sam_print_job_queue_ua.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

begin
   
    update pmi_events.sam_print_job_queue
    set   status_code = new.status_code
         , queued_user_id = new.queued_user_id
         , job_start_timestamp = new.job_start_timestamp
         , job_complete_timestamp = new.job_complete_timestamp
         , blank_form_flag = new.blank_form_flag
         , comment = new.comment
         , client_id = new.client_id
         , last_user_id = new.last_user_id
    where print_job_id = old.print_job_id;
   
    if new.status_code in ('c','d','e','p') and new.blank_form_flag = 0 then
        # answer sheet browse table for pre-slug forms
        update  sam_test_event_school_list 
        set     answer_sheet_status_code = 
                case when new.status_code = 'c' then 'a'
                    else new.status_code
                end
        where   print_job_id = old.print_job_id
        ;
    # Handle blank forms for OCR validation
    elseif new.status_code in ('c','d','e','p') and new.blank_form_flag = 1 and new.test_event_id is null then
        update  sam_test
        set     print_job_id = 
                case when new.status_code = 'c' then new.print_job_id
                    else null
                end
        where   test_id = old.test_id
        ;
    # Blank forms on Test Event
    elseif new.status_code in ('c','d','e','p') and new.blank_form_flag = 1 then
        update  sam_test_event 
        set     blank_form_status_code = 
                case when new.status_code = 'c' then 'a'
                    else new.status_code
                end
        where   test_id = old.test_id
        and     test_event_id = old.test_event_id
        ;
    end if;
    
end;
//
