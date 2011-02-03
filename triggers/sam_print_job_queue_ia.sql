drop trigger if exists `sam_print_job_queue_ai` //
create definer=`dbadmin`@`localhost` trigger `sam_print_job_queue_ai` after insert on `sam_print_job_queue`
  for each row

/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/triggers/sam_print_job_queue_ia.sql $
$Id: sam_print_job_queue_ia.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

begin

   insert into pmi_events.sam_print_job_queue (
      print_job_id
      , test_id
      , test_event_id
      , school_id
      , status_code
      , queued_user_id
      , job_start_timestamp
      , job_complete_timestamp
      , blank_form_flag
      , comment
      , client_id
      , last_user_id
      ) 
   values (
   new.print_job_id
   , new.test_id
   , new.test_event_id
   , new.school_id
   , new.status_code
   , new.queued_user_id
   , new.job_start_timestamp
   , new.job_complete_timestamp
   , new.blank_form_flag
   , new.comment
   , new.client_id
   , new.last_user_id
   );
            
end;
//
