DROP TRIGGER IF EXISTS `sam_student_response_ia` //
CREATE definer=`dbadmin`@`localhost` trigger `sam_student_response_ia` AFTER INSERT ON `sam_student_response`
  FOR EACH ROW


/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/triggers/sam_student_response_ia.sql $
$Id: sam_student_response_ia.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

BEGIN
    
    # only insert if change made via UI
    IF NEW.gui_edit_flag = 1 THEN

        INSERT INTO sam_student_response_au 
            
        SET     action_code = 'i',
                audit_user_id = NEW.last_user_id,
                test_id = NEW.test_id,
                test_event_id = NEW.test_event_id,
                student_id = NEW.student_id,
                test_question_id = NEW.test_question_id,
                student_answer = NEW.student_answer,
                rubric_score = NEW.rubric_score,
                rubric_value = NEW.rubric_value,
                last_user_id = NEW.last_user_id,
                create_timestamp = NEW.create_timestamp,
                last_edit_timestamp = NEW.last_edit_timestamp;
    END IF;         
END;
//
