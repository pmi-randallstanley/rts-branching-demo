DROP TRIGGER IF EXISTS `sam_student_response_da` //
CREATE definer=`dbadmin`@`localhost` trigger `sam_student_response_da` AFTER DELETE ON `sam_student_response`
  FOR EACH ROW


/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/triggers/sam_student_response_da.sql $
$Id: sam_student_response_da.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

BEGIN
    
    # only insert if change made via UI
    IF old.gui_edit_flag = 1 THEN

        INSERT INTO sam_student_response_au 
            
        SET     action_code = 'd',
                audit_user_id = OLD.last_user_id,
                test_id = OLD.test_id,
                test_event_id = OLD.test_event_id,
                student_id = OLD.student_id,
                test_question_id = OLD.test_question_id,
                student_answer = OLD.student_answer,
                rubric_score = OLD.rubric_score,
                rubric_value = OLD.rubric_value,
                last_user_id = OLD.last_user_id,
                create_timestamp = OLD.create_timestamp,
                last_edit_timestamp = OLD.last_edit_timestamp;
    END IF;         
END;
//
