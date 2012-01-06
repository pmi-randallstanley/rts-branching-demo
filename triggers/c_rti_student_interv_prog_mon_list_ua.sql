DROP TRIGGER IF EXISTS `c_rti_student_interv_prog_mon_list_ua` //
CREATE definer=`dbadmin`@`localhost` trigger `c_rti_student_interv_prog_mon_list_ua` AFTER UPDATE ON `c_rti_student_interv_prog_mon_list`
  FOR EACH ROW


BEGIN

    INSERT INTO c_rti_student_interv_prog_mon_list_au 
        
    SET     intervention_item_id = NEW.intervention_item_id
            ,prog_mon_id = NEW.prog_mon_id
            ,event_date = OLD.event_date
            ,score = IF(NEW.score != OLD.score, OLD.score, NULL)
            ,action_code = 'u'
            ,audit_user_id = NEW.last_user_id
            ,interv_user_id = IF(NEW.interv_user_id != OLD.interv_user_id, OLD.interv_user_id, NULL)
            ,school_year_id = IF(NEW.school_year_id != OLD.school_year_id, OLD.school_year_id, NULL)
            ,client_id = IF(NEW.client_id != OLD.client_id, OLD.client_id, NULL)
            ,create_user_id = NEW.create_user_id
            ,last_user_id = IF(NEW.last_user_id != OLD.last_user_id, OLD.last_user_id, NULL)
            ,create_timestamp = NEW.create_timestamp
            ,last_edit_timestamp = IF(NEW.last_edit_timestamp != OLD.last_edit_timestamp, OLD.last_edit_timestamp, NULL)

    ;

END;
//
