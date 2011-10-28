DROP TRIGGER IF EXISTS `c_rti_student_interv_prog_mon_list_ia` //
CREATE definer=`dbadmin`@`localhost` trigger `c_rti_student_interv_prog_mon_list_ia` AFTER INSERT ON `c_rti_student_interv_prog_mon_list`
  FOR EACH ROW


BEGIN
    
    INSERT INTO c_rti_student_interv_prog_mon_list_au 
        
    SET     intervention_item_id = NEW.intervention_item_id
            ,prog_mon_id = NEW.prog_mon_id
            ,event_date = NEW.event_date
            ,score = NEW.score
            ,action_code = 'i'
            ,audit_user_id = NEW.last_user_id
            ,interv_user_id = NEW.interv_user_id
            ,school_year_id = NEW.school_year_id
            ,client_id = NEW.client_id
            ,create_user_id = NEW.create_user_id
            ,last_user_id = NEW.last_user_id
            ,create_timestamp = NEW.create_timestamp
            ,last_edit_timestamp = NEW.last_edit_timestamp
    ;

END;
//
