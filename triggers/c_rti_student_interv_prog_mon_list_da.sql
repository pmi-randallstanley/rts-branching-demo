DROP TRIGGER IF EXISTS `c_rti_student_interv_prog_mon_list_da` //
CREATE definer=`dbadmin`@`localhost` trigger `c_rti_student_interv_prog_mon_list_da` AFTER DELETE ON `c_rti_student_interv_prog_mon_list`
  FOR EACH ROW


BEGIN
    
    INSERT INTO c_rti_student_interv_prog_mon_list_au 
        
    SET     intervention_item_id = OLD.intervention_item_id
            ,prog_mon_id = OLD.prog_mon_id
            ,event_date = OLD.event_date
            ,score = OLD.score
            ,action_code = 'd'
            ,audit_user_id = OLD.last_user_id
            ,interv_user_id = OLD.interv_user_id
            ,school_year_id = OLD.school_year_id
            ,client_id = OLD.client_id
            ,create_user_id = OLD.create_user_id
            ,last_user_id = OLD.last_user_id
            ,create_timestamp = OLD.create_timestamp
            ,last_edit_timestamp = OLD.last_edit_timestamp
    ;

END;
//
