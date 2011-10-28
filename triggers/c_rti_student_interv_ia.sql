DROP TRIGGER IF EXISTS `c_rti_student_interv_ia` //
CREATE definer=`dbadmin`@`localhost` trigger `c_rti_student_interv_ia` AFTER INSERT ON `c_rti_student_interv`
  FOR EACH ROW


BEGIN
    
    INSERT INTO c_rti_student_interv_au 
        
    SET     intervention_item_id = NEW.intervention_item_id
            ,student_id = NEW.student_id
            ,measure_id = NEW.measure_id
            ,intervention_id = NEW.intervention_id
            ,start_date = NEW.start_date
            ,action_code = 'i'
            ,audit_user_id = NEW.last_user_id
            ,tier_code = NEW.tier_code
            ,area_code = NEW.area_code
            ,subject_text = NEW.subject_text
            ,baseline_score = NEW.baseline_score
            ,goal_score = NEW.goal_score
            ,goal_date = NEW.goal_date
            ,completion_date = NEW.completion_date
            ,interv_user_id = NEW.interv_user_id
            ,remarks = NEW.remarks
            ,school_year_id = NEW.school_year_id
            ,client_id = NEW.client_id
            ,create_user_id = NEW.create_user_id
            ,last_user_id = NEW.last_user_id
            ,create_timestamp = NEW.create_timestamp
            ,last_edit_timestamp = NEW.last_edit_timestamp
    ;

END;
//
