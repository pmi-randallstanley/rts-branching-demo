DROP TRIGGER IF EXISTS `c_rti_student_interv_da` //
CREATE definer=`dbadmin`@`localhost` trigger `c_rti_student_interv_da` AFTER DELETE ON `c_rti_student_interv`
  FOR EACH ROW


BEGIN
    
    INSERT INTO c_rti_student_interv_au 
        
    SET     intervention_item_id = OLD.intervention_item_id
            ,student_id = OLD.student_id
            ,measure_id = OLD.measure_id
            ,intervention_id = OLD.intervention_id
            ,start_date = OLD.start_date
            ,action_code = 'd'
            ,audit_user_id = OLD.last_user_id
            ,tier_code = OLD.tier_code
            ,area_code = OLD.area_code
            ,subject_text = OLD.subject_text
            ,baseline_score = OLD.baseline_score
            ,goal_score = OLD.goal_score
            ,goal_date = OLD.goal_date
            ,completion_date = OLD.completion_date
            ,interv_user_id = OLD.interv_user_id
            ,unit_of_meas = OLD.unit_of_meas
            ,frequency = OLD.frequency
            ,duration = OLD.duration
            ,group_size = OLD.group_size
            ,attendance = OLD.attendance
            ,delivery = OLD.delivery
            ,student_response = OLD.student_response
            ,student_status = OLD.student_status
            ,remarks = OLD.remarks
            ,school_year_id = OLD.school_year_id
            ,client_id = OLD.client_id
            ,create_user_id = OLD.create_user_id
            ,last_user_id = OLD.last_user_id
            ,create_timestamp = OLD.create_timestamp
            ,last_edit_timestamp = OLD.last_edit_timestamp
    ;

END;
//
