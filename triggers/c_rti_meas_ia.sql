DROP TRIGGER IF EXISTS `c_rti_meas_ia` //
CREATE definer=`dbadmin`@`localhost` trigger `c_rti_meas_ia` AFTER INSERT ON `c_rti_meas`
  FOR EACH ROW


BEGIN
    
    INSERT INTO c_rti_meas_au 
        
    SET     measure_id = NEW.measure_id
            ,moniker = NEW.moniker
            ,sort_order = NEW.sort_order
            ,action_code = 'i'
            ,audit_user_id = NEW.last_user_id
            ,client_id = NEW.client_id
            ,create_user_id = NEW.create_user_id
            ,last_user_id = NEW.last_user_id
            ,create_timestamp = NEW.create_timestamp
            ,last_edit_timestamp = NEW.last_edit_timestamp
    ;

END;
//
