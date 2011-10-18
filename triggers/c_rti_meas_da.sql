DROP TRIGGER IF EXISTS `c_rti_meas_da` //
CREATE definer=`dbadmin`@`localhost` trigger `c_rti_meas_da` AFTER DELETE ON `c_rti_meas`
  FOR EACH ROW


BEGIN
    
    INSERT INTO c_rti_meas_au 
        
    SET     measure_id = OLD.measure_id
            ,moniker = OLD.moniker
            ,sort_order = OLD.sort_order
            ,action_code = 'd'
            ,audit_user_id = OLD.last_user_id
            ,client_id = OLD.client_id
            ,create_user_id = OLD.create_user_id
            ,last_user_id = OLD.last_user_id
            ,create_timestamp = OLD.create_timestamp
            ,last_edit_timestamp = OLD.last_edit_timestamp
    ;

END;
//
