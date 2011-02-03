DROP FUNCTION IF EXISTS pmi_f_get_etl_setting//

CREATE definer=`dbadmin`@`localhost` function pmi_f_get_etl_setting (p_etl_setting_code varchar(100)) 
RETURNS varchar(100)
DETERMINISTIC
CONTAINS SQL
SQL SECURITY INVOKER
comment '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'

/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/functions/pmi_f_get_etl_setting.sql $
$Id: pmi_f_get_etl_setting.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

BEGIN
    DECLARE v_not_found BOOLEAN default 0;
    DECLARE v_etl_setting varchar(100);

    DECLARE CONTINUE HANDLER FOR NOT FOUND 
        SET v_not_found = TRUE;

    SELECT  l.value
    INTO    v_etl_setting
    FROM    pmi_etl_client_settings_list AS l
    WHERE   l.etl_setting_code = p_etl_setting_code;
    
    IF v_not_found THEN
        SET v_etl_setting = NULL;
    END IF;
    
    RETURN v_etl_setting;
END;
//
