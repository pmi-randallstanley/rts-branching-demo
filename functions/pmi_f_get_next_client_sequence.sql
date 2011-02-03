DROP FUNCTION IF EXISTS pmi_f_get_next_client_sequence//

CREATE definer=`dbadmin`@`localhost` function pmi_f_get_next_client_sequence (p_moniker varchar(25), p_incr_value int) 
RETURNS char(10)
DETERMINISTIC
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'

/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/functions/pmi_f_get_next_client_sequence.sql $
$Id: pmi_f_get_next_client_sequence.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

BEGIN
   INSERT INTO pmi_client_sequence 
   SET moniker = p_moniker,
      last_sequence_id = LAST_INSERT_ID(0 + p_incr_value )
   ON DUPLICATE KEY UPDATE last_sequence_id = LAST_INSERT_ID(last_sequence_id + p_incr_value);
   
    RETURN CAST(LAST_INSERT_ID() AS char(10));
END;
//
