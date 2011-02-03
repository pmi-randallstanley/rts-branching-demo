drop function if exists pmi_f_get_next_sequence_app_db //

create definer=`dbadmin`@`localhost` function pmi_f_get_next_sequence_app_db (p_sequence_code varchar(64), p_incr_value int) 
returns int
deterministic
contains sql
sql security invoker
comment '$rev$ $date$'

/*
$Rev: 8372 $
$Author: randall.stanley $ 
$Date: 2010-04-01 15:46:13 -0400 (Thu, 01 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/functions/pmi_f_get_next_sequence_app_db.sql $
$Id: pmi_f_get_next_sequence_app_db.sql 8372 2010-04-01 19:46:13Z randall.stanley $ 
*/

begin

    update  pmi_sequence_local
    set     last_value_id = last_insert_id(last_value_id + p_incr_value)
    where   sequence_code = p_sequence_code
    ;

    return last_insert_id();
end

//
