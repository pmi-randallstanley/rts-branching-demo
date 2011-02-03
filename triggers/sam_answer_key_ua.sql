drop trigger if exists `sam_answer_key_ua` //

create definer=`dbadmin`@`localhost` trigger `sam_answer_key_ua` after update on `sam_answer_key` 
for each row

/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/triggers/sam_answer_key_ua.sql $
$Id: sam_answer_key_ua.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

begin

    if  coalesce(old.question_type_id,0)        != coalesce(new.question_type_id,0)     or
        coalesce(old.answer,'')                 != coalesce(new.answer,'')              or 
        coalesce(old.rubric_id,0)               != coalesce(new.rubric_id,0)            or
        coalesce(old.flatfile_question_num,0)   != coalesce(new.flatfile_question_num,0) then

        update sam_test as t
        set t.force_rescore_flag = 1
            ,t.last_user_id = new.last_user_id
            ,t.valid_fsl_flag = 0
        where t.test_id = old.test_id;

    end if;
                            
end;
//
