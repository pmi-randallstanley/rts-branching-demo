drop trigger if exists `sam_test_ib`//
create definer=`dbadmin`@`localhost` trigger `sam_test_ib` before insert on `sam_test`
  for each row


begin

    select  external_answer_source_id
    into    @lexmark_answer_source_id
    from    sam_external_answer_source
    where   external_answer_source_code = 'lexmark'
    ;

    if  new.external_answer_source_id = @lexmark_answer_source_id then

        set new.show_on_device_flag = 1;

    end if;
    
end;
//
