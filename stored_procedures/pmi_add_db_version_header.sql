drop procedure if exists pmi_add_db_version_header//

create definer=`dbadmin`@`localhost` procedure pmi_add_db_version_header(
    p_version_code      varchar(15)
    ,p_release_date     date
    ,p_comment          varchar(150)
)
contains sql
sql security invoker
comment '$Rev$ $Date$'


proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);

    insert into pmi_db_version ( 
        version_code
        ,major_release_number
        ,minor_release_number
        ,point_release_number
        ,release_date
        ,`comment`
        ,create_user_id
        ,last_user_id
        .create_timestamp
    )
    
    values (
        p_version_code
        ,substring_index(p_version_code, '.', 1)
        ,substring_index(substring_index(p_version_code, '.', 2), '.', -1)
        ,substring_index(p_version_code, '.', -1)
        ,p_release_date'
        ,p_comment
        ,1234
        ,1234,
        ,now()
    );

end proc;
//
