/*
$Rev: 8370 $ 
$Author: randall.stanley $ 
$Date: 2010-04-01 15:43:48 -0400 (Thu, 01 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/adm_sch_init_security_filter.sql $
$Id: adm_sch_init_security_filter.sql 8370 2010-04-01 19:43:48Z randall.stanley $ 
*/

drop procedure if exists adm_sch_init_security_filter//

create definer=`dbadmin`@`localhost` procedure adm_sch_init_security_filter(p_new_client_id int)
contains sql
sql security invoker
comment '$Rev: 8370 $ $Date: 2010-04-01 15:43:48 -0400 (Thu, 01 Apr 2010) $'


proc: begin 

    declare v_school_client_exists     int(11) default '0';

    select  count(*)
    into    v_school_client_exists
    from    pmi_admin.pmi_client
    where   client_id = p_new_client_id
    and     shared_db_member_flag = 1
    ;
    

    if v_school_client_exists > 0 then

        delete from pmi_filter_da_list
        where   client_id = p_new_client_id
        ;
        
        # Add customer level (school) visibility
        insert pmi_filter_da_list (
            client_id
            ,filter_id
            ,accessor_id
            ,last_user_id
        )

        select  p_new_client_id
            ,f.filter_id
            ,p_new_client_id
            ,1234
            
        from    pmi_filter as f
        where   f.filter_code in ('global','glbDemographics','glbGender','glbSwd','glbLep','glbEthnicity','glbGrade'
                                ,'glbGeographical','glbCourseSubject','glbCourse','glbClass','glbStudent','cohort'
                                ,'cohDemographics','cohGender','cohSwd','cohLep','cohEthnicity','cohGrade','cohGeographical'
                                ,'cohCourseSubject','cohCourse','cohClass' )
        on duplicate key update last_user_id = values(last_user_id)
        ;

        # Add principal level visibility for school solution
        insert pmi_filter_da_list (
            client_id
            ,filter_id
            ,accessor_id
            ,last_user_id
        )

        select  p_new_client_id
            ,f.filter_id
            ,r.role_id
            ,1234
            
        from    pmi_filter as f
        cross join  c_role as r
                on      r.role_code in ('principal')
        where   f.filter_code in ('glbEd','glbTeacher','cohEd','cohTeacher')
        on duplicate key update last_user_id = values(last_user_id)
        ;

        # Add school admin level visibility for school solution
        insert pmi_filter_da_list (
            client_id
            ,filter_id
            ,accessor_id
            ,last_user_id
        )

        select  p_new_client_id
            ,f.filter_id
            ,r.role_id
            ,1234
            
        from    pmi_filter as f
        cross join  c_role as r
                on      r.role_code in ('schoolAdmin')
        where   f.filter_code in ('glbEd','glbTeacher','cohEd','cohTeacher')
        on duplicate key update last_user_id = values(last_user_id)
        ;

    end if;

end proc;
//
