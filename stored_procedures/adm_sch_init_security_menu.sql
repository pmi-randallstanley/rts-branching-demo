/*
$Rev: 8479 $ 
$Author: randall.stanley $ 
$Date: 2010-04-30 08:22:58 -0400 (Fri, 30 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/adm_sch_init_security_menu.sql $
$Id: adm_sch_init_security_menu.sql 8479 2010-04-30 12:22:58Z randall.stanley $ 
*/

drop procedure if exists adm_sch_init_security_menu//

create definer=`dbadmin`@`localhost` procedure adm_sch_init_security_menu(p_new_client_id int)
contains sql
sql security invoker
comment '$Rev: 8479 $ $Date: 2010-04-30 08:22:58 -0400 (Fri, 30 Apr 2010) $'


proc: begin 

    declare v_school_client_exists     int(11) default '0';

    select  count(*)
    into    v_school_client_exists
    from    pmi_admin.pmi_client
    where   client_id = p_new_client_id
    and     shared_db_member_flag = 1
    ;
    

    if v_school_client_exists > 0 then

        delete from pmi_menu_da_list
        where   client_id = p_new_client_id
        ;
        
        # Add customer level (school) visibility
        insert pmi_menu_da_list (
            client_id
            ,menu_id
            ,accessor_id
            ,last_user_id
        )

        select  p_new_client_id
            ,m.menu_id
            ,p_new_client_id
            ,1234
            
        from    pmi_menu as m
        where   m.moniker in ('progMon','pmLeading','itemAnalysis','rankingByBenchmark','studentItemAnalysis','scoresByTest'
                                ,'scoresByBenchmark','comparativeResults','samV2','samReporting','CurriculumExplorer'
                                ,'coreBuilder','cbWizard','cbTeachers','cbStudents','cbCourses','cbSchedules'
                                ,'StandardScoresByTest','PendItemAnalysis','PendStudentItemAnalysis','sdCourses','sdAssessments')
        on duplicate key update last_user_id = values(last_user_id)
        ;

        # Add principal level visibility for school solution
        insert pmi_menu_da_list (
            client_id
            ,menu_id
            ,accessor_id
            ,last_user_id
        )

        select  p_new_client_id
            ,m.menu_id
            ,r.role_id
            ,1234
            
        from    pmi_menu as m
        cross join  c_role as r
                on      r.role_code in ('principal')
        where   m.moniker in ('samTestMgt','TestBrowse','TestEventBrowse','OnlineScoring','admin','SystemReporting','UsageReport')
        on duplicate key update last_user_id = values(last_user_id)
        ;

        # Add school admin level visibility for school solution
        insert pmi_menu_da_list (
            client_id
            ,menu_id
            ,accessor_id
            ,last_user_id
        )

        select  p_new_client_id
            ,m.menu_id
            ,r.role_id
            ,1234
            
        from    pmi_menu as m
        cross join  c_role as r
                on      r.role_code in ('schoolAdmin')
        where   m.moniker in ('samTestMgt','TestBrowse','TestEventBrowse','OnlineScoring','admin','adminGui','newsDistrict'
                                ,'adminSystem','userManager','SystemReporting','UsageReport')
        on duplicate key update last_user_id = values(last_user_id)
        ;

    end if;

end proc;
//
