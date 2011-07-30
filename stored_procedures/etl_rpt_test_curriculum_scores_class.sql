DROP PROCEDURE IF EXISTS `etl_rpt_test_curriculum_scores_class` //

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt_test_curriculum_scores_class`()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev$ $Date$'
BEGIN

    declare no_more_rows                boolean; 
    declare v_test_id                   int(11) default '0';

    declare cur_test_id cursor for 
        select  test_id
        from    rpt_test_curriculum_scores
        group by test_id
        ;

    declare continue handler for not found 
    set no_more_rows = true;

    truncate table rpt_test_curriculum_scores_class;

    # Populate table - looping by test
    open cur_test_id;
    loop_cur_test_id: loop
    
        fetch  cur_test_id 
        into   v_test_id;
               
        if no_more_rows then
            close cur_test_id;
            leave loop_cur_test_id;
        end if;

    
        insert into rpt_test_curriculum_scores_class (
           class_id
           ,test_id
           ,curriculum_id
           ,points_earned
           ,points_possible
           ,last_user_id
        ) 
           
        select   cle.class_id
           ,rtcs.test_id
           ,rtcs.curriculum_id
           ,sum(rtcs.points_earned) as points_earned
           ,sum(rtcs.points_possible) as points_possible
           ,1234
        
        from    rpt_test_curriculum_scores as rtcs
        join    c_class_enrollment as cle
                on      cle.student_id = rtcs.student_id
        join    c_class as cl
                on      cl.class_id = cle.class_id
        where   rtcs.test_id = v_test_id
        group by cle.class_id, rtcs.test_id, rtcs.curriculum_id
        ;

    end loop loop_cur_test_id;

end;
//
