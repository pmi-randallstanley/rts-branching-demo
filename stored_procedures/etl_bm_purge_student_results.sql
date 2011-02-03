/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_purge_student_results.sql $
$Id: etl_bm_purge_student_results.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

DROP PROCEDURE IF EXISTS etl_bm_purge_student_results //

CREATE definer=`dbadmin`@`localhost` procedure etl_bm_purge_student_results(OUT p_delete_count INT(11) )
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
CONTAINS SQL
SQL SECURITY INVOKER


BEGIN

--  ====================================================================================================
--   This procedure will purge student responses where the record in sam_test_student has the purge flag set
--   
--  ====================================================================================================

    DECLARE no_more_rows BOOLEAN; 
    DECLARE v_test_id          int(11);
           
    DECLARE cur_1 CURSOR FOR 
    SELECT  test_id
    FROM    sam_test_student
    WHERE   purge_responses_flag = 1
    GROUP by test_id
    ;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND 
    SET no_more_rows = TRUE;

    drop table if exists tmp_bbcard_tests;
    create table tmp_bbcard_tests (
      bb_group_id int(11) not null,
      bb_measure_id int(11) not null,
      test_id int(11) not null,
      primary key  (`bb_group_id`,`bb_measure_id`),
      key `tmp_bbcard_tests_test_id` (`test_id`)
    );
   
    -- Do any student responses need to be purged?      
    SELECT  count(*)
    INTO    @purge_count
    FROM    sam_test_student
    WHERE   purge_responses_flag = 1
    ;

    SELECT @purge_count into p_delete_count;

    if @purge_count > 0 then

        select  bb_group_id
        into    @bb_group_id
        from    pm_baseball_group
        where   bb_group_code = 'assessments'
        ;
        
        # load tmp table used for deleting purged tests
        # and adding valid test results
        insert  tmp_bbcard_tests ( 
            bb_group_id
            ,bb_measure_id
            ,test_id
        )
        
        select  bm.bb_group_id
            ,bm.bb_measure_id
            ,t.test_id
            
        from    pm_baseball_measure as bm
        join    sam_test as t
                on      cast(bm.bb_measure_code as signed) = t.test_id
        where   bm.bb_group_id = @bb_group_id
        ;
   
        OPEN cur_1;
        
        loop_cur_1: LOOP
        
            FETCH  cur_1 
            INTO   v_test_id;
                   
            IF no_more_rows THEN
                CLOSE cur_1;
                LEAVE loop_cur_1;
            END IF;
                
            # purge responses for given test.
            delete  srs.*
            from    sam_test_student as sts
            join    sam_student_response as srs
                    on      sts.test_id = srs.test_id
                    and     sts.test_event_id = srs.test_event_id
                    and     sts.student_id = srs.student_id
            where   sts.test_id = v_test_id
            and     sts.purge_responses_flag = 1
            ;

            # Remove BBCard Assess data linked to Tests
            delete  rpt.*
            from    sam_test_student as sts
            join    tmp_bbcard_tests as tmp1
                    on      sts.test_id = tmp1.test_id
            join    rpt_baseball_detail_assessment as rpt
                    on      tmp1.bb_group_id = rpt.bb_group_id
                    and     tmp1.bb_measure_id = rpt.bb_measure_id
                    and     sts.student_id = rpt.student_id
            where   sts.test_id = v_test_id
            and     sts.purge_responses_flag = 1
            ;
    
            --  reset purge flag
            update  sam_test_student as sts
            set     sts.purge_responses_flag = 0
            where   sts.test_id = v_test_id
            ;
    
        END LOOP loop_cur_1;

    end if;

    drop table if exists tmp_bbcard_tests;
   
END;
//
