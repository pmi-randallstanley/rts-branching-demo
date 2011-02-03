DROP PROCEDURE IF EXISTS etl_bm_rescore_tests_for_key_update //

CREATE definer=`dbadmin`@`localhost` procedure etl_bm_rescore_tests_for_key_update(OUT p_rescored INT(11) )
COMMENT '$Rev: 9919 $ $Date: 2011-01-24 08:32:17 -0500 (Mon, 24 Jan 2011) $'
CONTAINS SQL
SQL SECURITY INVOKER

/*
$Rev: 9919 $ 
$Author: randall.stanley $ 
$Date: 2011-01-24 08:32:17 -0500 (Mon, 24 Jan 2011) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_bm_rescore_tests_for_key_update.sql $
$Id: etl_bm_rescore_tests_for_key_update.sql 9919 2011-01-24 13:32:17Z randall.stanley $ 
*/

BEGIN

--  ====================================================================================================
--   This procedure will do a re-scoring of both rubric_value and rubric_score on sam_student_responses 
--   for any tests where answers have changed 
--  ====================================================================================================

    DECLARE no_more_rows                BOOLEAN; 
    DECLARE v_test_id                   int(11);
    DECLARE v_external_grading_flag     tinyint(1);
    DECLARE v_ignore_rescore            char(1) default 'n';
    DECLARE v_gr_type_cnt               int(11) default '0';
           
    DECLARE cur_1 CURSOR FOR 
    SELECT  test_id, external_grading_flag, gr_type_cnt
    FROM    tmp_rescore_tests
    ;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND 
    SET no_more_rows = TRUE;
   
    drop table if exists `tmp_rescore_tests`;
    drop table if exists `tmp_ak_grid`;
    drop table if exists `tmp_stu_grid_response_calc`;

    create table `tmp_rescore_tests` (
      `test_id` int(11) not null,
      `external_grading_flag` tinyint(1) not null default '0',
      `gr_type_cnt` int(11) default '0',
      `last_edit_timestamp` timestamp not null default current_timestamp on update current_timestamp,
      primary key  (`test_id`)
    ) engine=innodb default charset=latin1
    ;

    CREATE TABLE `tmp_ak_grid` (
      `test_id` int(11) NOT NULL,
      `test_question_id` int(11) NOT NULL,
      `correct_answer` varchar(255) NOT NULL,
      `round_digits` tinyint(4) NOT NULL,
      `last_user_id` int(11) NOT NULL,
      `create_timestamp` datetime NOT NULL default '1980-12-31 00:00:00',
      `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
      PRIMARY KEY  (`test_id`,`test_question_id`),
      KEY `ind_tmp_ak_grid_round_digits` (`round_digits`,`test_id`,`test_question_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1
    ;

    CREATE TABLE `tmp_stu_grid_response_calc` (
      `test_id` int(11) NOT NULL,
      `test_event_id` int(11) NOT NULL,
      `student_id` int(11) NOT NULL,
      `test_question_id` int(11) NOT NULL,
      `student_answer_raw` varchar(255) default NULL,
      `student_answer_numeric_char` varchar(25) default NULL,
      `numerator_value` smallint(6) default NULL,
      `denominator_value` smallint(6) default NULL,
      `gui_edit_flag` tinyint(1) NOT NULL default '0',
      `last_user_id` int(11) NOT NULL,
      `create_timestamp` datetime NOT NULL default '1980-12-31 00:00:00',
      `last_edit_timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
      PRIMARY KEY  (`test_id`,`test_event_id`,`student_id`,`test_question_id`),
      KEY `ind_tmp_stu_grid_response_calc_stu` (`student_id`),
      KEY `ind_tmp_stu_grid_response_calc_ak` (`test_id`,`test_question_id`),
      KEY `ind_tmp_stu_grid_response_calc_stu_question` (`test_id`,`test_question_id`,`student_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1
    ;


    SET p_rescored = 0;
    
    SET @bmIgnoreTestRescore := pmi_f_get_etl_setting('bmIgnoreTestRescore');
    
    if @bmIgnoreTestRescore is not null then
        set v_ignore_rescore = @bmIgnoreTestRescore;
    end if;

    # Rescore if flag not in place to ignore
    if v_ignore_rescore = 'n' then

        insert tmp_rescore_tests (
            test_id
            ,external_grading_flag
            ,gr_type_cnt
        )
        
        select  t.test_id
            ,max(t.external_grading_flag) as external_grading_flag
            ,count(case when sm.scoring_method_id is not null then ak.test_question_id end) as gr_type_cnt
            
        from    sam_test as t
        join    sam_answer_key as ak 
                on ak.test_id = t.test_id 
        join    sam_question_type as qt 
                on ak.question_type_id = qt.question_type_id
        left join    sam_scoring_method as sm 
                on qt.scoring_method_id = sm.scoring_method_id
                and sm.scoring_method_code = 'g'
        where   t.force_rescore_flag = 1
        and     t.purge_flag = 0
        group by t.test_id
        ;

        -- Do any tables need to be rescored?      
        SELECT  count(*)
        INTO    @test_count
        FROM    tmp_rescore_tests
        ;

        -- set OUTPUT parameter to # of tests needing rescoring
        SELECT @test_count into p_rescored;
    
        OPEN cur_1;
        
        loop_cur_1: LOOP
        
            FETCH  cur_1 
            INTO   v_test_id, v_external_grading_flag, v_gr_type_cnt;
                   
            IF no_more_rows THEN
                CLOSE cur_1;
                LEAVE loop_cur_1;
            END IF;
                


            # Do not re-grade tests with external_grading_flag set; only re-score
            if v_external_grading_flag = 0 then
                # Normal grading update for rescore
                UPDATE  sam_student_response as er
                JOIN    sam_answer_key as ak
                        ON    er.test_id          = ak.test_id
                        AND   er.test_question_id = ak.test_question_id
                        AND   ak.test_id          = v_test_id
                JOIN    sam_question_type as qt
                        ON    ak.question_type_id = qt.question_type_id
                JOIN    sam_scoring_method as sm
                        ON    qt.scoring_method_id   = sm.scoring_method_id
                        AND   sm.scoring_method_code in ('c','s')
                SET     er.rubric_score = 
                            case WHEN sm.scoring_method_code in ('s') and er.student_answer = ak.answer then 1
                                 WHEN sm.scoring_method_code in ('s') then 0
                                 WHEN sm.scoring_method_code = 'c' and er.rubric_score is null  and er.student_answer REGEXP '^[0-9]' != 0 then cast(er.student_answer as signed)
                                 WHEN sm.scoring_method_code = 'c' and er.rubric_score is null  and er.student_answer REGEXP '^[0-9]' = 0 then 0
                            ELSE    er.rubric_score
                            END
                            
                WHERE   er.test_id = v_test_id 
#                AND     er.gui_edit_flag = 0
                ;

                # Temporary grading update for Online Scoring of SR question types
                # This should be removed once the App code is updated to set the "rubric_score"
                # column on Online Scoring Save event for SR types.
                UPDATE  sam_student_response as er
                JOIN    sam_answer_key as ak
                        ON    er.test_id          = ak.test_id
                        AND   er.test_question_id = ak.test_question_id
                        AND   ak.test_id          = v_test_id
                JOIN    sam_question_type as qt
                        ON    ak.question_type_id = qt.question_type_id
                JOIN    sam_scoring_method as sm
                        ON    qt.scoring_method_id   = sm.scoring_method_id
                        AND   sm.scoring_method_code = 's'
#                        AND   sm.scoring_method_code in ('s','g')
                SET     er.rubric_score = 
                            case WHEN er.student_answer = ak.answer then 1
                                ELSE    0
                            END
                            
                WHERE   er.test_id = v_test_id 
                AND     er.gui_edit_flag = 1
                ;

                # Gridded responses
                if v_gr_type_cnt > 0 then

                    truncate table tmp_ak_grid;
                    truncate table tmp_stu_grid_response_calc;

                    insert tmp_ak_grid (
                        test_id
                        ,test_question_id
                        ,correct_answer
                        ,round_digits
                        ,last_user_id
                        ,create_timestamp
                    )
                    
                    select  ak.test_id
                        ,ak.test_question_id
                        ,ak.answer
                        ,case when instr(ak.answer, '.') != 0 then char_length(substring_index(ak.answer, '.', -1)) else 0 end
                        ,1234
                        ,now()
                    from    tmp_rescore_tests as t 
                    join    sam_answer_key as ak 
                            on ak.test_id = t.test_id 
                    join    sam_question_type as qt 
                            on ak.question_type_id = qt.question_type_id
                    join    sam_scoring_method as sm 
                            on qt.scoring_method_id = sm.scoring_method_id
                            and sm.scoring_method_code = 'g'
                    where   t.test_id = v_test_id
                    ;
        
                    insert tmp_stu_grid_response_calc (
                        test_id
                        ,test_event_id
                        ,student_id
                        ,test_question_id
                        ,student_answer_raw
                        ,numerator_value
                        ,denominator_value
                        ,last_user_id
                        ,create_timestamp
                    )
                    
                    SELECT  sr.test_id
                            ,sr.test_event_id
                            ,sr.student_id
                            ,sr.test_question_id
                            ,sr.student_answer_raw_gr
                            ,case   # ignore responses with multiple division operators or decimal points
                                    when substring_index(sr.student_answer_raw_gr, '/', 2) != sr.student_answer_raw_gr then null
                                    when substring_index(sr.student_answer_raw_gr, '.', 2) != sr.student_answer_raw_gr then null
                                    # ignore responses with leading division operator
                                    when sr.student_answer_raw_gr regexp '^/' then null
                                    # ignore responses ending with division operator
                                    when sr.student_answer_raw_gr regexp '/$' then null
                                    # ignore responses beginning with decimal followed by division operator
                                    when sr.student_answer_raw_gr regexp '^\\./' then null
                                    # ignore responses containing both decimal point and division operator
                                    when sr.student_answer_raw_gr regexp '\\.+.*/+' then null
                                    when sr.student_answer_raw_gr regexp '/+.*\\.+' then null
                                    # extract and split responses requiring calculation by division
                                    when instr(sr.student_answer_raw_gr, '/') != 0 and sr.student_answer_raw_gr regexp '[^0-9./]+' = 0 then substring_index(sr.student_answer_raw_gr, '/', 1) 
                                    else null 
                            end as numerator_value
                            ,case   # ignore responses with multiple division operators or decimal points
                                    when substring_index(sr.student_answer_raw_gr, '/', 2) != sr.student_answer_raw_gr then null
                                    when substring_index(sr.student_answer_raw_gr, '.', 2) != sr.student_answer_raw_gr then null
                                    # ignore responses with leading division operator
                                    when sr.student_answer_raw_gr regexp '^/' then null
                                    # ignore responses ending with division operator
                                    when sr.student_answer_raw_gr regexp '/$' then null
                                    # ignore responses beginning with decimal followed by division operator
                                    when sr.student_answer_raw_gr regexp '^\\./' then null
                                    # ignore responses containing both decimal point and division operator
                                    when sr.student_answer_raw_gr regexp '\\.+.*/+' then null
                                    when sr.student_answer_raw_gr regexp '/+.*\\.+' then null
                                    # extract and split responses requiring calculation by division
                                    when instr(sr.student_answer_raw_gr, '/') != 0 and sr.student_answer_raw_gr regexp '[^0-9./]+' = 0 then substring_index(sr.student_answer_raw_gr, '/', -1) 
                                    else null 
                            end as denominator_value
                             ,1234
                             ,now()
                    FROM    tmp_ak_grid AS tak
                    JOIN    sam_student_response AS sr     
                            ON      sr.test_id = tak.test_id    
                            AND     sr.test_question_id = tak.test_question_id
                    ON DUPLICATE KEY UPDATE student_answer_raw = values(student_answer_raw)
                        ,numerator_value = values(numerator_value)
                        ,denominator_value = values(denominator_value)
                        ,last_user_id = values(last_user_id)
                    ;
    
                    update  tmp_stu_grid_response_calc as upd
                    join    tmp_ak_grid as akg
                            on      upd.test_id = akg.test_id
                            and     upd.test_question_id = akg.test_question_id
                    set     upd.student_answer_numeric_char = 
                            case when upd.numerator_value is not null and upd.denominator_value != 0 then round(upd.numerator_value/upd.denominator_value, akg.round_digits) 
                                    when upd.student_answer_raw regexp '[^0-9.]+' = 0 and upd.student_answer_raw != '.' and upd.student_answer_raw regexp '\\.+.*\\.+' = 0 then upd.student_answer_raw
                                    else null
                            end
                    ;
    
    
                    update  sam_student_response as sr
                    join    tmp_stu_grid_response_calc as tmp1
                            on      sr.test_id = tmp1.test_id
                            and     sr.test_event_id = tmp1.test_event_id
                            and     sr.student_id = tmp1.student_id
                            and     sr.test_question_id = tmp1.test_question_id
                    join    tmp_ak_grid as ak
                            on      tmp1.test_id = ak.test_id
                            and     tmp1.test_question_id = ak.test_question_id
    
                    set     student_answer = coalesce(tmp1.student_answer_numeric_char, tmp1.student_answer_raw)
                            ,rubric_score = case when cast(tmp1.student_answer_numeric_char as decimal(18,9)) = cast(ak.correct_answer as decimal(18,9)) then 1 else 0 end
                    ;
    
                end if;


            end if;

            # Re-score all tests with force_rescore_flag set
            # regardless of external_grading_flag value
            # This accounts for the case of changing a rubric
            # on the Answer Key of externally graded test.
            UPDATE sam_student_response as er
            JOIN    sam_answer_key as ak
                    ON     er.test_id          = ak.test_id
                    AND    er.test_question_id = ak.test_question_id
                    AND    ak.test_id          = v_test_id
            LEFT JOIN sam_rubric_list as rl
                    ON     ak.rubric_id    = rl.rubric_id
                    AND    er.rubric_score = rl.rubric_score
            SET     er.rubric_value = 
                        CASE when rl.rubric_item_id is null then 0 
                        ELSE rl.rubric_value 
                        END
                        
            WHERE   er.test_id = v_test_id;
    
            update  sam_test as t
            set     t.force_rescore_flag = 0
            where   t.test_id = v_test_id
            ;
    
        END LOOP loop_cur_1;

    end if;

    drop table if exists `tmp_rescore_tests`;
    drop table if exists `tmp_ak_grid`;
    drop table if exists `tmp_stu_grid_response_calc`;

END;
//
