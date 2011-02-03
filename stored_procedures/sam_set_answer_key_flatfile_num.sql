/*
$Rev: 6928 $ 
$Author: randall.stanley $ 
$Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/sam_set_answer_key_flatfile_num.sql $
$Id: sam_set_answer_key_flatfile_num.sql 6928 2009-04-10 14:08:29Z randall.stanley $ 
*/

DROP PROCEDURE IF EXISTS sam_set_answer_key_flatfile_num //

CREATE definer=`dbadmin`@`localhost` procedure sam_set_answer_key_flatfile_num ()

CONTAINS SQL
COMMENT '$Rev: 6928 $ $Date: 2009-04-10 10:08:29 -0400 (Fri, 10 Apr 2009) $'
SQL SECURITY INVOKER


PROC: BEGIN 
   

   DECLARE v_test_id int;
   DECLARE no_more_rows BOOLEAN;

      
   DECLARE cur_test CURSOR FOR
        SELECT  test_id
        FROM    sam_test
        WHERE   purge_flag = 0;

   DECLARE CONTINUE HANDLER FOR NOT FOUND 
      SET no_more_rows = TRUE;

    SET @bmDoesNotSendCRScanResults := pmi_f_get_etl_setting('bmDoesNotSendCRScanResults');
    
    # work-around for MySQL trigger bug
    drop table if exists tmp_test_list;
    create table tmp_test_list
    select  test_id
    from    sam_test
    where   purge_flag = 0
    ;
    
    update  sam_answer_key as ak
    join    tmp_test_list as t
            on      ak.test_id = t.test_id
    set     ak.flatfile_question_num = null;

   OPEN cur_test;

   loop_cur_test: LOOP
      FETCH   cur_test 
      INTO     v_test_id;
   
      IF no_more_rows THEN
            CLOSE cur_test;
            LEAVE loop_cur_test;
      END IF;

        SET @row_num := 0;

        if @bmDoesNotSendCRScanResults = 'y' then 
            UPDATE  sam_answer_key AS ak2
            JOIN    (
                        SELECT  ak.test_id
                            ,ak.test_question_id
                            ,ak.section_num
                            ,ak.section_question_num
                            ,(@row_num := @row_num + 1) AS row_num
                            
                        FROM    sam_answer_key AS ak
                        join    sam_question_type as qt
                                on      ak.question_type_id = qt.question_type_id
                        join    sam_scoring_method as sm
                                on      qt.scoring_method_id = sm.scoring_method_id
                                and     sm.scoring_method_code != 'c'
                        WHERE   ak.test_id = v_test_id
                        ORDER BY ak.section_num, ak.section_question_num
                    ) AS dt
                    ON  dt.test_id = ak2.test_id
                    AND dt.test_question_id = ak2.test_question_id
            SET     ak2.flatfile_question_num = dt.row_num ;
        else
            UPDATE  sam_answer_key AS ak2
            JOIN    (
                        SELECT  ak.test_id
                            ,ak.test_question_id
                            ,ak.section_num
                            ,ak.section_question_num
                            ,(@row_num := @row_num + 1) AS row_num
                            
                        FROM    sam_answer_key AS ak
                        WHERE   ak.test_id = v_test_id
                        ORDER BY ak.section_num, ak.section_question_num
                    ) AS dt
                    ON  dt.test_id = ak2.test_id
                    AND dt.test_question_id = ak2.test_question_id
            SET     ak2.flatfile_question_num = dt.row_num ;
        
        end if;
   END LOOP loop_cur_test;

    drop table if exists tmp_test_list;

END PROC;
//
