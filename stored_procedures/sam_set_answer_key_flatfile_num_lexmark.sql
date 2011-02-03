/*
$Rev: 9335 $ 
$Author: randall.stanley $ 
$Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/sam_set_answer_key_flatfile_num_lexmark.sql $
$Id: sam_set_answer_key_flatfile_num_lexmark.sql 9335 2010-10-03 18:10:23Z randall.stanley $ 
 */

drop procedure if exists sam_set_answer_key_flatfile_num_lexmark//

create definer=`dbadmin`@`localhost` procedure sam_set_answer_key_flatfile_num_lexmark(v_test_id int)
contains sql
sql security invoker
comment '$Rev: 9335 $ $Date: 2010-10-03 14:10:23 -0400 (Sun, 03 Oct 2010) $'

proc: begin 

    declare v_cr_mapped_end_flag tinyint;

    set @row_num := 0;
      
    select  lf.sr_total_number
        ,lf.cr_mapped_end_flag
    into    @row_num
            ,v_cr_mapped_end_flag
    from    sam_test as t
    join    sam_lexmark_form as lf
            on      t.lexmark_form_id = lf.lexmark_form_id
            and     t.test_id = v_test_id
    ;

    # if the form has CR question types identified as being mapped at the end
    # of the form (after the SR types), then update the flatfile_question_num
    # value. Otherwise, reset natural numbering. This change was to address
    # toggling of v_cr_mapped_end_flag value by customer.
    if v_cr_mapped_end_flag = 1 then

        UPDATE  sam_answer_key AS ak2
        JOIN    (
                  SELECT  ak.test_id
                      ,ak.test_question_id
                      ,ak.section_num
                      ,ak.section_question_num
                      ,(@row_num := @row_num + 1) AS flatfile_question_num
                      
                  FROM    sam_answer_key AS ak
                  join    sam_question_type as qt
                          on      ak.question_type_id = qt.question_type_id
                  join    sam_scoring_method as sm
                          on      qt.scoring_method_id = sm.scoring_method_id
                          and     sm.scoring_method_code = 'c'
                  WHERE   ak.test_id = v_test_id
                  ORDER BY ak.section_num, ak.section_question_num
              ) AS dt
              ON  dt.test_id = ak2.test_id
              AND dt.test_question_id = ak2.test_question_id
        SET     ak2.flatfile_question_num = dt.flatfile_question_num ;

    else

        SET @row_num := 0;

        UPDATE  sam_answer_key AS ak2
        JOIN    (
                    SELECT ak.test_id
                        ,ak.test_question_id
                        ,ak.section_num
                        ,ak.section_question_num
                        ,(@row_num := @row_num + 1) AS row_num
                    FROM sam_answer_key AS ak
                    WHERE ak.test_id = v_test_id
                    ORDER BY ak.section_num, ak.section_question_num
                ) AS dt
        ON      dt.test_id = ak2.test_id
        AND     dt.test_question_id = ak2.test_question_id
        SET     ak2.flatfile_question_num = dt.row_num ;

    end if;

end proc;
//
