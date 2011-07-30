/*
$Rev: 7946 $ 
$Author: randall.stanley $ 
$Date: 2009-12-07 12:45:29 -0500 (Mon, 07 Dec 2009) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_pm_natl_test_scores_itbs.sql $
$Id: etl_pm_natl_test_scores_itbs.sql 7946 2009-12-07 17:45:29Z randall.stanley $ 
 */

####################################################################
# Insert ITBS data into rpt tables for natl_tests.
# 
####################################################################

DROP PROCEDURE IF EXISTS etl_pm_natl_test_scores_itbs//

CREATE definer=`dbadmin`@`localhost` procedure etl_pm_natl_test_scores_itbs()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 7946 $ $Date: 2009-12-07 12:45:29 -0500 (Mon, 07 Dec 2009) $'

PROC: BEGIN 

    declare v_date_format_mask varchar(15) default '%m%d%Y';

    ##############################################################
    # ITBS inserts:  3 identical insert statements except
    #           student ID joins based on:
    #           -  st.fid_code
    #           -  st.student_state_code
    #           -  st.student_code
    ##############################################################

    ##############################################################
    # Insert ITBS scores - based on st.fid_code
    ##############################################################

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
    
    select  count(*) 
    into    @view_exists
    from    information_schema.tables t
    where   t.table_schema = @db_name_core
    and     t.table_name = 'v_pmi_ods_itbs';

    if @view_exists > 0 then

        set @itbsTestDateFormatMask := pmi_f_get_etl_setting('itbsTestDateFormatMask');
        
        if @itbsTestDateFormatMask is not null then
            set v_date_format_mask = @itbsTestDateFormatMask;
        end if;

        INSERT INTO pm_natl_test_scores (
            test_type_id
            ,subject_id
            ,student_id
            ,test_year
            ,test_month
            ,score
            ,npr_score
            ,ge_score
            ,last_user_id
            ,create_timestamp
            )
        SELECT 
                dt.test_type_id
                ,dt.subject_id
                ,dt.student_id
                ,dt.test_year
                ,dt.test_month
                ,dt.score
                ,dt.npr_score
                ,(CASE WHEN dt.ge_score like 'k%'
                        THEN REPLACE(dt.ge_score, 'K', '0')
                                            WHEN dt.ge_score like 'p%'
                        THEN REPLACE(dt.ge_score, 'P', '-1')
                        ELSE dt.ge_score END) AS ge_score
                ,1234
                ,now()
            FROM (
                SELECT 
                    tty.test_type_id
                    ,tsub.subject_id
                    ,st.student_id
                    ,year(str_to_date(odsv.test_date, v_date_format_mask)) AS test_year
                    ,month(str_to_date(odsv.test_date, v_date_format_mask)) AS test_month
                    ,(CASE
                            WHEN tsub.moniker like 'Vocabulary'
                                THEN odsv.vocabulary_ns
                            WHEN tsub.moniker like 'Comprehension'
                                THEN odsv.reading_comprehension_ns
                            WHEN tsub.moniker like 'Reading Total'
                                THEN odsv.reading_total_ns
                            WHEN tsub.moniker like 'Word Analysis'
                                THEN odsv.word_analysis_ns
                            WHEN tsub.moniker like 'Listening'
                                THEN odsv.listening_ns
                            WHEN tsub.moniker like 'Spelling'
                                THEN odsv.spelling_ns
                            WHEN tsub.moniker like 'Capitalization'
                                THEN odsv.capitalization_ns
                            WHEN tsub.moniker like 'Punctuation'
                                THEN odsv.reading_word_ns
                            WHEN tsub.moniker like 'Usage and Expression'
                                THEN odsv.usage_ns
                            WHEN tsub.moniker like 'Language Total'
                                THEN odsv.language_ns
                            WHEN tsub.moniker like 'Concepts and Estimation'
                                THEN odsv.concepts_estimation_ns
                            WHEN tsub.moniker like 'Problem Solving and Data Interpretation'
                                THEN odsv.data_interpretation_ns            
                            WHEN tsub.moniker like 'Math Total'
                                THEN odsv.math_total_ns
                            WHEN tsub.moniker like 'Computation'
                                THEN odsv.Computation_ns
                            WHEN tsub.moniker like 'Core Total'
                                THEN odsv.core_total_ns
                            WHEN tsub.moniker like 'Social Studies'
                                THEN odsv.social_studies_ns
                            WHEN tsub.moniker like 'Science'
                                THEN odsv.science_ns
                            WHEN tsub.moniker like 'Maps and Diagrams'
                                THEN odsv.maps_diagrams_ns
                            WHEN tsub.moniker like 'Reference Materials'
                                THEN odsv.reference_materials_ns
                            WHEN tsub.moniker like 'Sources of Information Total'
                                THEN odsv.source_total_ns   
                            ELSE NULL
                            END) AS score
                    ,(CASE
                            WHEN tsub.moniker like 'Vocabulary'
                                THEN odsv.vocabulary_npr
                            WHEN tsub.moniker like 'Comprehension'
                                THEN odsv.reading_comprehension_npr
                            WHEN tsub.moniker like 'Reading Total'
                                THEN odsv.reading_total_npr
                            WHEN tsub.moniker like 'Word Analysis'
                                THEN odsv.word_analysis_npr
                            WHEN tsub.moniker like 'Listening'
                                THEN odsv.listening_npr
                            WHEN tsub.moniker like 'Spelling'
                                THEN odsv.spelling_npr
                            WHEN tsub.moniker like 'Capitalization'
                                THEN odsv.capitalization_npr
                            WHEN tsub.moniker like 'Punctuation'
                                THEN odsv.reading_word_npr
                            WHEN tsub.moniker like 'Usage and Expression'
                                THEN odsv.usage_npr
                            WHEN tsub.moniker like 'Language Total'
                                THEN odsv.language_npr
                            WHEN tsub.moniker like 'Concepts and Estimation'
                                THEN odsv.concepts_estimation_npr
                            WHEN tsub.moniker like 'Problem Solving and Data Interpretation'
                                THEN odsv.data_interpretation_npr           
                            WHEN tsub.moniker like 'Math Total'
                                THEN odsv.math_total_npr
                            WHEN tsub.moniker like 'Computation'
                                THEN odsv.Computation_npr
                            WHEN tsub.moniker like 'Core Total'
                                THEN odsv.core_total_npr
                            WHEN tsub.moniker like 'Social Studies'
                                THEN odsv.social_studies_npr
                            WHEN tsub.moniker like 'Science'
                                THEN odsv.science_npr
                            WHEN tsub.moniker like 'Maps and Diagrams'
                                THEN odsv.maps_diagrams_npr
                            WHEN tsub.moniker like 'Reference Materials'
                                THEN odsv.reference_materials_npr
                            WHEN tsub.moniker like 'Sources of Information Total'
                                THEN odsv.source_total_npr  
                            ELSE NULL
                            END) AS npr_score
                    ,(CASE
                            WHEN tsub.moniker like 'Vocabulary'
                                THEN odsv.vocabulary_ge
                            WHEN tsub.moniker like 'Comprehension'
                                THEN odsv.reading_comprehension_ge
                            WHEN tsub.moniker like 'Reading Total'
                                THEN odsv.reading_total_ge
                            WHEN tsub.moniker like 'Word Analysis'
                                THEN odsv.word_analysis_ge
                            WHEN tsub.moniker like 'Listening'
                                THEN odsv.listening_ge
                            WHEN tsub.moniker like 'Spelling'
                                THEN odsv.spelling_ge
                            WHEN tsub.moniker like 'Capitalization'
                                THEN odsv.capitalization_ge
                            WHEN tsub.moniker like 'Punctuation'
                                THEN odsv.reading_word_ge
                            WHEN tsub.moniker like 'Usage and Expression'
                                THEN odsv.usage_ge
                            WHEN tsub.moniker like 'Language Total'
                                THEN odsv.language_ge
                            WHEN tsub.moniker like 'Concepts and Estimation'
                                THEN odsv.concepts_estimation_ge
                            WHEN tsub.moniker like 'Problem Solving and Data Interpretation'
                                THEN odsv.data_interpretation_ge            
                            WHEN tsub.moniker like 'Math Total'
                                THEN odsv.math_total_ge
                            WHEN tsub.moniker like 'Computation'
                                THEN odsv.Computation_ge
                            WHEN tsub.moniker like 'Core Total'
                                THEN odsv.core_total_ge
                            WHEN tsub.moniker like 'Social Studies'
                                THEN odsv.social_studies_ge
                            WHEN tsub.moniker like 'Science'
                                THEN odsv.science_ge
                            WHEN tsub.moniker like 'Maps and Diagrams'
                                THEN odsv.maps_diagrams_ge
                            WHEN tsub.moniker like 'Reference Materials'
                                THEN odsv.reference_materials_ge
                            WHEN tsub.moniker like 'Sources of Information Total'
                                THEN odsv.source_total_ge   
                            ELSE NULL
                            END) AS ge_score            
                FROM v_pmi_ods_itbs AS odsv
                JOIN pm_natl_test_type AS tty
                    ON  tty.moniker regexp odsv.test
                JOIN  c_student AS st
                    ON odsv.student_id = st.fid_code
                JOIN c_school_year sy
                  ON STR_TO_DATE(odsv.test_date, v_date_format_mask) BETWEEN sy.begin_date AND sy.end_date 
                JOIN  c_student_year AS sty
                    ON st.student_id = sty.student_id
                    AND sty.school_year_id = sy.school_year_id
                JOIN pm_natl_test_subject AS tsub
                    ON  tty.test_type_id = tsub.test_type_id
                ) as dt
            WHERE dt.score IS NOT NULL
        ON DUPLICATE key UPDATE last_user_id = 1234
            ,score = values(score)
            ,npr_score = values(npr_score)
            ,ge_score = values(ge_score)
        ;
    
    
        ##############################################################
        # Insert ITBS scores - based on st.student_state_code
        ##############################################################
    
        INSERT INTO pm_natl_test_scores (
            test_type_id
            ,subject_id
            ,student_id
            ,test_year
            ,test_month
            ,score
            ,npr_score
            ,ge_score
            ,last_user_id
            ,create_timestamp
            )
        SELECT 
                dt.test_type_id
                ,dt.subject_id
                ,dt.student_id
                ,dt.test_year
                ,dt.test_month
                ,dt.score
                ,dt.npr_score
                ,(CASE WHEN dt.ge_score like 'k%'
                        THEN REPLACE(dt.ge_score, 'K', '0')
                                            WHEN dt.ge_score like 'p%'
                        THEN REPLACE(dt.ge_score, 'P', '-1')
                        ELSE dt.ge_score END) AS ge_score
                ,1234
                ,now()
            FROM (
                SELECT 
                    tty.test_type_id
                    ,tsub.subject_id
                    ,st.student_id
                    ,year(str_to_date(odsv.test_date, v_date_format_mask)) AS test_year
                    ,month(str_to_date(odsv.test_date, v_date_format_mask)) AS test_month
                    ,(CASE
                            WHEN tsub.moniker like 'Vocabulary'
                                THEN odsv.vocabulary_ns
                            WHEN tsub.moniker like 'Comprehension'
                                THEN odsv.reading_comprehension_ns
                            WHEN tsub.moniker like 'Reading Total'
                                THEN odsv.reading_total_ns
                            WHEN tsub.moniker like 'Word Analysis'
                                THEN odsv.word_analysis_ns
                            WHEN tsub.moniker like 'Listening'
                                THEN odsv.listening_ns
                            WHEN tsub.moniker like 'Spelling'
                                THEN odsv.spelling_ns
                            WHEN tsub.moniker like 'Capitalization'
                                THEN odsv.capitalization_ns
                            WHEN tsub.moniker like 'Punctuation'
                                THEN odsv.reading_word_ns
                            WHEN tsub.moniker like 'Usage and Expression'
                                THEN odsv.usage_ns
                            WHEN tsub.moniker like 'Language Total'
                                THEN odsv.language_ns
                            WHEN tsub.moniker like 'Concepts and Estimation'
                                THEN odsv.concepts_estimation_ns
                            WHEN tsub.moniker like 'Problem Solving and Data Interpretation'
                                THEN odsv.data_interpretation_ns            
                            WHEN tsub.moniker like 'Math Total'
                                THEN odsv.math_total_ns
                            WHEN tsub.moniker like 'Computation'
                                THEN odsv.Computation_ns
                            WHEN tsub.moniker like 'Core Total'
                                THEN odsv.core_total_ns
                            WHEN tsub.moniker like 'Social Studies'
                                THEN odsv.social_studies_ns
                            WHEN tsub.moniker like 'Science'
                                THEN odsv.science_ns
                            WHEN tsub.moniker like 'Maps and Diagrams'
                                THEN odsv.maps_diagrams_ns
                            WHEN tsub.moniker like 'Reference Materials'
                                THEN odsv.reference_materials_ns
                            WHEN tsub.moniker like 'Sources of Information Total'
                                THEN odsv.source_total_ns   
                            ELSE NULL
                            END) AS score
                    ,(CASE
                            WHEN tsub.moniker like 'Vocabulary'
                                THEN odsv.vocabulary_npr
                            WHEN tsub.moniker like 'Comprehension'
                                THEN odsv.reading_comprehension_npr
                            WHEN tsub.moniker like 'Reading Total'
                                THEN odsv.reading_total_npr
                            WHEN tsub.moniker like 'Word Analysis'
                                THEN odsv.word_analysis_npr
                            WHEN tsub.moniker like 'Listening'
                                THEN odsv.listening_npr
                            WHEN tsub.moniker like 'Spelling'
                                THEN odsv.spelling_npr
                            WHEN tsub.moniker like 'Capitalization'
                                THEN odsv.capitalization_npr
                            WHEN tsub.moniker like 'Punctuation'
                                THEN odsv.reading_word_npr
                            WHEN tsub.moniker like 'Usage and Expression'
                                THEN odsv.usage_npr
                            WHEN tsub.moniker like 'Language Total'
                                THEN odsv.language_npr
                            WHEN tsub.moniker like 'Concepts and Estimation'
                                THEN odsv.concepts_estimation_npr
                            WHEN tsub.moniker like 'Problem Solving and Data Interpretation'
                                THEN odsv.data_interpretation_npr           
                            WHEN tsub.moniker like 'Math Total'
                                THEN odsv.math_total_npr
                            WHEN tsub.moniker like 'Computation'
                                THEN odsv.Computation_npr
                            WHEN tsub.moniker like 'Core Total'
                                THEN odsv.core_total_npr
                            WHEN tsub.moniker like 'Social Studies'
                                THEN odsv.social_studies_npr
                            WHEN tsub.moniker like 'Science'
                                THEN odsv.science_npr
                            WHEN tsub.moniker like 'Maps and Diagrams'
                                THEN odsv.maps_diagrams_npr
                            WHEN tsub.moniker like 'Reference Materials'
                                THEN odsv.reference_materials_npr
                            WHEN tsub.moniker like 'Sources of Information Total'
                                THEN odsv.source_total_npr  
                            ELSE NULL
                            END) AS npr_score
                    ,(CASE
                            WHEN tsub.moniker like 'Vocabulary'
                                THEN odsv.vocabulary_ge
                            WHEN tsub.moniker like 'Comprehension'
                                THEN odsv.reading_comprehension_ge
                            WHEN tsub.moniker like 'Reading Total'
                                THEN odsv.reading_total_ge
                            WHEN tsub.moniker like 'Word Analysis'
                                THEN odsv.word_analysis_ge
                            WHEN tsub.moniker like 'Listening'
                                THEN odsv.listening_ge
                            WHEN tsub.moniker like 'Spelling'
                                THEN odsv.spelling_ge
                            WHEN tsub.moniker like 'Capitalization'
                                THEN odsv.capitalization_ge
                            WHEN tsub.moniker like 'Punctuation'
                                THEN odsv.reading_word_ge
                            WHEN tsub.moniker like 'Usage and Expression'
                                THEN odsv.usage_ge
                            WHEN tsub.moniker like 'Language Total'
                                THEN odsv.language_ge
                            WHEN tsub.moniker like 'Concepts and Estimation'
                                THEN odsv.concepts_estimation_ge
                            WHEN tsub.moniker like 'Problem Solving and Data Interpretation'
                                THEN odsv.data_interpretation_ge            
                            WHEN tsub.moniker like 'Math Total'
                                THEN odsv.math_total_ge
                            WHEN tsub.moniker like 'Computation'
                                THEN odsv.Computation_ge
                            WHEN tsub.moniker like 'Core Total'
                                THEN odsv.core_total_ge
                            WHEN tsub.moniker like 'Social Studies'
                                THEN odsv.social_studies_ge
                            WHEN tsub.moniker like 'Science'
                                THEN odsv.science_ge
                            WHEN tsub.moniker like 'Maps and Diagrams'
                                THEN odsv.maps_diagrams_ge
                            WHEN tsub.moniker like 'Reference Materials'
                                THEN odsv.reference_materials_ge
                            WHEN tsub.moniker like 'Sources of Information Total'
                                THEN odsv.source_total_ge   
                            ELSE NULL
                            END) AS ge_score            
                FROM v_pmi_ods_itbs AS odsv
                JOIN pm_natl_test_type AS tty
                    ON  tty.moniker regexp odsv.test
                JOIN  c_student AS st
                    ON odsv.student_id = st.student_state_code
                JOIN c_school_year sy
                  ON STR_TO_DATE(odsv.test_date, v_date_format_mask) BETWEEN sy.begin_date AND sy.end_date 
                JOIN  c_student_year AS sty
                    ON st.student_id = sty.student_id
                    AND sty.school_year_id = sy.school_year_id
                JOIN pm_natl_test_subject AS tsub
                    ON  tty.test_type_id = tsub.test_type_id
                ) as dt
            WHERE dt.score IS NOT NULL
        ON DUPLICATE key UPDATE last_user_id = 1234
            ,score = values(score)
            ,npr_score = values(npr_score)
            ,ge_score = values(ge_score)
        ;
    
        ##############################################################
        # Insert ITBS scores - based on st.student_code
        ##############################################################
    
        INSERT INTO pm_natl_test_scores (
            test_type_id
            ,subject_id
            ,student_id
            ,test_year
            ,test_month
            ,score
            ,npr_score
            ,ge_score
            ,last_user_id
            ,create_timestamp
            )
        SELECT 
                dt.test_type_id
                ,dt.subject_id
                ,dt.student_id
                ,dt.test_year
                ,dt.test_month
                ,dt.score
                ,dt.npr_score
                ,(CASE WHEN dt.ge_score like 'k%'
                        THEN REPLACE(dt.ge_score, 'K', '0')
                                            WHEN dt.ge_score like 'p%'
                        THEN REPLACE(dt.ge_score, 'P', '-1')
                        ELSE dt.ge_score END) AS ge_score
                ,1234
                ,now()
            FROM (
                SELECT 
                    tty.test_type_id
                    ,tsub.subject_id
                    ,st.student_id
                    ,year(str_to_date(odsv.test_date, v_date_format_mask)) AS test_year
                    ,month(str_to_date(odsv.test_date, v_date_format_mask)) AS test_month
                    ,(CASE
                            WHEN tsub.moniker like 'Vocabulary'
                                THEN odsv.vocabulary_ns
                            WHEN tsub.moniker like 'Comprehension'
                                THEN odsv.reading_comprehension_ns
                            WHEN tsub.moniker like 'Reading Total'
                                THEN odsv.reading_total_ns
                            WHEN tsub.moniker like 'Word Analysis'
                                THEN odsv.word_analysis_ns
                            WHEN tsub.moniker like 'Listening'
                                THEN odsv.listening_ns
                            WHEN tsub.moniker like 'Spelling'
                                THEN odsv.spelling_ns
                            WHEN tsub.moniker like 'Capitalization'
                                THEN odsv.capitalization_ns
                            WHEN tsub.moniker like 'Punctuation'
                                THEN odsv.reading_word_ns
                            WHEN tsub.moniker like 'Usage and Expression'
                                THEN odsv.usage_ns
                            WHEN tsub.moniker like 'Language Total'
                                THEN odsv.language_ns
                            WHEN tsub.moniker like 'Concepts and Estimation'
                                THEN odsv.concepts_estimation_ns
                            WHEN tsub.moniker like 'Problem Solving and Data Interpretation'
                                THEN odsv.data_interpretation_ns            
                            WHEN tsub.moniker like 'Math Total'
                                THEN odsv.math_total_ns
                            WHEN tsub.moniker like 'Computation'
                                THEN odsv.Computation_ns
                            WHEN tsub.moniker like 'Core Total'
                                THEN odsv.core_total_ns
                            WHEN tsub.moniker like 'Social Studies'
                                THEN odsv.social_studies_ns
                            WHEN tsub.moniker like 'Science'
                                THEN odsv.science_ns
                            WHEN tsub.moniker like 'Maps and Diagrams'
                                THEN odsv.maps_diagrams_ns
                            WHEN tsub.moniker like 'Reference Materials'
                                THEN odsv.reference_materials_ns
                            WHEN tsub.moniker like 'Sources of Information Total'
                                THEN odsv.source_total_ns   
                            ELSE NULL
                            END) AS score
                    ,(CASE
                            WHEN tsub.moniker like 'Vocabulary'
                                THEN odsv.vocabulary_npr
                            WHEN tsub.moniker like 'Comprehension'
                                THEN odsv.reading_comprehension_npr
                            WHEN tsub.moniker like 'Reading Total'
                                THEN odsv.reading_total_npr
                            WHEN tsub.moniker like 'Word Analysis'
                                THEN odsv.word_analysis_npr
                            WHEN tsub.moniker like 'Listening'
                                THEN odsv.listening_npr
                            WHEN tsub.moniker like 'Spelling'
                                THEN odsv.spelling_npr
                            WHEN tsub.moniker like 'Capitalization'
                                THEN odsv.capitalization_npr
                            WHEN tsub.moniker like 'Punctuation'
                                THEN odsv.reading_word_npr
                            WHEN tsub.moniker like 'Usage and Expression'
                                THEN odsv.usage_npr
                            WHEN tsub.moniker like 'Language Total'
                                THEN odsv.language_npr
                            WHEN tsub.moniker like 'Concepts and Estimation'
                                THEN odsv.concepts_estimation_npr
                            WHEN tsub.moniker like 'Problem Solving and Data Interpretation'
                                THEN odsv.data_interpretation_npr           
                            WHEN tsub.moniker like 'Math Total'
                                THEN odsv.math_total_npr
                            WHEN tsub.moniker like 'Computation'
                                THEN odsv.Computation_npr
                            WHEN tsub.moniker like 'Core Total'
                                THEN odsv.core_total_npr
                            WHEN tsub.moniker like 'Social Studies'
                                THEN odsv.social_studies_npr
                            WHEN tsub.moniker like 'Science'
                                THEN odsv.science_npr
                            WHEN tsub.moniker like 'Maps and Diagrams'
                                THEN odsv.maps_diagrams_npr
                            WHEN tsub.moniker like 'Reference Materials'
                                THEN odsv.reference_materials_npr
                            WHEN tsub.moniker like 'Sources of Information Total'
                                THEN odsv.source_total_npr  
                            ELSE NULL
                            END) AS npr_score
                    ,(CASE
                            WHEN tsub.moniker like 'Vocabulary'
                                THEN odsv.vocabulary_ge
                            WHEN tsub.moniker like 'Comprehension'
                                THEN odsv.reading_comprehension_ge
                            WHEN tsub.moniker like 'Reading Total'
                                THEN odsv.reading_total_ge
                            WHEN tsub.moniker like 'Word Analysis'
                                THEN odsv.word_analysis_ge
                            WHEN tsub.moniker like 'Listening'
                                THEN odsv.listening_ge
                            WHEN tsub.moniker like 'Spelling'
                                THEN odsv.spelling_ge
                            WHEN tsub.moniker like 'Capitalization'
                                THEN odsv.capitalization_ge
                            WHEN tsub.moniker like 'Punctuation'
                                THEN odsv.reading_word_ge
                            WHEN tsub.moniker like 'Usage and Expression'
                                THEN odsv.usage_ge
                            WHEN tsub.moniker like 'Language Total'
                                THEN odsv.language_ge
                            WHEN tsub.moniker like 'Concepts and Estimation'
                                THEN odsv.concepts_estimation_ge
                            WHEN tsub.moniker like 'Problem Solving and Data Interpretation'
                                THEN odsv.data_interpretation_ge            
                            WHEN tsub.moniker like 'Math Total'
                                THEN odsv.math_total_ge
                            WHEN tsub.moniker like 'Computation'
                                THEN odsv.Computation_ge
                            WHEN tsub.moniker like 'Core Total'
                                THEN odsv.core_total_ge
                            WHEN tsub.moniker like 'Social Studies'
                                THEN odsv.social_studies_ge
                            WHEN tsub.moniker like 'Science'
                                THEN odsv.science_ge
                            WHEN tsub.moniker like 'Maps and Diagrams'
                                THEN odsv.maps_diagrams_ge
                            WHEN tsub.moniker like 'Reference Materials'
                                THEN odsv.reference_materials_ge
                            WHEN tsub.moniker like 'Sources of Information Total'
                                THEN odsv.source_total_ge   
                            ELSE NULL
                            END) AS ge_score            
                FROM v_pmi_ods_itbs AS odsv
                JOIN pm_natl_test_type AS tty
                    ON  tty.moniker regexp odsv.test
                JOIN  c_student AS st
                    ON odsv.student_id = st.student_code
                JOIN c_school_year sy
                  ON STR_TO_DATE(odsv.test_date, v_date_format_mask) BETWEEN sy.begin_date AND sy.end_date 
                JOIN  c_student_year AS sty
                    ON st.student_id = sty.student_id
                    AND sty.school_year_id = sy.school_year_id
                JOIN pm_natl_test_subject AS tsub
                    ON  tty.test_type_id = tsub.test_type_id
                ) as dt
            WHERE dt.score IS NOT NULL
        ON DUPLICATE key UPDATE last_user_id = 1234
            ,score = values(score)
            ,npr_score = values(npr_score)
            ,ge_score = values(ge_score)
        ;
        
        #################
        ## Update Log
        #################
        set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'pmi_ods_itbs\', \'P\', \'ETL Load Successful\')');
        prepare sql_scan_log from @sql_scan_log;
        execute sql_scan_log;
        deallocate prepare sql_scan_log;  

    end if;

END PROC;
//
