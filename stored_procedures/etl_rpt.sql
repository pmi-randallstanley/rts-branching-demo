/*
$Rev: 9893 $ 
$Author: randall.stanley $ 
$Date: 2011-01-18 09:10:39 -0500 (Tue, 18 Jan 2011) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt.sql $
$Id: etl_rpt.sql 9893 2011-01-18 14:10:39Z randall.stanley $ 
 */

DROP PROCEDURE IF EXISTS etl_rpt//

CREATE definer=`dbadmin`@`localhost` procedure `etl_rpt`()
CONTAINS SQL
COMMENT '$Rev: 9893 $ $Date: 2011-01-18 09:10:39 -0500 (Tue, 18 Jan 2011) $'
SQL SECURITY INVOKER
BEGIN

        call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);
        
        SELECT pmi_admin.pmi_f_get_next_sequence('etl_rpt_id', 1) INTO @etl_rpt_id;
    
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code)
        SELECT @etl_rpt_id, @client_id, 'etl_rpt', 'b';

        ##etl_rpt_student_group##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_student_group()', 'b';
        call etl_rpt_student_group();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_student_group()', 'c';
        
        ##etl_rpt_student_ayp_group##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_student_ayp_group()', 'b';
        call etl_rpt_student_ayp_group();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_student_ayp_group()', 'c';

        ##etl_rpt_bm_scores##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_scores()', 'b';
        call etl_rpt_bm_scores();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_scores()', 'c';
        
        ##etl_rpt_bm_scores_school##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_scores_school()', 'b';
        call etl_rpt_bm_scores_school();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_scores_school()', 'c';
        
        ##etl_rpt_bm_scores_school_grade##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_scores_school_grade()', 'b';
        call etl_rpt_bm_scores_school_grade();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_scores_school_grade()', 'c';
        
        ##etl_rpt_profile_leading_subject_stu##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_profile_leading_subject_stu()', 'b';
        call etl_rpt_profile_leading_subject_stu();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_profile_leading_subject_stu()', 'c';
        
        ##etl_rpt_profile_leading_strand_stu##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_profile_leading_strand_stu()', 'b';
        call etl_rpt_profile_leading_strand_stu();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_profile_leading_strand_stu()', 'c';
        
        ##etl_rpt_bm_assess_summ##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_assess_summ()', 'b';
        call etl_rpt_bm_assess_summ();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_assess_summ()', 'c';
        
        ##etl_rpt_ayp_teacher_performance##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_ayp_teacher_performance()', 'b';
        call etl_rpt_ayp_teacher_performance();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_ayp_teacher_performance()', 'c';
        
        ##etl_rpt_question_scores_district##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_question_scores_district()', 'b';
        call etl_rpt_question_scores_district();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_question_scores_district()', 'c';
        
        ##etl_rpt_profile_bm_stu##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_profile_bm_stu()', 'b';
        call etl_rpt_profile_bm_stu();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_profile_bm_stu()', 'c';
                
        ##etl_rpt_bm_summ_ayp_group_school_grade##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_summ_ayp_group_school_grade()', 'b';
        call etl_rpt_bm_summ_ayp_group_school_grade();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_summ_ayp_group_school_grade()', 'c';
        
        ##etl_rpt_test_scores##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_test_scores()', 'b';
        call etl_rpt_test_scores();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_test_scores()', 'c';
        
        ##etl_rpt_bm_scores_district##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_scores_district()', 'b';
        call etl_rpt_bm_scores_district();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_scores_district()', 'c';
        
        ##etl_rpt_bm_scores_district_grade##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_scores_district_grade()', 'b';
        call etl_rpt_bm_scores_district_grade();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_scores_district_grade()', 'c';
        
        ##etl_rpt_bm_scores_class##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_scores_class()', 'b';
        call etl_rpt_bm_scores_class();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_scores_class()', 'c';
        
        ##etl_rpt_bm_scores_strand_class##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_scores_strand_class()', 'b';
        call etl_rpt_bm_scores_strand_class();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_scores_strand_class()', 'c';
        
        ##etl_rpt_test_curriculum_scores##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_test_curriculum_scores()', 'b';
        call etl_rpt_test_curriculum_scores();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_test_curriculum_scores()', 'c';
                
        ##etl_rpt_ayp_accel_summ_tables##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_ayp_accel_summ_tables()', 'b';
        call etl_rpt_ayp_accel_summ_tables();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_ayp_accel_summ_tables()', 'c';

        ##etl_rpt_matrix##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_matrix(0)', 'b';
        call etl_rpt_matrix(0);
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_matrix(0)', 'c';
        
        ##etl_rpt_bm_summ_ayp_stu_period##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_summ_ayp_stu_period()', 'b';
        call etl_rpt_bm_summ_ayp_stu_period();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bm_summ_ayp_stu_period()', 'c';

        ##etl_rpt_isel_scores##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_isel_scores()', 'b';
        call etl_rpt_isel_scores();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_isel_scores()', 'c';

        ##etl_rpt_baseball_rebuild##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_baseball_rebuild()', 'b';
        call etl_rpt_baseball_rebuild();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_baseball_rebuild()', 'c';

        ## New BBCard rebuild ##
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bbcard_rebuild()', 'b';
        call etl_rpt_bbcard_rebuild();
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code) SELECT @etl_rpt_id, @client_id, 'etl_rpt_bbcard_rebuild()', 'c';
        
        INSERT tmp.etl_rpt_log (etl_rpt_id, client_id, action, time_code)
        SELECT @etl_rpt_id, @client_id, 'etl_rpt', 'c';

END;
//
