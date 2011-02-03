DROP PROCEDURE IF EXISTS etl_uploader_core_qa //

CREATE definer=`dbadmin`@`localhost` procedure `etl_uploader_core_qa`()
SQL SECURITY INVOKER
COMMENT '$Rev: 6928 $ $Date: 2007-08-28 15:05:22 -0400 (Tue, 28 Aug 2007)'
BEGIN
select concat(database(), '_ods') into @db;
               
SET @sql_school := '';
SET @sql_school := CONCAT(@sql_school, 'select max(ul.upload_id) INTO @pmi_ods_school_id from ', @db, '.imp_upload_log ul ');
SET @sql_school := CONCAT(@sql_school, 'join pmi_admin.imp_table t on ul.table_id = t.table_id and ul.upload_status_code = \'', 'c', '\'');
SET @sql_school := CONCAT(@sql_school, 'where t.target_table_name = \'', 'pmi_ods_school', '\'');
prepare sql_school from @sql_school;
            
  execute sql_school;
            
  deallocate prepare sql_school;
IF  @pmi_ods_school_id > 1
    THEN 
                  SET @check_1 := '';
                    SET @sql_school_check_1 := '';
                    SET @sql_school_check_1 := CONCAT(@sql_school_check_1, 'select count(*) into @check_1 from ', @db, '.pmi_ods_school where school_id is null');
                    prepare sql_school_check_1 from @sql_school_check_1;
            
       execute sql_school_check_1;
            
       deallocate prepare sql_school_check_1;
                        IF @check_1 >= 1
                            THEN    
                                SET @sql_check_1_log := '';
                                SET @sql_check_1_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_school', '\',', '\'', 'F', '\',', '\'', 'School_id is null', '\')');
                                
                                prepare sql_check_1_log from @sql_check_1_log;
            
                                    execute sql_check_1_log;
                                                
                                    deallocate prepare sql_check_1_log;
                        END IF;
                    
                    
END IF;
        
SET @sql_teacher := '';
SET @sql_teacher := CONCAT(@sql_teacher, 'select max(ul.upload_id) INTO @pmi_ods_teacher_id from ', @db, '.imp_upload_log ul ');
SET @sql_teacher := CONCAT(@sql_teacher, 'join pmi_admin.imp_table t on ul.table_id = t.table_id and ul.upload_status_code = \'', 'c', '\'');
SET @sql_teacher := CONCAT(@sql_teacher, 'where t.target_table_name = \'', 'pmi_ods_teacher', '\'');
prepare sql_teacher from @sql_teacher;
            
        execute sql_teacher;
            
        deallocate prepare sql_teacher;
IF  @pmi_ods_teacher_id > 1
    THEN 
                  SET @check_1 := '';
                    SET @sql_teacher_check_1 := '';
                    SET @sql_teacher_check_1 := CONCAT(@sql_teacher_check_1, 'select count(*) into @check_1 from ', @db, '.pmi_ods_teacher where teacher_id is null');
                    prepare sql_teacher_check_1 from @sql_teacher_check_1;
            
       execute sql_teacher_check_1;
            
       deallocate prepare sql_teacher_check_1;
                        IF @check_1 >= 1
                            THEN    
                                SET @pmi_ods_teacher_id := '';
                                SET @sql_check_1_log := '';
                                SET @sql_check_1_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_teacher', '\',', '\'', 'F', '\',', '\'', 'teacher_id is null', '\')');
                                
                                prepare sql_check_1_log from @sql_check_1_log;
            
                                    execute sql_check_1_log;
                                                
                                    deallocate prepare sql_check_1_log;
                        END IF;
                    
                    
END IF;
        IF  @pmi_ods_teacher_id > 1
    THEN 
                  SET @check_2 := '';
                    SET @sql_teacher_check_2 := '';
                    SET @sql_teacher_check_2 := CONCAT(@sql_teacher_check_2, 'select count(*) into @check_2 from ', @db, '.pmi_ods_teacher where email is null');
                    prepare sql_teacher_check_2 from @sql_teacher_check_2;
            
       execute sql_teacher_check_2;
            
       deallocate prepare sql_teacher_check_2;
                        IF @check_2 >= 1
                            THEN
                                SET @pmi_ods_teacher_id := '';
                                SET @sql_check_2_log := '';
                                SET @sql_check_2_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_teacher', '\',', '\'', 'F', '\',', '\'', 'email is null', '\')');
                                
                                prepare sql_check_2_log from @sql_check_2_log;
            
                                    execute sql_check_2_log;
                                                
                                    deallocate prepare sql_check_2_log;
                        END IF;
            
            END IF;
            
        IF  @pmi_ods_teacher_id > 1
  THEN 
              SET @check_3 := '';
                SET @sql_teacher_check_3 := '';
                SET @sql_teacher_check_3 := CONCAT(@sql_teacher_check_3, 'select count(*) into @check_3 from ', @db, '.pmi_ods_teacher t where not exists ');
                SET @sql_teacher_check_3 := CONCAT(@sql_teacher_check_3, '(select * from ', @db, '.pmi_ods_school s where s.school_id = t.school_id)');
                prepare sql_teacher_check_3 from @sql_teacher_check_3;
          
     execute sql_teacher_check_3;
          
     deallocate prepare sql_teacher_check_3;
                    IF @check_3 >= 1
                        THEN
                            SET @pmi_ods_teacher_id := '';
                            SET @sql_check_3_log := '';
                            SET @sql_check_3_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_teacher', '\',', '\'', 'F', '\',', '\'', 'school_id does not tie to school file', '\')');
                            
                            prepare sql_check_3_log from @sql_check_3_log;
          
                                execute sql_check_3_log;
                                            
                                deallocate prepare sql_check_3_log;
                    END IF;
        
        END IF;
                                    
SET @sql_principle := '';
SET @sql_principle := CONCAT(@sql_principle, 'select max(ul.upload_id) INTO @pmi_ods_principle_id from ', @db, '.imp_upload_log ul ');
SET @sql_principle := CONCAT(@sql_principle, 'join pmi_admin.imp_table t on ul.table_id = t.table_id and ul.upload_status_code = \'', 'c', '\'');
SET @sql_principle := CONCAT(@sql_principle, 'where t.target_table_name = \'', 'pmi_ods_principle', '\'');
prepare sql_principle from @sql_principle;
            
        execute sql_principle;
            
        deallocate prepare sql_principle;
  
    
SET @sql_central_office := '';
SET @sql_central_office := CONCAT(@sql_central_office, 'select max(ul.upload_id) INTO @pmi_ods_central_office_id from ', @db, '.imp_upload_log ul ');
SET @sql_central_office := CONCAT(@sql_central_office, 'join pmi_admin.imp_table t on ul.table_id = t.table_id and ul.upload_status_code = \'', 'c', '\'');
SET @sql_central_office := CONCAT(@sql_central_office, 'where t.target_table_name = \'', 'pmi_ods_central_office', '\'');
prepare sql_central_office from @sql_central_office;
            
        execute sql_central_office;
            
        deallocate prepare sql_central_office;
   
    
SET @sql_student := '';
SET @sql_student := CONCAT(@sql_student, 'select max(ul.upload_id) INTO @pmi_ods_student_id from ', @db, '.imp_upload_log ul ');
SET @sql_student := CONCAT(@sql_student, 'join pmi_admin.imp_table t on ul.table_id = t.table_id and ul.upload_status_code = \'', 'c', '\'');
SET @sql_student := CONCAT(@sql_student, 'where t.target_table_name = \'', 'pmi_ods_student', '\'');
prepare sql_student from @sql_student;
            
        execute sql_student;
            
        deallocate prepare sql_student;
IF  @pmi_ods_student_id > 1
    THEN 
                    SET @check_1 := '';
                    SET @sql_student_check_1 := '';
                    SET @sql_student_check_1 := CONCAT(@sql_student_check_1, 'select count(*) into @check_1 from ', @db, '.pmi_ods_student where student_id is null');
                    prepare sql_student_check_1 from @sql_student_check_1;
            
       execute sql_student_check_1;
            
       deallocate prepare sql_student_check_1;
                        IF @check_1 >= 1
                            THEN    
                                SET @pmi_ods_student_id := '';
                                SET @sql_check_1_log := '';
                                SET @sql_check_1_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_student', '\',', '\'', 'F', '\',', '\'', 'student_id is null', '\')');
                                
                                prepare sql_check_1_log from @sql_check_1_log;
            
                                    execute sql_check_1_log;
                                                
                                    deallocate prepare sql_check_1_log;
                        END IF;
END IF;
        IF  @pmi_ods_student_id > 1
  THEN 
              SET @check_3 := '';
                SET @sql_student_check_3 := '';
                SET @sql_student_check_3 := CONCAT(@sql_student_check_3, 'select count(*) into @check_3 from ', @db, '.pmi_ods_student t where not exists ');
                SET @sql_student_check_3 := CONCAT(@sql_student_check_3, '(select * from ', @db, '.pmi_ods_school s where s.school_id = t.school_id)');
                prepare sql_student_check_3 from @sql_student_check_3;
          
     execute sql_student_check_3;
          
     deallocate prepare sql_student_check_3;
                    IF @check_3 >= 1
                        THEN
                            SET @pmi_ods_student_id := '';
                            SET @sql_check_3_log := '';
                            SET @sql_check_3_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_student', '\',', '\'', 'F', '\',', '\'', 'school_id does not tie to school file', '\')');
                            
                            prepare sql_check_3_log from @sql_check_3_log;
          
                                execute sql_check_3_log;
                                            
                                deallocate prepare sql_check_3_log;
                    END IF;
        
        END IF;
SET @sql_course := '';
SET @sql_course := CONCAT(@sql_course, 'select max(ul.upload_id) INTO @pmi_ods_course_id from ', @db, '.imp_upload_log ul ');
SET @sql_course := CONCAT(@sql_course, 'join pmi_admin.imp_table t on ul.table_id = t.table_id and ul.upload_status_code = \'', 'c', '\'');
SET @sql_course := CONCAT(@sql_course, 'where t.target_table_name = \'', 'pmi_ods_course', '\'');
prepare sql_course from @sql_course;
            
        execute sql_course;
            
        deallocate prepare sql_course;
IF  @pmi_ods_course_id > 1
   THEN
            SET @check_1 := '';
                    SET @sql_course_check_1 := '';
                    SET @sql_course_check_1 := CONCAT(@sql_course_check_1, 'select count(*) into @check_1 from ', @db, '.pmi_ods_course where course_id is null');
                    prepare sql_course_check_1 from @sql_course_check_1;
            
       execute sql_course_check_1;
            
       deallocate prepare sql_course_check_1;
                        IF @check_1 >= 1
                            THEN    
                                SET @pmi_ods_course_id := '';
                                SET @sql_check_1_log := '';
                                SET @sql_check_1_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_course', '\',', '\'', 'F', '\',', '\'', 'course_id is null', '\')');
                                
                                prepare sql_check_1_log from @sql_check_1_log;
            
                                    execute sql_check_1_log;
                                                
                                    deallocate prepare sql_check_1_log;
                        END IF;
END IF;
        
        SET @setting := pmi_f_get_etl_setting('coreExcludeSchoolInCourseCode');
        
        IF  @pmi_ods_teacher_id > 1 AND @setting = 'y'
  THEN 
              SET @check_3 := '';
                SET @sql_course_check_3 := '';
                SET @sql_course_check_3 := CONCAT(@sql_course_check_3, 'select count(*) into @check_3 from ', @db, '.pmi_ods_course t where not exists ');
                SET @sql_course_check_3 := CONCAT(@sql_course_check_3, '(select * from ', @db, '.pmi_ods_school s where s.school_id = t.school_id)');
                prepare sql_course_check_3 from @sql_course_check_3;
          
     execute sql_course_check_3;
          
     deallocate prepare sql_course_check_3;
                    IF @check_3 >= 1
                        THEN
                            SET @pmi_ods_course_id := '';
                            SET @sql_check_3_log := '';
                            SET @sql_check_3_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_course', '\',', '\'', 'F', '\',', '\'', 'school_id does not tie to school file', '\')');
                            
                            prepare sql_check_3_log from @sql_check_3_log;
          
                                execute sql_check_3_log;
                                            
                                deallocate prepare sql_check_3_log;
                    END IF;
        
        END IF;
        
SET @sql_schedule := '';
SET @sql_schedule := CONCAT(@sql_schedule, 'select max(ul.upload_id) INTO @pmi_ods_schedule_id from ', @db, '.imp_upload_log ul ');
SET @sql_schedule := CONCAT(@sql_schedule, 'join pmi_admin.imp_table t on ul.table_id = t.table_id and ul.upload_status_code = \'', 'c', '\'');
SET @sql_schedule := CONCAT(@sql_schedule, 'where t.target_table_name = \'', 'pmi_ods_schedule', '\'');
prepare sql_schedule from @sql_schedule;
            
        execute sql_schedule;
            
        deallocate prepare sql_schedule;
IF  @pmi_ods_schedule_id > 1
   THEN
              SET @check_3 := '';
                SET @sql_schedule_check_3 := '';
                SET @sql_schedule_check_3 := CONCAT(@sql_schedule_check_3, 'select count(*) into @check_3 from ', @db, '.pmi_ods_schedule t where not exists ');
                SET @sql_schedule_check_3 := CONCAT(@sql_schedule_check_3, '(select * from ', @db, '.pmi_ods_school s where s.school_id = t.school_id)');
                prepare sql_schedule_check_3 from @sql_schedule_check_3;
          
     execute sql_schedule_check_3;
          
     deallocate prepare sql_schedule_check_3;
                    IF @check_3 >= 1
                        THEN
                            SET @pmi_ods_schedule_id := '';
                            SET @sql_check_3_log := '';
                            SET @sql_check_3_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_schedule', '\',', '\'', 'F', '\',', '\'', 'school_id does not tie to school file', '\')');
                            
                            prepare sql_check_3_log from @sql_check_3_log;
          
                                execute sql_check_3_log;
                                            
                                deallocate prepare sql_check_3_log;
                    END IF;
        
        END IF;                     
        
IF  @pmi_ods_schedule_id > 1
   THEN
              SET @check_4 := '';
                SET @sql_schedule_check_4 := '';
                SET @sql_schedule_check_4 := CONCAT(@sql_schedule_check_4, 'select count(*) into @check_4 from ', @db, '.pmi_ods_schedule t where not exists ');
                SET @sql_schedule_check_4 := CONCAT(@sql_schedule_check_4, '(select * from ', @db, '.pmi_ods_student s where s.student_id = t.student_id)');
                prepare sql_schedule_check_4 from @sql_schedule_check_4;
          
     execute sql_schedule_check_4;
          
     deallocate prepare sql_schedule_check_4;
                    IF @check_4 >= 1
                        THEN
                            SET @pmi_ods_schedule_id := '';
                            SET @sql_check_4_log := '';
                            SET @sql_check_4_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_schedule', '\',', '\'', 'F', '\',', '\'', 'student_id does not tie to student file', '\')');
                            
                            prepare sql_check_4_log from @sql_check_4_log;
          
                                execute sql_check_4_log;
                                            
                                deallocate prepare sql_check_4_log;
                    END IF;
        
        END IF;                     
IF  @pmi_ods_schedule_id > 1
   THEN
              SET @check_5 := '';
                SET @sql_schedule_check_5 := '';
                SET @sql_schedule_check_5 := CONCAT(@sql_schedule_check_5, 'select count(*) into @check_5 from ', @db, '.pmi_ods_schedule t where not exists ');
                SET @sql_schedule_check_5 := CONCAT(@sql_schedule_check_5, '(select * from ', @db, '.pmi_ods_teacher s where s.teacher_id = t.teacher_id)');
                prepare sql_schedule_check_5 from @sql_schedule_check_5;
          
     execute sql_schedule_check_5;
          
     deallocate prepare sql_schedule_check_5;
                    IF @check_5 >= 1
                        THEN
                            SET @pmi_ods_schedule_id := '';
                            SET @sql_check_5_log := '';
                            SET @sql_check_5_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_schedule', '\',', '\'', 'F', '\',', '\'', 'teacher_id does not tie to teacher file', '\')');
                            
                            prepare sql_check_5_log from @sql_check_5_log;
          
                                execute sql_check_5_log;
                                            
                                deallocate prepare sql_check_5_log;
                    END IF;
        
        END IF;                     
        
IF  @pmi_ods_schedule_id > 1
   THEN
              SET @check_6 := '';
                SET @sql_schedule_check_6 := '';
                SET @sql_schedule_check_6 := CONCAT(@sql_schedule_check_6, 'select count(*) into @check_6 from ', @db, '.pmi_ods_schedule t where not exists ');
                SET @sql_schedule_check_6 := CONCAT(@sql_schedule_check_6, '(select * from ', @db, '.pmi_ods_course s where s.course_id = t.course_id)');
                prepare sql_schedule_check_6 from @sql_schedule_check_6;
          
     execute sql_schedule_check_6;
          
     deallocate prepare sql_schedule_check_6;
                    IF @check_6 >= 1
                        THEN
                            SET @pmi_ods_schedule_id := '';
                            SET @sql_check_6_log := '';
                            SET @sql_check_6_log := CONCAT('call ', @db, '.imp_set_upload_file_status (\'', 'pmi_ods_schedule', '\',', '\'', 'F', '\',', '\'', 'course_id does not tie to course file', '\')');
                            
                            prepare sql_check_6_log from @sql_check_6_log;
          
                                execute sql_check_6_log;
                                            
                                deallocate prepare sql_check_6_log;
                    END IF;
        
        END IF;                                     
END
//
