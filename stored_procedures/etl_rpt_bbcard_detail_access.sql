drop procedure if exists etl_rpt_bbcard_detail_access//                                                          
                                                                          
CREATE DEFINER=`dbadmin`@`localhost` PROCEDURE `etl_rpt_bbcard_detail_access`()
    SQL SECURITY INVOKER                                                  
    COMMENT 'Date: 2012-09-25 etl_rpt_bbcard_detail_access'               
proc: begin                                                               
                                                                          
                                                                          
                                                                          
    declare v_ods_table varchar(64);                                      
    declare v_ods_view varchar(64);                                       
    declare v_view_exists tinyint(1);                                     
    declare v_bb_group_id int(11);                                        
    declare v_backfill_needed smallint(6);                                
    ####declare v_date_format_mask varchar(15) default '%m%d%Y';        
    declare v_date_format_mask varchar(15) default '%Y%m';        
    declare v_grade_unassigned_id  int(10);                               
    declare v_school_unassigned_id  int(10);                              
                                                                          
    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend, @db_name_dw);
                                                                          
    set v_ods_table = 'pmi_ods_access';                                   
    set v_ods_view = concat('v_', v_ods_table);                           
                                                                          
    select  count(*)                                                      
    into    v_view_exists                                                 
    from    information_schema.views t                                    
    where   t.table_schema = database()                                   
    and     t.table_name = v_ods_view;                                    
                                                                          
                                                                          
    if v_view_exists > 0 then                                             
                                                                          
        select  bb_group_id                                               
        into    v_bb_group_id                                             
        from    pm_bbcard_group                                           
        where   bb_group_code = 'access'                                  
        ;                                                                 
                                                                          
        select  grade_level_id                                            
        into    v_grade_unassigned_id                                     
        from    c_grade_level                                             
        where   grade_code = 'unassigned'                                 
        ;                                                                 
                                                                          
        select school_id                                                  
        into    v_school_unassigned_id                                    
        from    c_school                                                  
        where   school_code = 'unassigned'                                
        ;                                                                 
                                                                          
        set @access_date_format_mask := pmi_f_get_etl_setting('accessDateFormatMask');
                                                                          
        if @access_date_format_mask is not null then                      
            set v_date_format_mask = @access_date_format_mask;            
        end if;                                                           
                                                                          
        drop table if exists `tmp_stu_admin`;                             
        drop table if exists `tmp_date_conversion`;                       
        drop table if exists `tmp_student_year_backfill`;                 
                                                                          
        create table `tmp_stu_admin` (                                    
          `student_code` varchar(15) NOT NULL,                            
          `row_num` int(10) NOT NULL,                                     
          `student_id` int(10) NOT NULL,                                  
          `school_year_id` smallint(4) NOT NULL,                          
          `grade_code` varchar(15) default null,                          
          `grade_id` int(10) default null,                                
          `school_code` varchar(15) default null,                         
          `backfill_needed_flag` tinyint(1),                              
          primary key (`student_id`, `school_year_id`)                    
        ) engine=innodb default charset=latin1                            
        ;                                                                 
                                                                          
        create table `tmp_date_conversion` (                              
          `date_tested` varchar(10) NOT NULL                              
         ,`school_year_id` int unsigned,                                  
         primary key (`school_year_id`),                                  
          key (`date_tested`)                                             
        ) engine=innodb default charset=latin1                            
        ;                                                                 
                                                                          
        create table `tmp_student_year_backfill` (                        
           `ods_row_num` int(10) not null,                                
           `student_id` int(10) not null,                                 
           `school_year_id` smallint(4) not null,                         
           `grade_level_id` int(10) null,                                 
           `school_id` int(10) null,                                      
           primary key  (`ods_row_num`),                                  
           unique key `uq_tmp_student_year_backfill` (`student_id`, `school_year_id`)
         ) engine=innodb default charset=latin1                           
         ;                                                                
                                                                          
                                                                          
                                                                          
                                                                          
                                                                          
                                                                          
        insert tmp_date_conversion (                                      
            date_tested                                                   
        )                                                                 
        select distinct                                                   
            test_year_mo                                               
        from v_pmi_ods_access ods;                                        
                                                                          
        update tmp_date_conversion tdc                                    
        join c_school_year sy                                             
           on str_to_date(tdc.date_tested, v_date_format_mask) between sy.begin_date and sy.end_date
        set tdc.school_year_id = sy.school_year_id;    
        
        
        ####set @date_tested = '20-JAN-12';
        
        #####insert into tmp_date_conversion values (@date_tested,2012);                                                                    
                                                                          
      #### replaced ods.date_tested with @date_tested for now    
      
      #### replaced ods.grade with @ods_grade for now    
      
      ### set @ods_grade =  null;
                                                                      
                                                                          
                                                                          
        insert  tmp_stu_admin (                                           
                row_num                                                   
               ,student_id                                                
               ,student_code                                              
               ,school_year_id                                            
               ,grade_code                                                
               ,grade_id                                                  
               ,school_code                                               
               ,backfill_needed_flag                                      
        )                                                                 
        select  max(ods.row_num)                                          
               ,s.student_id                                              
               ,ods.district_student_id                                            
               ,tdc.school_year_id                                        
               ,ods.grade_code                                              
               ,NULL                                                      
               ,NULL                                                      
               ,case when sty.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_access as ods   
        join    tmp_date_conversion tdc
                on ods.test_year_mo = tdc.date_tested                                                  
        join    c_student as s                                            
                on    s.student_state_code = ods.district_student_id      
        left join c_student_year as sty                                   
                on    sty.student_id = s.student_id                       
                and   sty.school_year_id = tdc.school_year_id             
        where   ods.district_student_id is not null                       
        group by ods.district_student_id                                  
        union all                                                         
        select  max(ods.row_num)                                          
               ,s.student_id                                              
               ,ods.district_student_id                                   
               ,tdc.school_year_id                                        
               ,ods.grade_code                                                
               ,NULL                                                      
               ,NULL                                                      
               ,case when sty.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_access as ods                                   
        join    tmp_date_conversion tdc                                   
                on ods.test_year_mo = tdc.date_tested                      
        join    c_student as s                                            
                on    s.fid_code = ods.district_student_id                
        left join c_student_year as sty                                   
                on    sty.student_id = s.student_id                       
                and   sty.school_year_id = tdc.school_year_id             
        where   ods.district_student_id is not null                       
        group by ods.district_student_id                                  
        union all                                                         
        select  max(ods.row_num)                                          
               ,s.student_id                                              
               ,ods.district_student_id                                   
               ,tdc.school_year_id                                        
               ,ods.grade_code                                                
               ,NULL                                                      
               ,NULL                                                      
               ,case when sty.school_year_id is null then 1 end as backfill_needed_flag
        from    v_pmi_ods_access as ods                                   
        join    tmp_date_conversion tdc                                   
                on ods.test_year_mo = tdc.date_tested                      
        join    c_student as s                                            
                on    s.student_code = ods.district_student_id            
        left join c_student_year as sty                                   
                on    sty.student_id = s.student_id                       
                and   sty.school_year_id = tdc.school_year_id             
        where   ods.district_student_id is not null                       
        group by ods.district_student_id                                  
        order by 1                                                        
        on duplicate key update row_num = values(row_num)                 
        ;                                                                 
                                                                          
                                                                          
                                                                          
                                                                          
                                                                          
        update tmp_stu_admin sadmin                                       
        left join v_pmi_xref_grade_level as gxref                         
                on sadmin.grade_code = gxref.client_grade_code            
        left join c_grade_level as grd                                    
                on gxref.pmi_grade_code = grd.grade_code                  
        set sadmin.grade_id = coalesce(grd.grade_level_id, v_grade_unassigned_id)
        ;                                                                 
                                                                          
                                                                          
                                                                          
                                                                          
                                                                          
                                                                          
                                                                          
        select count(*)                                                   
        into v_backfill_needed                                            
        from tmp_stu_admin                                                
        where backfill_needed_flag = 1                                    
        ;                                                                 
                                                                          
        if v_backfill_needed > 0 then                                     
                                                                          
            insert tmp_student_year_backfill (                            
                   ods_row_num                                            
                  ,student_id                                             
                  ,school_year_id                                         
                  ,grade_level_id                                         
                  ,school_id                                              
            )                                                             
            select   sadmin.row_num                                       
                    ,sadmin.student_id                                    
                    ,sadmin.school_year_id                                
                    ,sadmin.grade_id                                      
                    ,v_school_unassigned_id                               
            from tmp_stu_admin as sadmin                                  
            where sadmin.backfill_needed_flag = 1                         
            on duplicate key update grade_level_id = values(grade_level_id)
                ,school_id = values(school_id)                            
            ;                                                             
                                                                          
                                                                          
                                                                          
                                                                          
                                                                          
                                                                          
                                                                          
            call etl_hst_load_backfill_stu_year();                        
                                                                          
        end if;                                                           
                                                                          
                                                                          
                                                                          
        insert rpt_bbcard_detail_access (                                 
            bb_group_id                                                   
            ,bb_measure_id                                                
            ,bb_measure_item_id                                           
            ,student_id                                                   
            ,school_year_id                                               
            ,score                                                        
            ,score_type                                                   
            ,score_color                                                  
            ,last_user_id                                                 
            ,create_timestamp                                             
        )                                                                 
        select  m.bb_group_id                                             
            ,m.bb_measure_id                                              
            ,mi.bb_measure_item_id                                        
            ,s.student_id                                                 
            ,tdc.school_year_id                                           
            ,max(case                                                     
                  when m.bb_measure_code = 'accComposite' and mi.bb_measure_item_code = 'accCompositeScaleScore' then ods.composite_ss 
                  when m.bb_measure_code = 'accComposite' and mi.bb_measure_item_code = 'accCompositeProfScore' then ods.composite_pl
                  when m.bb_measure_code = 'accComposite' and mi.bb_measure_item_code = 'accCompositeProfKGScore' then ods.composite_pl_kg
                  when m.bb_measure_code = 'accWriting' and mi.bb_measure_item_code = 'accWriteScaleScore' then ods.writing_ss 
                  when m.bb_measure_code = 'accWriting' and mi.bb_measure_item_code = 'accWriteProfScore' then ods.Writing_pl
                  when m.bb_measure_code = 'accWriting' and mi.bb_measure_item_code = 'accWriteProfKGScore' then ods.Writing_pl_kg   
                  when m.bb_measure_code = 'accReading' and mi.bb_measure_item_code = 'accReadScaleScore' then ods.Reading_ss      
                  when m.bb_measure_code = 'accReading' and mi.bb_measure_item_code = 'accReadProfScore' then ods.Reading_pl       
                  when m.bb_measure_code = 'accReading' and mi.bb_measure_item_code = 'accReadProfKGScore' then ods.Reading_pl_kg  
                  when m.bb_measure_code = 'accSpeaking' and mi.bb_measure_item_code = 'accSpeakScaleScore' then ods.Speaking_ss       
                  when m.bb_measure_code = 'accSpeaking' and mi.bb_measure_item_code = 'accSpeakProfScore' then ods.Speaking_pl       
                  when m.bb_measure_code = 'accSpeaking' and mi.bb_measure_item_code = 'accSpeakProfKGScore' then ods.Speaking_pl_kg 
                  when m.bb_measure_code = 'accListen' and mi.bb_measure_item_code = 'accListenScaleScore' then ods.Listening_ss       
                  when m.bb_measure_code = 'accListen' and mi.bb_measure_item_code = 'accListenProfScore' then ods.Listening_pl       
                  when m.bb_measure_code = 'accListen' and mi.bb_measure_item_code = 'accListenProfKGScore' then ods.Listening_pl_kg  
                  when m.bb_measure_code = 'accOral' and mi.bb_measure_item_code = 'accOralScaleScore' then ods.Oral_ss          
                  when m.bb_measure_code = 'accOral' and mi.bb_measure_item_code = 'accOralProfScore' then ods.Oral_pl       
                  when m.bb_measure_code = 'accOral' and mi.bb_measure_item_code = 'accOralProfKGScore' then ods.Oral_pl_kg  
                  when m.bb_measure_code = 'accLiteracy' and mi.bb_measure_item_code = 'accLiteracyScaleScore' then ods.Literacy_ss      
                  when m.bb_measure_code = 'accLiteracy' and mi.bb_measure_item_code = 'accLiteracyProfScore' then ods.Literacy_pl       
                  when m.bb_measure_code = 'accLiteracy' and mi.bb_measure_item_code = 'accLiteracyProfKGScore' then ods.Literacy_pl_kg 
                  when m.bb_measure_code = 'accCompre' and mi.bb_measure_item_code = 'accCompreScaleScore' then ods.Comprehension_ss      
                  when m.bb_measure_code = 'accCompre' and mi.bb_measure_item_code = 'accCompreProfScore' then ods.Comprehension_pl        
                  when m.bb_measure_code = 'accCompre' and mi.bb_measure_item_code = 'accCompreProfKGScore' then ods.Comprehension_pl_kg                     
       end) as score                                                      
            ,'a'                                                          
            ,null                                                         
            ,1234                                                         
            ,now()                                                        
        from    v_pmi_ods_access as ods                                   
        join    tmp_date_conversion tdc                                   
                on ods.test_year_mo = tdc.date_tested                      
        join    tmp_stu_admin sadmin                                      
                on ods.row_num = sadmin.row_num                           
        join    c_student as s                                            
                on      s.student_code = ods.district_student_id          
        join    pm_bbcard_measure as m                                    
                on      m.bb_group_id = v_bb_group_id                     
        join    pm_bbcard_measure_item as mi                              
                on      m.bb_group_id = mi.bb_group_id                    
                   and  m.bb_measure_id = mi.bb_measure_id                
        group by m.bb_group_id                                            
            ,m.bb_measure_id                                              
            ,mi.bb_measure_item_id                                        
            ,s.student_id                                                 
            ,tdc.school_year_id                                           
        having score is not null and score != ''                          
        on duplicate key update score = values(score)                     
            ,score_type = values(score_type)                              
            ,score_color = values(score_color)                            
            ,last_user_id = values(last_user_id)                          
            ,last_edit_timestamp = values(last_edit_timestamp)            
        ;                                                                 
                                                                          
      
       
        ## Only color proficiency level scores 
        
        update rpt_bbcard_detail_access as rpt
        join c_student_year as sy
                on rpt.student_id = sy.student_id
                and rpt.school_year_id = sy.school_year_id
        join c_grade_level as g
                on sy.grade_level_id = g.grade_level_id
        join pm_bbcard_measure_item mi
                on  rpt.bb_group_id = mi.bb_group_id
                and rpt.bb_measure_id = mi.bb_measure_id
                and rpt.bb_measure_item_id = mi.bb_measure_item_id
                and mi.bb_measure_item_code like '%Prof%'
        join pm_color_access as c
                on sy.school_year_id between c.begin_year and c.end_year
                and g.grade_sequence between c.begin_grade_sequence and c.end_grade_sequence
                and rpt.score between c.min_score and c.max_score
        join pmi_color as pmic
              on c.color_id = pmic.color_id
        set score_color = pmic.moniker
        ;  
                                                    
                                                                          
        set @sql_scan_log := concat('call ', @db_name_ods, '.imp_set_upload_file_status (\'', v_ods_table, '\', \'P\', \'ETL Load Successful\')');
                                                                          
        prepare sql_scan_log from @sql_scan_log;                          
        execute sql_scan_log;                                             
        deallocate prepare sql_scan_log;                                  
                                                                          
        drop table if exists `tmp_stu_admin`;                             
        drop table if exists `tmp_date_conversion`;                       
        drop table if exists `tmp_student_year_backfill`;                 
                                                                          
    end if;                                                               
                                                                          
end proc//                                                                
                                                                          
