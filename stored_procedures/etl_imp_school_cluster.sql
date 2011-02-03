/*
$Rev: 8472 $ 
$Author: randall.stanley $ 
$Date: 2010-04-29 16:01:54 -0400 (Thu, 29 Apr 2010) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_imp_school_cluster.sql $
$Id: etl_imp_school_cluster.sql 8472 2010-04-29 20:01:54Z randall.stanley $ 
 */

############################################################################
#INSERT into:
#    c_school_cluster
#    c_school_cluster_list
############################################################################

DROP PROCEDURE IF EXISTS etl_imp_school_cluster //

CREATE definer=`dbadmin`@`localhost` procedure etl_imp_school_cluster()
CONTAINS SQL
SQL SECURITY INVOKER
COMMENT '$Rev: 8472 $ $Date: 2010-04-29 16:01:54 -0400 (Thu, 29 Apr 2010) $'
BEGIN

  call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

  SELECT  count(*) 
  INTO    @view_exists
  FROM    information_schema.tables t
  WHERE   t.table_schema = @db_name_core
  AND     t.table_name = 'v_pmi_ods_cluster';

  IF @view_exists > 0 THEN
  
    SELECT count(*)
    INTO @row_count
    FROM v_pmi_ods_cluster;
    
    IF @row_count > 0 THEN 
  
          SELECT count(*)
          INTO @cluster_list_count
          FROM c_school_cluster_list;
          
          IF @cluster_list_count > 0 THEN
              TRUNCATE c_school_cluster_list;
          END IF;
          
          DROP TABLE IF EXISTS tmp_id_assign_cluster;
            CREATE TABLE tmp_id_assign_cluster (
                new_id int(11) not null,
                base_code varchar(20) not null,
                PRIMARY KEY  (`new_id`),
                UNIQUE KEY `uq_tmp_id_assign_cluster` (`base_code`)
            );
            
        
            ### obtain a new cluster id only for clusters that are not already in the target table.
            INSERT tmp_id_assign_cluster (new_id, base_code)
            SELECT  pmi_f_get_next_sequence_app_db('c_school_cluster', 1), ods.cluster_id
            FROM    v_pmi_ods_cluster AS ods
            LEFT JOIN   c_school_cluster as tar
                    ON      ods.cluster_id = tar.cluster_code
            WHERE   tar.cluster_id IS NULL
            GROUP BY ods.cluster_id
            ;      
              
            ### insert into c_school_cluster: this will insert a new id for a new cluster or update the records for existing records
            ### in the target table and it will update changes to the moniker of the cluster as well
            INSERT INTO c_school_cluster (
              cluster_id,
              cluster_code,
              moniker,
              last_user_id,
              create_timestamp
              )
              SELECT   COALESCE(tmpid.new_id, tar.cluster_id),
                        MIN(ods.cluster_id),
                        MIN(ods.cluster_title),
                        1234,
                        now()
              FROM v_pmi_ods_cluster AS ods
              LEFT JOIN  tmp_id_assign_cluster AS tmpid
                    ON ods.cluster_id = tmpid.base_code
              LEFT JOIN c_school_cluster AS tar
                    ON ods.cluster_id = tar.cluster_code
            GROUP BY ods.cluster_id
            ON DUPLICATE KEY UPDATE last_user_id = 1234,
                      moniker = values(moniker)
            ;   
          
          
            ### insert into c_school_cluster_list
            INSERT INTO c_school_cluster_list (
              school_id,
              cluster_id,
              last_user_id,
              create_timestamp
              )
              SELECT sch.school_id
                      ,cl.cluster_id
                      ,1234
                      ,now()
              FROM c_school AS sch
              JOIN v_pmi_ods_cluster AS vc
                      on  vc.school_id = sch.school_code
              JOIN c_school_cluster AS cl
                      ON vc.cluster_id = cl.cluster_code
              ON DUPLICATE KEY UPDATE last_user_id = 1234
            ;
          
            DROP TABLE IF EXISTS tmp_id_assign_cluster;
            
      END IF;  # end if rows in view
     
    #################
    ## Update Log
    #################
        
    SET @sql_scan_log := '';
    SET @sql_scan_log := CONCAT('call ', @db_name_ods, '.imp_set_upload_file_status (\'', 'pmi_ods_cluster', '\',', '\'', 'P', '\',', '\'', 'ETL Load Successful', '\')');

    prepare sql_scan_log from @sql_scan_log;
    execute sql_scan_log;
    deallocate prepare sql_scan_log;  
         
  END IF; # end if view exisits

END;
//
