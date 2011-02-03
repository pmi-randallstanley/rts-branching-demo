/*
$Rev: 9893 $ 
$Author: randall.stanley $ 
$Date: 2011-01-18 09:10:39 -0500 (Tue, 18 Jan 2011) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bbcard_detail_grades.sql $
$Id: etl_rpt_bbcard_detail_grades.sql 9893 2011-01-18 14:10:39Z randall.stanley $ 
*/

drop procedure if exists etl_rpt_bbcard_detail_grades//

create definer=`dbadmin`@`localhost` procedure etl_rpt_bbcard_detail_grades()
contains sql
sql security invoker
comment '$Rev: 9893 $ $Date: 2011-01-18 09:10:39 -0500 (Tue, 18 Jan 2011) $'


proc: begin 

    call set_db_vars(@client_id, @state_id, @db_name, @db_name_core, @db_name_ods, @db_name_ib, @db_name_view, @db_name_pend);

    drop table if exists `tmp_id_assign_bb_meas`;
    drop table if exists `tmp_id_assign_bb_meas_item`;

    create table `tmp_id_assign_bb_meas` (
      `bb_group_id` int(11) not null,
      `new_id` int(11) not null,
      `base_code` varchar(50) not null,
      `moniker` varchar(50) default null,
      primary key  (`bb_group_id`,`new_id`),
      unique key `uq_tmp_id_assign_bb_meas` (`bb_group_id`,`base_code`)
    ) engine=innodb default charset=latin1
    ;
    create table `tmp_id_assign_bb_meas_item` (
      `bb_group_id` int(11) NOT NULL,
      `bb_measure_id` int(11) NOT NULL,
      `new_id` int(11) not null,
      `base_code` varchar(50) not null,
      `moniker` varchar(120) default null,
      `sort_order` smallint(6) default null,
      primary key  (`bb_group_id`,`bb_measure_id`,`new_id`),
      unique key `uq_tmp_id_assign_bb_meas_item` (`bb_group_id`,`bb_measure_id`,`base_code`)
    );

    select  bb_group_id
    into    @bb_group_id
    from    pm_bbcard_group
    where   bb_group_code = 'grades'
    ;

    # Get id's for new measures (new courses)
    insert  tmp_id_assign_bb_meas (
        bb_group_id
        ,new_id
        ,base_code
        ,moniker
    )
    select  @bb_group_id
        ,pmi_admin.pmi_f_get_next_sequence('pm_bbcard_measure', 1)
        ,dt.course_code
        ,coalesce(dt.course_title, 'Unknown')
    from    (
                select   src.course_code, min(src.course_title) as course_title
                from    rpt_grades as src
                left join   pm_bbcard_measure as tar
                        on      tar.bb_group_id = @bb_group_id
                        and     tar.bb_measure_code = src.course_code
                where   tar.bb_measure_id is null
                group by src.course_code
            ) as dt

    ;      

    insert  pm_bbcard_measure ( 
        bb_group_id
        ,bb_measure_id
        ,bb_measure_code
        ,moniker
        ,sort_order
        ,swatch_id
        ,active_flag
        ,dynamic_creation_flag
        ,last_user_id
        ,create_timestamp
    )
    select  tmpid.bb_group_id
        ,tmpid.new_id
        ,tmpid.base_code
        ,tmpid.moniker
        ,0
        ,null
        ,1
        ,1
        ,1234
        ,now()
    from    tmp_id_assign_bb_meas as tmpid
    ;

    # Get id's for new measures items (new mp)
    insert tmp_id_assign_bb_meas_item (
        bb_group_id
        ,bb_measure_id
        ,new_id
        ,base_code
        ,moniker
        ,sort_order
    )
    select  dt.bb_group_id
        ,dt.bb_measure_id
        ,pmi_admin.pmi_f_get_next_sequence('pm_bbcard_measure_item', 1)
        ,dt.mp_code
        ,coalesce(dt.mp_title, 'Unknown')
        ,dt.sort_order
    from    (
                select  m.bb_group_id, m.bb_measure_id, src.mp_code, src.mp_title, min(src.mp_order) as sort_order
                from    rpt_grades as src
                join    pm_bbcard_measure as m
                        on      m.bb_group_id = @bb_group_id
                        and     m.bb_measure_code = src.course_code
                left join   pm_bbcard_measure_item as tar
                        on      tar.bb_group_id = m.bb_group_id
                        and     tar.bb_measure_id = m.bb_measure_id
                        and     tar.bb_measure_item_code = src.mp_code
                where   tar.bb_measure_item_id is null
                group by m.bb_group_id, m.bb_measure_id, src.mp_code, src.mp_title
            ) as dt

    ;


    # need to discuss handling sort order with incremental course adds
    insert  pm_bbcard_measure_item ( 
        bb_group_id
        ,bb_measure_id
        ,bb_measure_item_id
        ,bb_measure_item_code
        ,moniker
        ,sort_order
        ,swatch_id
        ,score_sort_type_code
        ,active_flag
        ,dynamic_creation_flag
        ,last_user_id
        ,create_timestamp
    )
    select tmpid.bb_group_id
        ,tmpid.bb_measure_id
        ,tmpid.new_id
        ,tmpid.base_code
        ,tmpid.moniker
        ,coalesce(tmpid.sort_order,0)
        ,null
        ,'a'
        ,1
        ,1
        ,1234
        ,now()
    from    tmp_id_assign_bb_meas_item as tmpid
    ;    
    

  
    insert rpt_bbcard_detail_grades (
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

    select  mi.bb_group_id
        ,mi.bb_measure_id
        ,mi.bb_measure_item_id
        ,g.student_id
        ,g.school_year_id
        ,g.mark
        ,'a'
        ,null
        ,1234
        ,now()

    from    rpt_grades as g
    join    pm_bbcard_group b
            on      bb_group_id = @bb_group_id
    join    pm_bbcard_measure as m
            on      m.bb_group_id = b.bb_group_id
            and     m.bb_measure_code = g.course_code
    join    pm_bbcard_measure_item as mi
            on      mi.bb_group_id = m.bb_group_id
            and     mi.bb_measure_id = m.bb_measure_id
            and     mi.bb_measure_item_code = g.mp_code
    on duplicate key update score = values(score)
        ,score_color = values(score_color)
    ;

    drop table if exists `tmp_id_assign_bb_meas`;
    drop table if exists `tmp_id_assign_bb_meas_item`;


end proc;
//
