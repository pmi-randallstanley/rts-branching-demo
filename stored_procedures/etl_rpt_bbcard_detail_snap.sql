/*
$Rev: 9908 $
$Author: ryan.riordan $
$Date: 2011-01-19 14:46:35 -0500 (Wed, 19 Jan 2011) $
$HeadURL: http://atlanta-web.performancematters.com:8099/svn/pminternal/Data/Redwood/Core/stored_procedures/etl_rpt_bbcard_detail_snap.sql $
$Id: etl_rpt_bbcard_detail_snap.sql 9908 2011-01-19 19:46:35Z ryan.riordan $
*/

delimiter //
drop procedure if exists etl_rpt_bbcard_detail_snap //

create definer=`dbadmin`@`localhost` procedure etl_rpt_bbcard_detail_snap()
contains sql
sql security invoker
comment '$Rev: 9908 $ $Date: 2011-01-19 14:46:35 -0500 (Wed, 19 Jan 2011) $'

proc: begin

    drop table if exists `tmp_snap_bbcard_measure`;
    drop table if exists `tmp_snap_bbcard_map`;

    create table `tmp_snap_bbcard_measure` (
      `pm_measure_code` varchar(20) NOT NULL,
      `bb_measure_code` varchar(40) NOT NULL,
      PRIMARY KEY (`pm_measure_code`),
      UNIQUE KEY (`bb_measure_code`)
    ) engine=innodb default charset=latin1
    ;

    create table `tmp_snap_bbcard_map` (
        `measure_period_id` int(11) NOT NULL,
        `bb_group_id` int(11) NOT NULL,
        `bb_measure_id` int(11) NOT NULL,
        `bb_measure_item_id` int(11) NOT NULL,
      PRIMARY KEY (`measure_period_id`)
    ) engine=innodb default charset=latin1
    ;

    insert into tmp_snap_bbcard_measure (
        pm_measure_code
       ,bb_measure_code
    )
    values ('FNWS','fwdNumWordSeq'),
           ('BNWS','backNumWordSeq'),
           ('NID', 'numId'),
           ('AS',  'addSub'),
           ('FP',  'fingerPatterns'),
           ('SP',  'spatialPatterns')
    ;

    insert into tmp_snap_bbcard_map (
        measure_period_id
       ,bb_group_id
       ,bb_measure_id
       ,bb_measure_item_id
    )
    select smp.measure_period_id
          ,pbmi.bb_group_id
          ,pbmi.bb_measure_id
          ,pbmi.bb_measure_item_id
    from pm_snap_measure sm
    join pm_snap_measure_period smp
      on smp.measure_id = sm.measure_id
    join pm_snap_assess_freq_period safp
      on safp.freq_id   = smp.freq_id
     and safp.period_id = smp.period_id
    join tmp_snap_bbcard_measure t
      on sm.measure_code = t.pm_measure_code
    join pm_bbcard_measure pbm
      on pbm.bb_measure_code = t.bb_measure_code
    join pm_bbcard_measure_item pbmi
      on pbmi.bb_measure_id = pbm.bb_measure_id
     and pbmi.moniker = safp.moniker
    ;

    insert into rpt_bbcard_detail_snap (
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
    select m.bb_group_id
          ,m.bb_measure_id
          ,m.bb_measure_item_id
          ,rpt.student_id
          ,rpt.school_year_id
          ,rpt.score
          ,'n'
          ,rpt.score_color
          ,1234
          ,NOW()
    from rpt_snap_scores rpt
    join tmp_snap_bbcard_map m
      on m.measure_period_id = rpt.measure_period_id
      on duplicate key update score = values(score)
        ,score_type = values(score_type)
        ,score_color = values(score_color)
        ,last_user_id = values(last_user_id)
    ;

    drop table if exists `tmp_snap_bbcard_measure`;
    drop table if exists `tmp_snap_bbcard_map`;

end proc;
//
