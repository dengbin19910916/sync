drop table if exists job_property;

create table job_property
(
    name        varchar(100) primary key,
    description varchar(100),
    enabled     boolean      not null,
    bean_name   varchar(200) not null,
    cron        varchar(20)  not null,
    sign        varchar(50)
);

insert into job_property(name, description, enabled, bean_name, cron)
values ('scheduleCalendar', '生成Schedule', 1, 'scheduleCalendar', '*/1 * * * * ?');
insert into job_property(name, description, enabled, bean_name, cron)
values ('jdDpsOrderSynchronizer8505', '京东自营订单单同步', 1, 'jdDpsOrderSynchronizer8505', '*/1 * * * * ?');
insert into job_property(name, description, enabled, bean_name, cron)
values ('jdDpsRefundSynchronizer8505', '京东自营退单同步', 1, 'jdDpsRefundSynchronizer8505', '*/1 * * * * ?');

drop table if exists sync_property;

create table sync_property
(
    id            bigint auto_increment
        primary key,
    type          tinyint               not null,
    shop_code     varchar(100)          not null,
    shop_name     varchar(100)          not null,
    origin_time   datetime              not null,
    start_page    int                   null,
    delay         int        default 60 null,
    time_interval int        default 60 not null,
    bean_name     varchar(100)          null,
    bean_class    varchar(200)          null,
    enabled       tinyint(1) default 1  not null,
    fired         tinyint(1) default 1  not null
);

INSERT INTO sync_property (id, type, shop_code, shop_name, origin_time, start_page, delay, time_interval,
                           bean_name, bean_class, enabled, fired)
VALUES (1, 1, '8505,8529,8997', '京东自营旗舰店', '2020-08-20 10:00:00', 1, 60, 60, 'jdDpsOrderSynchronizer8505',
        'io.xxx.sync.core.JdDpsOrderSynchronizer', 1, 1);
INSERT INTO sync_property (id, type, shop_code, shop_name, origin_time, start_page, delay, time_interval,
                           bean_name, bean_class, enabled, fired)
VALUES (2, 2, '8505,8529,8997', '京东自营旗舰店', '2020-08-20 10:00:00', 1, 60, 60, 'jdDpsRefundSynchronizer8505',
        'io.xxx.sync.core.JdDpsRefundSynchronizer', 1, 1);

drop table if exists sync_schedule;

create table sync_schedule
(
    id           bigint primary key,
    property_id  bigint                not null,
    start_time   datetime              not null,
    end_time     datetime              not null,
    priority     int     default 1     not null,
    completed    boolean default false not null,
    count        bigint,
    pull_millis  bigint,
    save_millis  bigint,
    total_millis bigint
);

drop table if exists sync_document;

create table sync_document
(
    id            bigint primary key,
    property_id   bigint      not null,
    shop_code     varchar(50) not null,
    shop_name     varchar(100),
    sn            varchar(64) not null,
    rsn           varchar(64),
    data          json        not null,
    created       datetime    not null,
    modified      datetime    not null,
    sync_created  datetime    not null,
    sync_modified datetime    not null
);