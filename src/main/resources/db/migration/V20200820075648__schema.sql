drop table if exists job_property;

create table job_property
(
    bean_name   varchar(200) primary key,
    description varchar(100),
    enabled     boolean     not null,
    address     varchar(32) not null,
    cron        varchar(20) not null
);

insert into job_property(bean_name, description, enabled, address, cron)
values ('scheduleCalendar', '生成Schedule', 1, '127.0.0.1', '*/1 * * * * ?');
insert into job_property(bean_name, description, enabled, address, cron)
values ('jdDpsOrderSynchronizer8505', '京东自营订单单同步', 1, '127.0.0.1', '*/1 * * * * ?');
insert into job_property(bean_name, description, enabled, address, cron)
values ('jdDpsRefundSynchronizer8505', '京东自营退单同步', 1, '127.0.0.1', '*/1 * * * * ?');

drop table if exists sync_property;

create table sync_property
(
    id                 bigint auto_increment primary key,
    type               tinyint               not null,
    shop_code          varchar(100)          not null,
    shop_name          varchar(100)          not null,
    origin_time        datetime              not null,
    start_page         int                   null,
    delay              int        default 60 null,
    time_interval      int        default 60 not null,
    host               varchar(100),
    count_path         varchar(200),
    data_path          varchar(200),
    count_json_path    varchar(100),
    data_json_path     varchar(100),
    sn_json_path       varchar(100),
    rsn_json_path      varchar(100),
    created_json_path  varchar(100),
    modified_json_path varchar(100),
    bean_name          varchar(100)          null,
    bean_class         varchar(200)          null,
    enabled            tinyint(1) default 1  not null,
    fired              tinyint(1) default 1  not null,
    compositional      tinyint(1) default 0  not null
);

INSERT INTO sync_property (id, type, shop_code, shop_name, origin_time, start_page, delay, time_interval,
                           host, count_path, data_path, count_json_path, data_json_path, sn_json_path, rsn_json_path,
                           created_json_path, modified_json_path, bean_name, bean_class, enabled, fired, compositional)
VALUES (1, 1, '8505,8529,8997', '京东自营旗舰店', '2020-08-01 00:00:00', 1, 60, 60,
        null, null, null, null, null, null, null, null, null,
        'jdDpsOrderSynchronizer8505', 'io.xxx.sync.core.JdDpsOrderSynchronizer', 1, 1, 0);
INSERT INTO sync_property (id, type, shop_code, shop_name, origin_time, start_page, delay, time_interval,
                           host, count_path, data_path, count_json_path, data_json_path, sn_json_path, rsn_json_path,
                           created_json_path, modified_json_path, bean_name, bean_class, enabled, fired, compositional)
VALUES (2, 2, '8505,8529,8997', '京东自营旗舰店', '2020-08-01 00:00:00', 1, 60, 60,
        null, null, null, null, null, null, null, null, null,
        'jdDpsRefundSynchronizer8505', 'io.xxx.sync.core.JdDpsRefundSynchronizer', 1, 1, 0);

drop table if exists sync_schedule;

create table sync_schedule
(
    id           bigint primary key,
    property_id  bigint                not null,
    start_time   datetime              not null,
    end_time     datetime              not null,
    priority     int     default 0     not null,
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