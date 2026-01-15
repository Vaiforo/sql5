create database if not exists auto_seller_dm default character set utf8mb4 collate utf8mb4_unicode_ci;
use auto_seller_dm;
create table if not exists t_dm_task (
    id bigint not null primary key,
    src_id bigint,
    car_id bigint,
    owner_name text,
    brand text,
    model text,
    year int,
    vin text,
    mileage decimal(18, 6),
    last_service_date date,
    issue_description text,
    service_cost decimal(18, 6),
    cleaned_at datetime,
    owner_id int,
    brand_id int,
    model_id int,
    year_id int,
    vin_id int,
    issue_id int
) engine = InnoDB default charset = utf8mb4;