create table if not exists s_psql_dds.t_dm_owner (
    id   serial primary key,
    name varchar not null unique
);

create table if not exists s_psql_dds.t_dm_brand (
    id   serial primary key,
    name varchar not null unique
);

create table if not exists s_psql_dds.t_dm_model (
    id   serial primary key,
    name varchar not null unique
);

create table if not exists s_psql_dds.t_dm_year (
    id   serial primary key,
    name varchar not null unique
);

create table if not exists s_psql_dds.t_dm_vin (
    id   serial primary key,
    name varchar not null unique
);

create table if not exists s_psql_dds.t_dm_issue (
    id   serial primary key,
    name varchar not null unique
);


create table if not exists s_psql_dds.t_dm_task (
    id bigserial primary key,

    src_id bigint,
    car_id bigint,
    owner_name text,
    brand text,
    model text,
    year int,
    vin text,

    mileage numeric,
    last_service_date date,
    issue_description text,
    service_cost numeric,
    cleaned_at timestamptz,

    owner_id int,
    brand_id int,
    model_id int,
    year_id  int,
    vin_id   int,
    issue_id int,

    constraint fk_dm_owner foreign key (owner_id) references s_psql_dds.t_dm_owner(id),
    constraint fk_dm_brand foreign key (brand_id) references s_psql_dds.t_dm_brand(id),
    constraint fk_dm_model foreign key (model_id) references s_psql_dds.t_dm_model(id),
    constraint fk_dm_year  foreign key (year_id)  references s_psql_dds.t_dm_year(id),
    constraint fk_dm_vin   foreign key (vin_id)   references s_psql_dds.t_dm_vin(id),
    constraint fk_dm_issue foreign key (issue_id) references s_psql_dds.t_dm_issue(id)
);

create index if not exists ix_dm_task_last_service_date
    on s_psql_dds.t_dm_task(last_service_date);