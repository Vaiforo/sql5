create schema if not exists s_psql_dds;
create table if not exists s_psql_dds.t_sql_source_unstructured (
    id bigserial primary key,
    car_id varchar,
    owner_name varchar,
    brand varchar,
    model varchar,
    year varchar,
    vin varchar,
    mileage varchar,
    last_service_date varchar,
    issue_description varchar,
    service_cost varchar,
    loaded_at timestamptz default now()
);
select *
from s_psql_dds.t_sql_source_unstructured;
drop table s_psql_dds.t_sql_source_unstructured;