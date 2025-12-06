create table if not exists s_psql_dds.t_sql_source_structured (
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
    cleaned_at timestamptz default now()
);
select *
from s_psql_dds.t_sql_source_structured;
drop table s_psql_dds.t_sql_source_structured;