create or replace function s_psql_dds.fn_dm_data_load(start_dt date, end_dt date) returns void language plpgsql as $$ begin if start_dt is null
    or end_dt is null then raise exception 'start_dt and end_dt must be not null';
end if;
if start_dt > end_dt then raise exception 'start_dt (%) > end_dt (%)',
start_dt,
end_dt;
end if;
delete from s_psql_dds.t_dm_task
where last_service_date between start_dt and end_dt;
insert into s_psql_dds.t_dm_owner(name)
select distinct s.owner_name
from s_psql_dds.t_sql_source_structured s
where s.last_service_date between start_dt and end_dt
    and s.owner_name is not null on conflict (name) do nothing;
insert into s_psql_dds.t_dm_brand(name)
select distinct s.brand
from s_psql_dds.t_sql_source_structured s
where s.last_service_date between start_dt and end_dt
    and s.brand is not null on conflict (name) do nothing;
insert into s_psql_dds.t_dm_model(name)
select distinct s.model
from s_psql_dds.t_sql_source_structured s
where s.last_service_date between start_dt and end_dt
    and s.model is not null on conflict (name) do nothing;
insert into s_psql_dds.t_dm_year(name)
select distinct s.year::varchar
from s_psql_dds.t_sql_source_structured s
where s.last_service_date between start_dt and end_dt
    and s.year is not null on conflict (name) do nothing;
insert into s_psql_dds.t_dm_vin(name)
select distinct s.vin
from s_psql_dds.t_sql_source_structured s
where s.last_service_date between start_dt and end_dt
    and s.vin is not null on conflict (name) do nothing;
insert into s_psql_dds.t_dm_issue(name)
select distinct s.issue_description
from s_psql_dds.t_sql_source_structured s
where s.last_service_date between start_dt and end_dt
    and s.issue_description is not null on conflict (name) do nothing;
insert into s_psql_dds.t_dm_task (
        src_id,
        car_id,
        owner_name,
        brand,
        model,
        year,
        vin,
        mileage,
        last_service_date,
        issue_description,
        service_cost,
        cleaned_at,
        owner_id,
        brand_id,
        model_id,
        year_id,
        vin_id,
        issue_id
    )
select s.src_id,
    s.car_id,
    s.owner_name,
    s.brand,
    s.model,
    s.year,
    s.vin,
    s.mileage,
    s.last_service_date,
    s.issue_description,
    s.service_cost,
    s.cleaned_at,
    o.id as owner_id,
    b.id as brand_id,
    m.id as model_id,
    y.id as year_id,
    v.id as vin_id,
    i.id as issue_id
from s_psql_dds.t_sql_source_structured s
    left join s_psql_dds.t_dm_owner o on o.name = s.owner_name
    left join s_psql_dds.t_dm_brand b on b.name = s.brand
    left join s_psql_dds.t_dm_model m on m.name = s.model
    left join s_psql_dds.t_dm_year y on y.name = s.year::varchar
    left join s_psql_dds.t_dm_vin v on v.name = s.vin
    left join s_psql_dds.t_dm_issue i on i.name = s.issue_description
where s.last_service_date between start_dt and end_dt;
end;
$$;