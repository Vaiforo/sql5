create or replace function s_psql_dds.fn_etl_data_load(start_date date, end_date date) returns void language plpgsql as $$ begin
delete from s_psql_dds.t_sql_source_structured t
where t.last_service_date between start_date and end_date;
with src as (
    select u.*,
        case
            when u.last_service_date ~ '^\d{4}-\d{2}-\d{2}$' then u.last_service_date::date
            when u.last_service_date ~ '^\d{2}\.\d{2}\.\d{4}$' then to_date(u.last_service_date, 'DD.MM.YYYY')
            when u.last_service_date ~ '^\d{2}-\d{2}-\d{4}$' then to_date(u.last_service_date, 'DD-MM-YYYY')
            when u.last_service_date ~ '^\d{4}-\d{2}-\d{2}T' then to_timestamp(
                u.last_service_date,
                'YYYY-MM-DD"T"HH24:MI:SS'
            )::date
            else null
        end as parsed_date,
        regexp_replace(u.mileage, '[^0-9,]', '', 'g') as mileage_clean,
        regexp_replace(u.service_cost, '[^0-9,\.]', '', 'g') as cost_clean
    from s_psql_dds.t_sql_source_unstructured u
)
insert into s_psql_dds.t_sql_source_structured (
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
        service_cost
    )
select s.id as src_id,
    case
        when s.car_id ~ '^\d+$' then s.car_id::bigint
        else null
    end as car_id,
    nullif(s.owner_name, '')::text as owner_name,
    nullif(s.brand, '')::text as brand,
    nullif(s.model, '')::text as model,
    case
        when s.year ~ '^\d{4}' then substring(
            s.year
            from '^\d{4}'
        )::int
        else null
    end as year,
    case
        when upper(s.vin) ~ '^[A-HJ-NPR-Z0-9]{17}$' then upper(s.vin)
        else null
    end as vin,
    case
        when s.mileage_clean ~ '^\d+([\,\.]\d+)?$' then replace(s.mileage_clean, ',', '.')::numeric
        else null
    end as mileage,
    s.parsed_date as last_service_date,
    nullif(s.issue_description, '')::text as issue_description,
    case
        when s.cost_clean ~ '^\d+([\,\.]\d+)?$' then replace(s.cost_clean, ',', '.')::numeric
        else null
    end as service_cost
from src s
where s.parsed_date is not null
    and s.parsed_date between start_date and end_date;
end;
$$;
drop function s_psql_dds.fn_etl_data_load;
select s_psql_dds.fn_etl_data_load(date '2023-01-01', date '2025-12-31');