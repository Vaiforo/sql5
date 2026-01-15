create or replace function s_psql_dds.fn_generate_unstructured(p_rows int)
returns void
language sql
as
$$
insert into s_psql_dds.t_sql_source_unstructured (
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
select
    case
        when random() < 0.05 then null
        when random() < 0.10 then 'ID-' || (100000 + gs)::text
        when random() < 0.10 then (100000 + gs)::text || ' '
        else (100000 + gs)::text
    end as car_id,

    case
        when random() < 0.05 then null
        when random() < 0.05 then ''
        when random() < 0.03 then 'NaN'
        else (array[
            'Иван Петров','Олег Смирнов','Анна Иванова','Мария Кузнецова','Дмитрий Соколов',
            'Алексей Попов','Екатерина Морозова','Никита Волков','Сергей Васильев','Виктория Фёдорова',
            'Павел Михайлов','Елена Новикова','Артём Захаров','Татьяна Орлова','Максим Павлов',
            'Юлия Макарова','Илья Егоров','Анастасия Никитина','Кирилл Андреев','Дарья Романова',
            'Владимир Козлов','Полина Алексеева','Роман Белов','Оксана Сергеева','Вячеслав Гусев',
            'Наталья Зайцева','Михаил Тарасов','Алина Борисова','Денис Фролов','Ирина Семёнова',
            'Константин Воробьёв','Евгения Сидорова','Антон Куликов','Светлана Яковлева','Андрей Григорьев',
            'Маргарита Данилова','Руслан Крылов','Ксения Мельникова','Олег Пантелеев','Вероника Лебедева',
            'Станислав Кузьмин','Алёна Фомина','Ярослав Чернов','Людмила Широкова','Валентин Осипов',
            'Елизавета Котова','Пётр Савельев','Надежда Исакова','Георгий Афанасьев','Полина Устинова'
        ])[1 + floor(random()*10)::int]
    end as owner_name,

    case
        when random() < 0.03 then ''
        when random() < 0.02 then null
        else (array[
            'Toyota','BMW','Lada','Kia','Hyundai','Mercedes','Volkswagen','Audi','Skoda','Renault',
            'Ford','Nissan','Mazda','Honda','Chevrolet','Peugeot','Citroen','Opel','Subaru','Mitsubishi'
        ])[1 + floor(random()*10)::int]
    end as brand,

    case
        when random() < 0.03 then null
        when random() < 0.05 then '???'
        else (array[
            'Camry','Corolla','X5','X3','Vesta','Granta','Rio','Solaris','C-Class','Passat',
            'A4','Octavia','Duster','Logan',
            'Rav4','Land Cruiser','Supra','Highlander','Polo','Golf','Tiguan','Touareg','Jetta','Arteon',
            'Q3','Q5','Q7','A6','A8','RS6',
            'E-Class','S-Class','GLC','GLE','Sprinter','Vito','CLS','CLA',
            'CX-5','CX-30','Mazda6','Mazda3','Outlander','ASX','Pajero','Forester'
        ])[1 + floor(random()*14)::int]
    end as model,

    case
        when random() < 0.03 then null
        when random() < 0.03 then '20O5'                 
        when random() < 0.20 then (1995 + floor(random()*31)::int)::text || ' г.'
        else (1995 + floor(random()*31)::int)::text
    end as year,

    case
        when random() < 0.05 then null
        when random() < 0.10 then upper(substr(md5(random()::text || gs::text), 1, 16))  
        when random() < 0.10 then upper(substr(md5(random()::text || gs::text), 1, 17)) || 'Z' 
        when random() < 0.10 then upper(replace(substr(md5(random()::text || gs::text), 1, 17), 'A', 'I')) 
        when random() < 0.10 then '***' || upper(substr(md5(random()::text || gs::text), 1, 14)) 
        else upper(substr(md5(random()::text || gs::text), 1, 17))
    end as vin,

    case
        when random() < 0.05 then null
        when random() < 0.05 then ''
        when random() < 0.03 then 'NaN'
        when random() < 0.20 then (5000 + floor(random()*295000)::int)::text || ' km'
        when random() < 0.20 then regexp_replace((5000 + floor(random()*295000)::int)::text, '(\d)(?=(\d{3})+$)', '\1 ', 'g')
        when random() < 0.15 then (50 + floor(random()*250)::int)::text || 'k'
        else (5000 + floor(random()*295000)::int)::text
    end as mileage,

    case
        when random() < 0.03 then null
        when random() < 0.03 then '32.13.2024'
        else (
            case (floor(random()*4)::int)
                when 0 then to_char((date '2021-01-01' + (floor(random()*1800)::int)), 'YYYY-MM-DD')
                when 1 then to_char((date '2021-01-01' + (floor(random()*1800)::int)), 'DD.MM.YYYY')
                when 2 then to_char((date '2021-01-01' + (floor(random()*1800)::int)), 'DD-MM-YYYY')
                else to_char((timestamp '2021-01-01' + (random() * interval '1800 days')), 'YYYY-MM-DD"T"HH24:MI:SS')
            end
        )
    end as last_service_date,

    case
        when random() < 0.04 then null
        when random() < 0.04 then ''
        else (array[
            'стук в двигателе','не заводится','течёт масло','ошибка датчика кислорода',
            'brake squeal','engine misfire','перегрев','плохой запуск на холодную',
            'шум в подвеске','vibration at speed',
            'проблемы с коробкой передач','рывки при переключении','скрип тормозов',
            'посторонний шум при езде','горит чек двигателя','потеря мощности',
            'нестабильные обороты','утечка охлаждающей жидкости','перегрев двигателя',
            'проблемы с электроникой',
            'ABS error','battery drain','engine knocking','oil pressure low',
            'coolant leak','fuel pump failure','rough idle','suspension clunk',
            'brake pedal vibration','steering wheel shake'
        ])[1 + floor(random()*10)::int]
    end as issue_description,

    case
        when random() < 0.05 then null
        when random() < 0.05 then ''
        when random() < 0.03 then 'NaN'
        when random() < 0.20 then (1000 + floor(random()*90000)::int)::text || ' ₽'
        when random() < 0.20 then regexp_replace((1000 + floor(random()*90000)::int)::text, '(\d)(?=(\d{3})+$)', '\1 ', 'g') || ' руб.'
        when random() < 0.20 then (1000 + floor(random()*90000)::int)::text || ',00'
        when random() < 0.15 then '$' || (50 + floor(random()*950)::int)::text
        else (1000 + floor(random()*90000)::int)::text
    end as service_cost

from generate_series(1, p_rows) as gs;
$$;
select s_psql_dds.fn_generate_unstructured(100000);