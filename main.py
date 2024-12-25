import pandas as pd
import psycopg2
import datetime as dt
import os
import shutil
from datetime import datetime

def log_dt():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

user = ''
password = ''

connection = {
    'user': f'{user}',
    'password': f'{password}',
    'host': "rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net",
    'port': "6432",
    'database': "db"
}
print(f'Скрипт начал работать: {log_dt()}')
print(f'посик файлов по наименованию')

path = os.getcwd()
filenames = ('terminals_', 'transactions_', 'passport_blacklist_')
files_dir = [file for file in os.listdir(path) if file.startswith(filenames)]
files_dt = sorted(list(set([datetime.strptime(dt.split('.')[0][-8:], "%d%m%Y").date() for dt in files_dir])))

if files_dt == []:
    print('Даты для загрузки не найдены')

for teq_dt in files_dt:
    dt_sql = datetime.strftime(teq_dt, "%Y-%m-%d")
    print(f'данные за: {dt_sql}')

    dt_of_file = datetime.strftime(teq_dt, "%d%m%Y")
    # считываем исходники
    passport_blacklist_excel = pd.read_excel(f'passport_blacklist_{dt_of_file}.xlsx')
    terminals_excel = pd.read_excel(f'terminals_{dt_of_file}.xlsx')
    transactions_excel = pd.read_csv(f'transactions_{dt_of_file}.txt', sep=';')
    transactions_excel['amount'] = transactions_excel['amount'].apply(lambda x: x.replace(',', '.')).astype('float')

    print(f'данные прочитаны {log_dt()}')

# Загружаем в STG
    conn = psycopg2.connect(database=connection['database'],
                                host=connection['host'],
                                user=connection['user'],
                                password=connection['password'],
                                port=connection['port'])

    conn.autocommit = False
    print(f'Подключился к бд {log_dt()}')
    curs = conn.cursor()
    curs.execute(
        # Если в DIM нет записей из таблиц STG
        f''' 
            truncate table public.penn_stg_accounts;
            truncate table public.penn_stg_clients;
            truncate table public.penn_stg_cards;
            truncate table public.penn_stg_blacklist;
            truncate table public.penn_stg_terminals;
            truncate table public.penn_stg_transactions;
    ''')
    conn.commit()

    curs.execute(
        f'''
            insert into public.penn_stg_accounts (account_num,valid_to,client,create_dt,update_dt)
            select account as account_num,valid_to,client,create_dt,update_dt from info.accounts;
    
            insert into public.penn_stg_clients (client_id,last_name,first_name,patronymic,date_of_birth,passport_num,passport_valid_to,phone,create_dt,update_dt)
            select client_id,last_name,first_name,patronymic,date_of_birth,passport_num,passport_valid_to,phone,create_dt,update_dt from info.clients;
    
            insert into public.penn_stg_cards (card_num,account_num,create_dt,update_dt)
            select card_num,account as account_num,create_dt,update_dt from info.cards;
    '''
    )
    conn.commit()

    curs.executemany("INSERT INTO public.penn_stg_blacklist(date,passport) VALUES( %s, %s )",
                           passport_blacklist_excel.values.tolist())
    conn.commit()

    curs.executemany("INSERT INTO public.penn_stg_terminals(terminal_id,terminal_type,terminal_city,terminal_address) VALUES(%s,%s,%s,%s)",
            terminals_excel.values.tolist())
    conn.commit()

    curs.executemany("INSERT INTO public.penn_stg_transactions(transaction_id,transaction_date,amount,card_num,oper_type,oper_result,terminal) VALUES(%s,%s,%s,%s,%s,%s,%s)",
            transactions_excel.values.tolist())

    conn.commit()

    print(f'Данные загружены в STG \t/ {log_dt()}')
    # Загрузка данных в DIM и Fact
    curs.execute(f'''
        ---запросы для clients
        ------------------------------------------
        -- 1) В DIM нет данных из STG
        ------------------------------------------
        with t0 as(
                select stg.*, dim.mrk
                from public.penn_stg_cards as stg
                left join (select card_num,1 as mrk from public.penn_dwh_dim_cards_hist where deleted_flg is false) as dim on stg.card_num=dim.card_num
            )
            insert into public.penn_dwh_dim_cards_hist(card_num,account_num,effective_from,effective_to,deleted_flg)
            select card_num,account_num,
                create_dt as effective_from, '3000-01-01' as effective_to, false as deleted_flg
            from t0 where mrk is null;
            ------------------------------------------
            -- 2) в STG отсутствуют данные из DIM
            ------------------------------------------
            with t0 as(
                select dim.*, stg.mrk
                from (select card_num from public.penn_dwh_dim_cards_hist where deleted_flg is false) as dim
                left join (select card_num,1 as mrk from public.penn_stg_cards) as stg on stg.card_num=dim.card_num
                where stg.mrk is null
            )
            update public.penn_dwh_dim_cards_hist
            set deleted_flg = true, effective_to = '{dt_sql}' where card_num in (select card_num from t0);
            ------------------------------------------
            -- 3) в STG есть изменения данных из DIM
            ------------------------------------------
            insert into public.penn_dwh_dim_cards_hist(card_num,account_num,effective_from,effective_to,deleted_flg)
            select card_num,account_num,
                create_dt as effective_from, '3000-01-01' as effective_to, false as deleted_flg
            from public.penn_stg_cards where update_dt is not null;
            update public.penn_dwh_dim_cards_hist
            set effective_to = t_n.effective_to
            from(
                select card_num,account_num,effective_from,
                lead(effective_from,1,'3000-01-01') over(partition by card_num order by effective_from) as effective_to,deleted_flg
                from public.penn_dwh_dim_cards_hist) as t_n
            where public.penn_dwh_dim_cards_hist.card_num = t_n.card_num
                and public.penn_dwh_dim_cards_hist.effective_from = t_n.effective_from;
    ''')
    conn.commit()

    curs.execute(f'''
        -- запросы для accounts
        ------------------------------------------
        -- 1) В DIM нет данных из STG
        ------------------------------------------
            with t0 as(
        	select stg.*, dim.mrk
        	from public.penn_stg_clients as stg
        	left join (select client_id,1 as mrk from public.penn_dwh_dim_clients_hist where deleted_flg is false) as dim on stg.client_id=dim.client_id
        )
        insert into public.penn_dwh_dim_clients_hist(client_id,last_name,first_name,patronymic,date_of_birth,passport_num,passport_valid_to,phone,effective_from,effective_to,deleted_flg)
        select client_id,last_name,first_name,patronymic,date_of_birth,passport_num,passport_valid_to,phone,
        	create_dt as effective_from, '3000-01-01' as effective_to, false as deleted_flg
        from t0 where mrk is null;
            ------------------------------------------
            -- 2) в STG отсутствуют данные из DIM
            ------------------------------------------
            with t0 as(
        	select dim.*, stg.mrk
        	from (select client_id from public.penn_dwh_dim_clients_hist where deleted_flg is false) as dim
        	left join (select client_id,1 as mrk from public.penn_stg_clients) as stg on stg.client_id=dim.client_id
        	where stg.mrk is null
        )
        update public.penn_dwh_dim_clients_hist
        set deleted_flg = true, effective_to = '{dt_sql}' where client_id in (select client_id from t0);
            ------------------------------------------
            -- 3) в STG есть изменения данных из DIM
            ------------------------------------------
            insert into public.penn_dwh_dim_clients_hist(client_id,last_name,first_name,patronymic,date_of_birth,passport_num,passport_valid_to,phone,effective_from,effective_to,deleted_flg)
        select client_id,last_name,first_name,patronymic,date_of_birth,passport_num,passport_valid_to,phone,
        	create_dt as effective_from, '3000-01-01' as effective_to, false as deleted_flg
        from public.penn_stg_clients where update_dt is not null;
        update public.penn_dwh_dim_clients_hist
        set effective_to = t_n.effective_to
        from(
        	select client_id,last_name,first_name,patronymic,date_of_birth,passport_num,passport_valid_to,phone,effective_from,
        	lead(effective_from,1,'3000-01-01') over(partition by client_id order by effective_from) as effective_to,deleted_flg
        	from public.penn_dwh_dim_clients_hist) as t_n
        where public.penn_dwh_dim_clients_hist.client_id = t_n.client_id
        	and public.penn_dwh_dim_clients_hist.effective_from = t_n.effective_from;
    ''')
    conn.commit()

    curs.execute(f'''
        -- запросы для cards
        ------------------------------------------
        --  1) В DIM нет данных из STG
        ------------------------------------------
            with t0 as(
                select stg.*, dim.mrk
                from public.penn_stg_cards as stg
                left join (select card_num,1 as mrk from public.penn_dwh_dim_cards_hist where deleted_flg is false) as dim on stg.card_num=dim.card_num
            )
            insert into public.penn_dwh_dim_cards_hist(card_num,account_num,effective_from,effective_to,deleted_flg)
            select card_num,account_num,
                create_dt as effective_from, '3000-01-01' as effective_to, false as deleted_flg
            from t0 where mrk is null;
            ------------------------------------------
            -- 2) в STG отсутствуют данные из DIM
            ------------------------------------------
            with t0 as(
                select dim.*, stg.mrk
                from (select card_num from public.penn_dwh_dim_cards_hist where deleted_flg is false) as dim
                left join (select card_num,1 as mrk from public.penn_stg_cards) as stg on stg.card_num=dim.card_num
                where stg.mrk is null
            )
            update public.penn_dwh_dim_cards_hist
            set deleted_flg = true, effective_to = '{dt_sql}' where card_num in (select card_num from t0);
            ------------------------------------------
            -- 3) в STG есть изменения данных из DIM
            ------------------------------------------
            insert into public.penn_dwh_dim_cards_hist(card_num,account_num,effective_from,effective_to,deleted_flg)
            select card_num,account_num,
                create_dt as effective_from, '3000-01-01' as effective_to, false as deleted_flg
            from public.penn_stg_cards where update_dt is not null;
            update public.penn_dwh_dim_cards_hist
            set effective_to = t_n.effective_to
            from(
                select card_num,account_num,effective_from,
                lead(effective_from,1,'3000-01-01') over(partition by card_num order by effective_from) as effective_to,deleted_flg
                from public.penn_dwh_dim_cards_hist) as t_n
            where public.penn_dwh_dim_cards_hist.card_num = t_n.card_num
                and public.penn_dwh_dim_cards_hist.effective_from = t_n.effective_from;
        ''')
    conn.commit()

    curs.execute(f'''
        -- запросы для terminals
        ------------------------------------------
        --  1) В DIM нет данных из STG
        ------------------------------------------
            with t0 as(
                select stg.*, dim.mrk
                from public.penn_stg_terminals as stg
                left join (select terminal_id,1 as mrk from public.penn_dwh_dim_terminals_hist where deleted_flg is false) as dim on stg.terminal_id=dim.terminal_id
            )
            insert into public.penn_dwh_dim_terminals_hist(terminal_id,terminal_type,terminal_city,terminal_address,effective_from,effective_to,deleted_flg)
            select terminal_id,terminal_type,terminal_city,terminal_address,
                '{dt_sql}' as effective_from, '3000-01-01' as effective_to, false as deleted_flg
            from t0 where mrk is null;
            ------------------------------------------
            -- 2) в STG отсутствуют данные из DIM
            ------------------------------------------
            with t0 as(
                select dim.*, stg.mrk
                from (select terminal_id from public.penn_dwh_dim_terminals_hist where deleted_flg is false) as dim
                left join (select terminal_id,1 as mrk from public.penn_stg_terminals) as stg on stg.terminal_id=dim.terminal_id
                where stg.mrk is null
            )
            update public.penn_dwh_dim_terminals_hist
            set deleted_flg = true, effective_to = '{dt_sql}' where terminal_id in (select terminal_id from t0);
            ------------------------------------------
            -- 3) в STG есть изменения данных из DIM
            ------------------------------------------
            with t0 as(
                select stg.*, dim.terminal_id as terminal_id_dim,dim.terminal_type as terminal_type_dim,dim.terminal_city as terminal_city_dim,dim.terminal_address as terminal_address_dim
                from public.penn_stg_terminals as stg
                left join public.penn_dwh_dim_terminals_hist as dim on stg.terminal_id=dim.terminal_id
            )
            insert into public.penn_dwh_dim_terminals_hist(terminal_id,terminal_type,terminal_city,terminal_address,effective_from,effective_to,deleted_flg)
            select terminal_id,terminal_type,terminal_city,terminal_address,
                '{dt_sql}' as effective_from, '3000-01-01' as effective_to, false as deleted_flg
            from t0 where
                terminal_id <> terminal_id_dim
                or terminal_type <> terminal_type_dim
                or terminal_city <> terminal_city_dim
                or terminal_address <> terminal_address_dim;
            update public.penn_dwh_dim_terminals_hist
            set effective_to = t_n.effective_to
            from(
                select terminal_id,terminal_type,terminal_city,terminal_address,effective_from,
                lead(effective_from,1,'3000-01-01') over(partition by terminal_id order by effective_from) as effective_to,deleted_flg
                from public.penn_dwh_dim_terminals_hist) as t_n
            where public.penn_dwh_dim_terminals_hist.terminal_id = t_n.terminal_id
                and public.penn_dwh_dim_terminals_hist.effective_from = t_n.effective_from;
    ''')
    conn.commit()

    curs.execute(f'''
            insert into public.penn_dwh_fact_transactions (trans_id,trans_date,card_num,oper_type,amt,oper_result,terminal)
            select transaction_id as trans_id,transaction_date::timestamp as trans_date,card_num,oper_type,amount as amt,oper_result,terminal
            from public.penn_stg_transactions;
    ''')
    conn.commit()

    curs.execute(f'''
            truncate table public.penn_dwh_fact_passport_blacklist;
            insert into public.penn_dwh_fact_passport_blacklist (entry_dt,passport)
            select "date" as entry_dt,passport from public.penn_stg_blacklist;
    ''')
    conn.commit()

    print(f'Данные загружены в DIM в: {log_dt()}')

    # создаём отчет
    curs.execute(f'''
        with t0 as(
                select
                    fct.trans_id,fct.trans_date::timestamptz,fct.card_num,fct.amt,fct.oper_result,
                    crd.account_num,
                    acc.valid_to,
                    cli.client_id,concat(cli.last_name,' ',cli.first_name,' ',cli.patronymic) as fio,cli.passport_num,cli.passport_valid_to,cli.phone,
                    trm.terminal_city,
                    min(trm.terminal_city) over(partition by fct.card_num ORDER BY trans_date RANGE BETWEEN '1 hour'::interval PRECEDING AND CURRENT ROW) as city_1,
                    max(trm.terminal_city) over(partition by fct.card_num ORDER BY trans_date RANGE BETWEEN '1 hour'::interval PRECEDING AND CURRENT ROW) as city_2,
                    blc.entry_dt as black_list_dt,
                    count(fct.amt) over(partition by fct.card_num ORDER BY trans_date RANGE BETWEEN '20 minute'::interval PRECEDING AND CURRENT ROW) as cnt_20_minute_trans,
                    lag(fct.amt, 1, null) over(partition by fct.card_num ORDER BY trans_date) as amt_lag1,
                    lag(fct.oper_result, 1, null) over(partition by fct.card_num ORDER BY trans_date) as amt_lag1_res,
                    lag(fct.amt, 2, null) over(partition by fct.card_num ORDER BY trans_date) as amt_lag2,
                    lag(fct.oper_result, 2, null) over(partition by fct.card_num ORDER BY trans_date) as amt_lag2_res
                from public.penn_dwh_fact_transactions as fct
                left join public.penn_dwh_dim_cards_hist as crd on fct.card_num = crd.card_num and crd.effective_to = '3000-01-01' and crd.deleted_flg is False
                left join public.penn_dwh_dim_accounts_hist as acc on crd.account_num = acc.account_num and acc.effective_to = '3000-01-01' and acc.deleted_flg is False
                left join public.penn_dwh_dim_clients_hist as cli on acc.client = cli.client_id and cli.effective_to = '3000-01-01' and cli.deleted_flg is False
                left join public.penn_dwh_dim_terminals_hist as trm on fct.terminal = trm.terminal_id and trm.effective_to = '3000-01-01' and trm.deleted_flg is False
                left join public.penn_dwh_fact_passport_blacklist blc on cli.passport_num = blc.passport
            ), t1 as(
            select *,
                case 
                    when black_list_dt is not null or passport_valid_to < trans_date then true else false
                end as mark_bad_passport,
                case 
                    when valid_to < trans_date then true else false
                end as mark_bad_contract,
                case 
                    when city_1 <> city_2 then true else false
                end as bad_trans1,
                case 
                    when cnt_20_minute_trans>3
                        and (amt<amt_lag1 and amt_lag1 < amt_lag2)
                        and (oper_result = 'SUCCESS' and amt_lag1_res = 'REJECT' and amt_lag2_res = 'REJECT') then true else false
                end as bad_trans2
            from t0
            )
            insert into public.penn_rep_fraud(event_dt,passport,fio,phone,event_type,report_dt)
            select distinct
                date_trunc('day', trans_date)::date as event_dt,
                passport_num as passport,
                fio,
                phone,
                case 
                    when mark_bad_passport then 'заблокированный паспорт'
                    when mark_bad_contract then 'недействующий договор'
                    when bad_trans1 then 'операций в разных городах за короткое время'
                    when bad_trans2 then 'попытка подбора суммы'
                end as event_type,
                '{dt_sql}'::date as report_dt
            from t1 where mark_bad_passport or mark_bad_contract or bad_trans1 or bad_trans2;
    ''')
    conn.commit()

    curs.close()
    conn.close()

    print(f'Отчёт REP сделан {log_dt()}')

    shutil.move(f"passport_blacklist_{dt_of_file}.xlsx",f"archive/passport_blacklist_{dt_of_file}.xlsx.backup")
    shutil.move(f"terminals_{dt_of_file}.xlsx", f"archive/terminals_{dt_of_file}.xlsx.backup")
    shutil.move(f"transactions_{dt_of_file}.txt", f"archive/transactions_{dt_of_file}.txt.backup")

    print(f'Файлы перемещены в архив {log_dt()}')

print(f'Скрипт отработал: {log_dt()}')

