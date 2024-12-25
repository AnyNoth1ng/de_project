--STG таблицы 
CREATE TABLE public.penn_stg_accounts (
	account_num	varchar(20) NULL,
	valid_to	date NULL,
	client 		varchar(10) NULL,
	create_dt	timestamp(0) NULL,
	update_dt	timestamp(0) NULL
);

CREATE TABLE public.penn_stg_clients (
	client_id			varchar(10) NULL,
	last_name			varchar(20) NULL,
	first_name			varchar(20) NULL,
	patronymic			varchar(20) NULL,
	date_of_birth		date NULL,
	passport_num 		varchar(15) NULL,
	passport_valid_to 	date NULL,
	phone 				varchar(16) NULL,
	create_dt			timestamp(0) NULL,
	update_dt			timestamp(0) NULL
);

CREATE TABLE public.penn_stg_cards (
	card_num	varchar(20) NULL,
	account_num	varchar(20) NULL,
	create_dt	timestamp(0) NULL,
	update_dt	timestamp(0) NULL
);

CREATE TABLE public.penn_stg_blacklist (
	"date"		date NULL,
	passport	varchar(20) null
);

CREATE TABLE public.penn_stg_terminals (
	terminal_id 		varchar(10) null,
	terminal_type 		varchar(20) null,
	terminal_city 		varchar(100) null,
	terminal_address 	varchar(200) null
);

CREATE TABLE public.penn_stg_transactions (
	transaction_id 		int8 null,
	transaction_date 	varchar(20) null,
	amount		 		float4 null,
	card_num		 	varchar(20) null,
	oper_type			varchar(10) null,
	oper_result			varchar(10) null,
	terminal			varchar(10) null
);

--DWH таблицы с SCD2

CREATE TABLE public.penn_dwh_dim_clients_hist (
	client_id			varchar(10) null,
	last_name			varchar(20) NULL,
	first_name			varchar(20) NULL,
	patronymic			varchar(20) NULL,
	date_of_birth		date NULL,
	passport_num 		varchar(15) NULL,
	passport_valid_to 	date NULL,
	phone 				varchar(16) NULL,
	effective_from		timestamp(0) null,
	effective_to		timestamp(0) null,
	deleted_flg			bool
);


CREATE TABLE public.penn_dwh_dim_accounts_hist (
	account_num		varchar(20) null,
	valid_to		date NULL,
	client 			varchar(10) NULL,
	effective_from	timestamp(0) null,
	effective_to	timestamp(0) null,
	deleted_flg		bool

);

CREATE TABLE public.penn_dwh_dim_cards_hist (
	card_num		varchar(20) null,
	account_num		varchar(20) NULL,
	effective_from	timestamp(0) null,
	effective_to	timestamp(0) null,
	deleted_flg		bool

);

CREATE TABLE public.penn_dwh_dim_terminals_hist (
	terminal_id 		varchar(10) null,
	terminal_type 		varchar(20) null,
	terminal_city 		varchar(100) null,
	terminal_address 	varchar(200) null,
	effective_from		timestamp(0) null,
	effective_to		timestamp(0) null,
	deleted_flg			bool
);


CREATE TABLE public.penn_dwh_fact_transactions (
	trans_id 	varchar null,
	trans_date 	timestamp null,
	card_num	varchar(20) null,
	oper_type	varchar(10) null,
	amt			decimal null,
	oper_result	varchar(10) null,
	terminal	varchar(10) null

);

CREATE TABLE public.penn_dwh_fact_passport_blacklist (
	entry_dt date NULL,
	passport varchar(20) null
);

--REP таблица

CREATE TABLE public.penn_rep_fraud (
	event_dt	date NULL,
	passport	varchar(20) null,
	fio			varchar(100) null,
	phone		varchar(16) NULL,
	event_type	varchar(50) null,
	report_dt	date NULL
);



