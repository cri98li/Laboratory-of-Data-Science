create table Date
(
	date_id varchar(10) not null
		constraint PK_Date
			primary key,
	year smallint not null,
	month varchar(20) not null,
	day varchar(20) not null,
	quarter tinyint not null,
	day_of_month tinyint not null,
	week_of_year tinyint not null,
	month_of_year tinyint not null
)
go

create table Geography
(
	country_ioc varchar(3) not null
		constraint PK_Geograpy
			primary key,
	country_name varchar(32) not null,
	continent varchar(7) not null,
	language varchar(20)
)
go

create table Player
(
	player_id int not null
		constraint PK_Player2
			primary key,
	country_id varchar(3) not null
		constraint FK_Player_Geography
			references Geography,
	name varchar(50) not null,
	sex varchar(6) not null,
	hand varchar not null,
	year_of_birth smallint not null,
	ht int not null
)
go

create table Tournament
(
	tourney_pk varchar(150) not null
		constraint PK_Tournament
			primary key,
	date_id varchar(10) not null
		constraint FK_Tournament_Date
			references Date,
	tourney_name varchar(56) not null,
	surface varchar(7) not null,
	draw_size tinyint not null,
	tourney_level varchar(3) not null,
	tourney_spectators smallint not null,
	tourney_revenue float not null,
	tourney_id varchar(50) not null
)
go

create table Match
(
	match_id varchar(150) not null
		constraint Match_pk
			primary key nonclustered,
	tourney_id varchar(150) not null
		constraint FK_Match_Tournament
			references Tournament,
	winner_id int not null
		constraint FK_Match_winer
			references Player,
	loser_id int not null
		constraint FK_Match_loser
			references Player,
	score varchar(31) not null,
	best_of int not null,
	round varchar(4) not null,
	minutes float not null,
	w_ace int not null,
	w_df int not null,
	w_svpt int not null,
	w_1stIn int not null,
	w_1stWon int not null,
	w_2ndWon int not null,
	w_SvGms int not null,
	w_bpSaved int not null,
	w_bpFaced int not null,
	l_ace int not null,
	l_df int not null,
	l_svpt int not null,
	l_1stIn int not null,
	l_1stWon int not null,
	l_2ndWon int not null,
	l_SvGms int not null,
	l_bpSaved int not null,
	l_bpFaced int not null,
	winner_rank int not null,
	winner_rank_points int not null,
	loser_rank int not null,
	loser_rank_points int not null
)
go

create unique index Match_match_id_uindex
	on Match (match_id)
go

