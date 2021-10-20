-- auto-generated definition
create table Geography
(
    country_ioc  varchar(3)  not null
        constraint PK_Geograpy
            primary key
        constraint FK_Geography_Geography1
            references Geography,
    country_name varchar(20) not null,
    continent    varchar(20) not null,
    language     varchar(20)
)
go




-- auto-generated definition
create table Player
(
    player_id     int        not null
        constraint PK_Player2
            primary key,
    country_id    varchar(3) not null
        constraint FK_Player_Geography
            references Geography,
    name          varchar(40),
    sex           varchar(6),
    hand          varchar,
    year_of_birth smallint
)
go


-- auto-generated definition
create table Tournament
(
    tourney_id         varchar(50) not null
        constraint PK_Tournament
            primary key,
    date_id            varchar(10) not null,
    tourney_name       varchar(56),
    surface            varchar(7),
    draw_size          tinyint     not null,
    tourney_level      varchar(3)  not null,
    tourney_spectators smallint    not null,
    tourney_revenue    float       not null
)
go



-- auto-generated definition
create table Match
(
    match_id           int         not null
        constraint PK_Match
            primary key,
    tourney_id         varchar(50) not null
        constraint FK_Match_Tournament
            references Tournament,
    winner_id          int         not null
        constraint FK_Match_winer
            references Player,
    loser_id           int         not null
        constraint FK_Match_loser
            references Player,
    score              varchar(31) not null,
    best_of            int,
    round              varchar(4),
    minutes            float,
    w_ace              int,
    w_df               int,
    w_svpt             int,
    w_1stIn            int,
    w_1stWon           int,
    w_2ndWon           int,
    w_SvGms            int,
    w_bpSaved          int,
    w_bpFaced          int,
    l_ace              int,
    l_df               int,
    l_svpt             int,
    l_1stIn            int,
    l_1stWon           int,
    l_2ndWon           int,
    l_SvGms            int,
    l_bpSaved          int,
    l_bpFaced          int,
    winner_rank        int,
    winner_rank_points int,
    loser_rank         int,
    loser_rank_points  int
)
go

