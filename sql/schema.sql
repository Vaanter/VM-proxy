create table upstream(
    connection integer,
    data blob,
    was_read integer
);

create index up on upstream(connection, was_read);

create table downstream(
    connection integer,
    data blob,
    was_read integer
);

create index down on downstream(connection, was_read);

create table connections(
    connection integer primary key autoincrement,
    used integer default 0
);
