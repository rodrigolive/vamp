package Vamp::Backend::SQLite;
use strict;
use warnings;
use Try::Tiny;
use base 'Vamp::Database';

sub drop {
    my $self = shift;
    for my $table( map { $self->{db_name} . $_ } qw/_kv _obj _rel/ ) {
        try { $self->query( qq{drop table $table} ) };
    }
}

sub deploy { 
    my $self = shift;
    $self->query(q{PRAGMA foreign_keys = ON});
    try { $self->query("select count(*) from $self->{db_name}_obj") }
    catch {
        $self->query(qq{create table $self->{db_name}_obj (
            id integer primary key,
            collection text
        )});
    };
    try { $self->query("select count(*) from $self->{db_name}_kv") }
    catch {
        $self->query(qq{create table $self->{db_name}_kv (
            id integer primary key, 
            oid integer,
            seq integer,
            datatype text,
            key text,
            value text,
            version integer,
            foreign key(oid) references $self->{db_name}_obj(id) on delete cascade
        )});
        $self->query("create index $self->{db_name}_kv_values on $self->{db_name}_kv (value)");
    };
    try { $self->query("select count(*) from $self->{db_name}_rel") }
    catch {
        $self->query(qq{create table $self->{db_name}_rel (
            id1 integer,
            id2 integer,
            foreign key(id1) references $self->{db_name}_obj(id) on delete cascade,
            foreign key(id2) references $self->{db_name}_obj(id) on delete cascade
            )
        });
    };
}

1;
