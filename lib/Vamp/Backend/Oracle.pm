package Vamp::Backend::Oracle;
use Mouse;
use Try::Tiny;
use DBD::Oracle qw(:ora_types);
use base 'Vamp::Database';

sub drop {
    my $self = shift;
    for my $table( map { $self->{db_name} . $_ } qw/_kv _obj _rel/ ) {
        try { $self->query( qq{drop table $table} ) };
        try { $self->query( qq{drop trigger ${table}_tr } ) };
        try { $self->query( qq{drop sequence ${table}_seq } ) };
    }
}

sub last_insert_id {
    my $self = shift;
    my @v = $self->query("select $self->{db_name}_obj_seq.currval from dual")->list;
    return $v[0];
}

sub query_find_id {
    my ($self, %args) = @_;
    my $db_name = $self->{db_name};
    my $query = "select distinct ${db_name}_obj.id from ${db_name}_obj, ${db_name}_kv
        where ${db_name}_obj.id=${db_name}_kv.oid and key = ? ";
    # truncate the data in chunks 
    my @parts = unpack '(a2000)*', $args{v};
    for my $idx (0..$#parts) {
          $query .= sprintf (
            ' AND UTL_RAW.CAST_TO_VARCHAR2(RAWTOHEX(DBMS_LOB.SUBSTR(value, 2000, %d))) = ?',
            ($idx*2000 + 1)
          );
    }
    
    my $sth = $self->dbh->prepare( $query ); 
    my $st = bless {
        db    => "$self",
        sth   => $sth,
        query => $query
    }, 'DBIx::Simple::Statement';
    $__PACKAGE__::statements{$self}{$st} = $st;
    
    $sth->bind_param(1, $args{k} );
    for my $idx (0..$#parts) {
        $sth->bind_param(2 + $idx, $parts[$idx], { dbd_attrs=>undef } );
    }
    $sth->execute;
    return bless { st => $st, lc_columns => $self->{lc_columns} }, $self->{result_class};
}

#TODO turn this into oracle clob syntax
sub query_findall {
    my ($self, $collname, $where ) = @_;
    my $db_name = $self->{db_name};
    my $query_head = "SELECT DISTINCT oid FROM ${db_name}_kv ";
    my @sqls;
    my @all_binds;
    my @wh = $self->_flatten( $where );
    for my $wh ( @wh ) {
        my ( $where, @binds ) = $self->_abstract( $wh );
        my $sql = $query_head . $where;
        push @sqls,      $sql;
        push @all_binds, @binds;
    }
    my $from = join ' INTERSECT ', @sqls;
    my $sql = "SELECT DISTINCT oid FROM ( $from ) vamp2 WHERE vamp1.oid = vamp2.oid ";
    $sql = "SELECT oid,key,value FROM ${db_name}_kv vamp1 WHERE EXISTS ( $sql ) ORDER BY vamp1.oid"; 
    #warn $sql;
    #warn Dump $self->query( $sql, @all_binds )->hashes;
    $self->query( $sql, @all_binds );
}

around '_abstract' => sub {
    my $orig = shift;
    my $self = shift;
    my ($where, @binds ) = $self->$orig( @_ );
    # oracle needs this for clobs:
    $where =~ s{value =}{value LIKE}g;
    ( $where, @binds );
};

sub deploy { 
    my $self = shift;
    my $db_name = $self->{db_name};
    $self->dbh->{LongReadLen} = 1024 * 1024; 
    $self->dbh->{LongTruncOk} = 1;
    try { $self->query("select count(*) from ${db_name}_obj") }
    catch {
        $self->query(qq{create table $self->{db_name}_obj (
            id integer primary key,
            collection varchar(1024)
        )});
    };
    try { $self->query("select count(*) from ${db_name}_kv") }
    catch {
        $self->query(qq{create table $self->{db_name}_kv (
            id integer primary key, 
            oid integer,
            seq integer,
            datatype varchar(1024),
            key varchar(1024),
            value CLOB,
            version integer,
            foreign key(oid) references ${db_name}_obj(id) on delete cascade
        )});
        $self->query("create sequence ${db_name}_kv_seq");
        $self->query("create trigger ${db_name}_kv_tr 
            BEFORE INSERT ON ${db_name}_kv
            REFERENCING NEW AS NEW OLD AS OLD
            FOR EACH ROW
            BEGIN
               SELECT ${db_name}_kv_seq.NEXTVAL INTO :NEW.ID FROM dual;
            END;
        ");
    };
    try { $self->query("select count(*) from $self->{db_name}_rel") }
    catch {
        $self->query(qq{create table ${db_name}_rel (
            id1 integer,
            id2 integer,
            foreign key(id1) references ${db_name}_obj(id) on delete cascade,
            foreign key(id2) references ${db_name}_obj(id) on delete cascade
            )
        });
        $self->query("create sequence ${db_name}_obj_seq");
        $self->query("create trigger ${db_name}_obj_tr
            BEFORE INSERT ON ${db_name}_obj
            REFERENCING NEW AS NEW OLD AS OLD
            FOR EACH ROW
            BEGIN
               SELECT ${db_name}_obj_seq.NEXTVAL INTO :NEW.ID FROM dual;
            END;
        ");
    };
    #$self->query("create index $self->{db_name}_values on $self->{db_name}_kv (value)");
}

1;

