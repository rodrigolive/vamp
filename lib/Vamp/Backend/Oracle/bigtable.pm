package Vamp::Backend::Oracle::kv;
use Mouse;
use Try::Tiny;
use DBD::Oracle qw(:ora_types);
use base 'Vamp::Database';
use constant DEBUG => Vamp::DEBUG();

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
        where ${db_name}_obj.id=${db_name}_kv.oid and key = ? and value like ?";
    my $sth = $self->dbh->prepare( $query ); 
    my $st = bless {
        db    => "$self",
        sth   => $sth,
        query => $query
    }, 'DBIx::Simple::Statement';
    $__PACKAGE__::statements{$self}{$st} = $st;
    
    $sth->bind_param(1, $args{k} );
    $sth->bind_param(2, $args{v} );
    $sth->execute;
    return bless { st => $st, lc_columns => $self->{lc_columns} }, $self->{result_class};
}

sub build_query_findall {
    my ($self, $collname, $where, $opts ) = @_;
    my $db_name = $self->{db_name};
    #my $query_head = "SELECT DISTINCT oid FROM ${db_name}_kv ";
    #my $coll_match = qq{ AND EXISTS ( select 1 from ${db_name}_obj vamp3
    #    where vamp3.collection='$collname' and vamp3.id=vamp_kv.oid ) };
    $where = $self->_flatten_as_hash( $where );

    my $hint = $opts->{hint};

    my %pivot_cols;
    my @order_by;
    # where?
    for my $key ( keys %$where ) {
        next unless length $key;
        $pivot_cols{ $key } = ();
    }
    # order_by ?
    if( my @order_by_param =
        ref $opts->{order_by} eq 'ARRAY' ? @{$opts->{order_by}} : ( $opts->{order_by} ) ) {
        for my $order_by_column ( @order_by_param ) {
            next unless defined $order_by_column;
            my $quoted = qq{"$order_by_column"};
            # cast type on hint?
            if( my $type = $hint->{ $order_by_column } ) {
                $quoted = "to_number( $quoted )" if $type eq 'number';
            }
            # add to order by
            push @order_by, $quoted; 
            # add to pivot column select
            $quoted =~ s{ |DESC|ASC}{}gi;
            $pivot_cols{ $order_by_column } = ()
                unless exists $pivot_cols{ $order_by_column };
        }
    }

    # TODO select?

    my $where_quoted = $self->_quote_keys( $where );
    my ( $wh, @binds ) = keys %$where ? $self->_abstract( $where_quoted ) : ('WHERE 1=1');
    #warn $wh;
    my $pivots = join ',' => qw/oid/,
        map {
            qq{max( case when key='$_' then to_char(value) else '' end) as "$_"};
        } keys %pivot_cols;
    my $selects = join ',' => qw/kv2.oid kv2.key kv2.value kv2.datatype/, map { qq{"$_"} } keys %pivot_cols;
    my $order_by_pivot = @order_by ? 'order by ' . join ',', @order_by : '';
    my $order_by = join ',', @order_by, 'kv2.oid', 'key', 'kv2.id';
    
    my $sql = qq{
            select $pivots
            from vamp_kv kv,vamp_obj obj
            where kv.oid = obj.id and obj.collection=?
            group by oid
            $order_by_pivot
    };
    # limit? (0 indexed)
    my $start = $opts->{start};
    my $limit = $opts->{limit};
    $sql = do {
        #my $page_num = int( $start / $limit ) + 1 ;
        my $limit_sql = $limit ? "where rownum < " . ( $limit + $start ) : "";
        my $start_sql = $start ? "where rownum__ >= $start" : "";
        qq{
            select * from (
                select m__.*, rownum rownum__ from ( $sql ) m__
                $limit_sql
            ) $start_sql
        }
    } if defined $start || defined $limit;

    $sql = qq{ 
        select $selects from (
            $sql
        ) pivot, vamp_kv kv2
        $wh
        and kv2.oid = pivot.oid
        order by $order_by
    };
    DEBUG && warn $sql;
    #warn $sql;
    [ $sql, $collname, @binds ];
}

sub query_findall {
    my $self = shift;
    $self->query( $self->build_query_findall( @_ ) );
}

sub query_find_one {
    my $self = shift;
    my ( $sql, @binds ) = $self->build_query_findall( @_ );
    $sql = "SELECT * FROM ( $sql ) WHERE rownum = 1";
    $self->query( $sql, @binds );
}

around _abstract => sub {
    my $orig = shift;
    my $self = shift;
    my ($where, @binds ) = $self->$orig( @_ );
    # oracle needs this for clobs:
    $where =~ s{value =}{value LIKE}g;
    ( $where, @binds );
};

around drop_database => sub {
    my $orig = shift;
    my $self = shift;

    # oracle specific
    my $db_name = $self->{db_name};
    eval { $self->query("drop trigger ${db_name}_obj_tr") };
    eval { $self->query("drop trigger ${db_name}_kv_tr") };
    eval { $self->query("drop sequence ${db_name}_obj_seq") };
    eval { $self->query("drop sequence ${db_name}_kv_seq") };
    eval { $self->query("drop index ${db_name}_values") };
    eval { $self->query("drop index ${db_name}_keys") };

    # drop the rest
    $self->$orig( @_ );
};

sub deploy { 
    my $self = shift;
    my $db_name = $self->{db_name};
    $self->dbh->{LongReadLen} = 1024 * 1024; 
    $self->dbh->{LongTruncOk} = 1;
    try { $self->query("select count(*) from ${db_name}_obj") }
    catch {
        $self->query(qq{create table $self->{db_name}_obj (
            id varchar(1024) primary key,
            collection varchar(1024),
            document CLOB
        )});
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
    try { $self->query("select count(*) from ${db_name}_kv") }
    catch {
        $self->query(qq{create table $self->{db_name}_kv (
            id integer primary key, 
            oid varchar(1024),
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
            oid varchar(1024),
            id1 varchar(1024),
            id2 varchar(1024),
            edge_name varchar(1024),
            foreign key(id1) references ${db_name}_obj(id) on delete cascade,
            foreign key(id2) references ${db_name}_obj(id) on delete cascade
            )
        });
    };
    $self->query("create index $self->{db_name}_values on $self->{db_name}_kv (to_char(value))");
    $self->query("create index $self->{db_name}_keys on $self->{db_name}_kv (key)");
}

1;



