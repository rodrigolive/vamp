package Vamp::Backend::SQLite;
use Mouse;
use Try::Tiny;
use base 'Vamp::Database';
use constant DEBUG => Vamp::DEBUG();

sub last_insert_id {
    my $self = shift;
    return $self->dbh->sqlite_last_insert_rowid;
}

sub build_query_find_id {
    my ($self, $collname, @ids ) = @_;
    my $db_name = $self->{db_name};
    my ($where, @binds) = $self->_abstract({ 'obj.id'=>\@ids });
    my $sql = "SELECT * FROM ${db_name}_kv kv,${db_name}_obj obj $where and obj.id=kv.oid order by oid";
    [ $sql, @binds ];
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
    if( my @order_by_param = ref $opts->{order_by} eq 'ARRAY'
                ? @{$opts->{order_by}}
                : ( $opts->{order_by} ) ) {
        for my $order_by_column ( @order_by_param ) {
            next unless defined $order_by_column;
            my $quoted = qq{"$order_by_column"};
            # XXX cast type on hint?
            if( my $type = $hint->{ $order_by_column } ) {
                $quoted = "cast( $quoted as number )" if $type eq 'number';
            }
            # add to order by
            push @order_by, $quoted; 
            # add to pivot column select
            $quoted =~ s{ |DESC|ASC}{}gi;
            $pivot_cols{ $order_by_column } = ()
                unless exists $pivot_cols{ $order_by_column };
        }
    }

    my $where_quoted = $self->_quote_keys( $where );
    my ( $wh, @binds ) = keys %$where ? $self->_abstract( $where_quoted ) : ('WHERE 1=1');
    #warn $wh;

    # select?
    my $select_filter_str;
    my $select_filter = $opts->{select} ? [ $self->_abstract( key => $opts->{select} ) ] : [];
    if( @$select_filter && $select_filter->[0] =~ s/WHERE/and/ig ) {
        push @binds, splice @{ $select_filter || [] },1;
        $select_filter_str = $select_filter->[0];
    } else {
        $select_filter_str = '';
    }

    my $pivots = join ',' => qw/oid/,
        map {
            qq{max( case when key='$_' then value else '' end) as "$_"};
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
        $limit ||= "9999999";
        defined $start
            ? "$sql LIMIT $start-1,$limit"
            : "$sql LIMIT $limit";
    } if defined $start || defined $limit;

    $sql = qq{ 
        select $selects from (
            $sql
        ) pivot, vamp_kv kv2
        $wh
        and kv2.oid = pivot.oid
        $select_filter_str
        order by $order_by
    };
    DEBUG && warn $sql;
    [ $sql, $collname, @binds ];
}

sub drop {
    my $self = shift;
    for my $table( map { $self->{db_name} . $_ } qw/_kv _obj _rel/ ) {
        try { $self->query( qq{drop table $table} ) };
    }
}

sub _insert_kv {
    my ($self, %args) = @_;
    my $db_name = $self->{db_name};
    $self->query( qq{INSERT INTO ${db_name}_kv ( oid, key, value, datatype, seq, version )
        VALUES (?,?,?,?,?,?) }, 
            $args{oid}, $args{key}, $args{value}, $args{datatype}, $args{seq}, $args{version} );
}

sub _update_kv {
    my ($self, %args) = @_;
    my $db_name = $self->{db_name};
    $self->query( qq{UPDATE ${db_name}_kv SET value = ? WHERE oid = ? AND key = ? }, $args{value}, $args{oid}, $args{key} );
}

sub _create {
    my ($self , %args ) = @_;
    my $db_name = $self->{db_name};
    if( exists $args{data}{id} && ( my $id = delete $args{data}{id} ) ) {
        if ( $args{serialize} ) {
            $self->query( qq{INSERT INTO ${db_name}_obj ( id, collection, document ) VALUES (?,?,?) },
                $id, $args{collection}, $self->_dump( $args{data} ) );
        } else {
            $self->query( qq{INSERT INTO ${db_name}_obj ( id, collection ) VALUES (?,?) },
                $id, $args{collection} );
        }
        return $id;
    } else {
        my $tempid = int(rand(999999999999) * $$ + time);
        if ( $args{serialize} && exists $args{data} ) {
            $self->query( qq{INSERT INTO ${db_name}_obj ( id, collection, document )
                VALUES ( ?,?,? ) },
                $tempid, $args{collection}, $self->_dump( $args{data} ) );
        } else {
            $self->query( qq{INSERT INTO ${db_name}_obj ( id, collection ) 
                VALUES (?,?) }, $tempid, $args{collection} );
        }
        my $id = $self->last_insert_id('','','','$self->{db_name}_obj');
        $self->query( qq{UPDATE ${db_name}_obj SET id=? WHERE id=?}, $id, $tempid );
        return $id;
    }
}

sub deploy { 
    my $self = shift;
    $self->query(q{PRAGMA foreign_keys = ON});
    try { $self->query("select count(*) from $self->{db_name}_obj") }
    catch {
        $self->query(qq{create table $self->{db_name}_obj (
            id text primary key,
            collection text,
            document text
        )});
    };
    try { $self->query("select count(*) from $self->{db_name}_kv") }
    catch {
        $self->query(qq{create table $self->{db_name}_kv (
            id integer primary key autoincrement, 
            oid text,
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
            id integer primary key autoincrement,
            id1,
            id2,
            edge_name text,
            foreign key(id1) references $self->{db_name}_obj(id) on delete cascade,
            foreign key(id2) references $self->{db_name}_obj(id) on delete cascade
            )
        });
    };
}

1;
