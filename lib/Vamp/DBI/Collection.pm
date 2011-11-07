package Vamp::DBI::Collection;
use Any::Moose 'Role';
use Try::Tiny;

with 'Vamp::Collection';

requires 'rs_class';

sub drop {
    my $self = shift;
    $self->db->drop_collection( $self->name ); 
}

sub insert {
    my $self = shift;
    my $first = shift;
    if( ref $first eq 'ARRAY' ) {
        my @oids;
        push @oids, $self->_insert_one( $_, @_ ) for @$first;
        return @oids;
    } else {
        return $self->_insert_one( $first, @_ );
    }
}

sub _insert_one {
    my $self = shift;
    my $data = ref $_[0] eq 'HASH' ? shift : \%{ @_ };
    die 'invalid data' unless ref $data eq 'HASH';
    my $oid = $self->db->_create( data=>$data, serialize=>$self->serialize, collection=>$self->name );
    die 'no last oid' unless defined $oid;
    try {
        while( my ($k,$v) = each %$data ) {
            $self->_serialdo( do=>'insert', oid=>$oid, k=>$k, v=>$v ); 
        }
    } catch {
        $self->_rollback( oid=>$oid );
        die "Error inserting: " . shift();
    };
    return $oid;
}

sub update {
    my $self = shift;
    my $oid = shift;
    my $data = ref $_[0] eq 'HASH' ? shift : \%{ @_ };
    try {
        while( my ($k,$v) = each %$data ) {
            $self->_serialdo( do=>'update', oid=>$oid, k=>$k, v=>$v ); 
        }
    } catch {
        $self->_rollback( oid=>$oid );
        die "Error updating: " . shift();
    };
}

sub upsert {
    my $self = shift;
    my $oid = shift;
    my $data = ref $_[0] eq 'HASH' ? shift : \%{ @_ };
    if( defined $self->find_one( $oid ) ) {
        return $self->update( $oid => $data );
    } else {
        $data->{id} = $oid;
        return $self->insert( $data );
    }
}

sub find {
    my $self = shift;
    my ($where, $opts ) = ref $_[0] eq 'HASH' 
        ? ( shift(), shift() )
        : @_
            ? ( { id=>[@_] } , {} )
            : ({},{});
    my $query = $self->db->build_query_findall( $self->name, $where, $opts );
    my $rs_class = $self->rs_class;
    my $rs = $rs_class->new( db=>$self->db, query=>$query );
    return wantarray ? $rs->all : $rs;
}

*query = \&find;
*all   = \&find;

sub get {
    my $self = shift;
    my $query = $self->db->build_query_find_id( $self->name, @_ );
    my $rs_class = $self->rs_class;
    my $rs = $rs_class->new( db=>$self->db, query=>$query );
    @_ > 1 ? $rs : $rs->next; 
}

sub find_one {
    my $self = shift;
    if( @_ > 0 ) {
        return !ref $_[0]
            ? $self->get( @_ )
            : $self->find( @_ )->next;
    } else {
        return $self->find->first;
    }
}

=head2 insert_from_query

Runs a SQL query, inserting rows into 
the collection.

May have C<map> and C<grep> callback
parameters to transform and skip rows.

    $people->insert_from_query(
        query=>'select * from my_table',
        map  => sub {
            $_->{uc_name} = uc $_->{name};
        },
        grep  => sub {
           $_->{name} =~ /^joe/i ;
        },
    );

=cut
sub insert_from_query {
    my ($self, %args) = @_;
    my $rs = $self->{db}->query( $args{query} ); 
    my @ids;
    for my $row ( $rs->hashes ) {
        if( $args{map} ) { 
            local $_;
            $_ = $row;
            $args{map}->($row);
            $row = $_;
        }
        if( $args{grep} ) {
            local $_;
            $_ = $row;
            ! $args{grep}->($row) and next;
            $row = $_;
        }
        push @ids, $self->insert( $row );
    }
    return @ids;
}

sub _deepen {
    my ($self, $row, @keys) = @_; 
    my $k = shift @keys;
    !@keys and return \($row->{$k});
    $row->{$k} ||= {};
    $self->_deepen( $row->{$k}, @keys );
}

sub _get {
    my ($self , $oid ) = @_;
    my $db_name = $self->db->{db_name};
    my $res = $self->db->query("select * from ${db_name}_kv where oid = ? order by seq", $oid );
    my %row = ( id => $oid );
    for( $res->hashes ) {
        my @keys = split /\./, $_->{key}; 
        if( $_->{datatype} eq 'a' ) {
            my $x = $self->_deepen( \%row, @keys );
            push @{ $$x }, $_->{value}; 
        } else {
            my $x = $self->_deepen( \%row, @keys );
            $$x = $_->{value}; 
        }
    }
    #warn "=" x 20, Dump \%row;
    \%row;
}


sub _rollback {
    my ($self , %args ) = @_;
    my $oid = $args{oid};
    my $db_name = $self->db->{db_name};
    $self->db->query("delete from ${db_name}_obj where id=?", $oid );
}

sub _serialdo {
    my ($self , %args ) = @_;

    my $oid = $args{oid};
    my $key = $args{prefix}
        ? join( '.', $args{prefix}, $args{k} )
        : $args{k};
    my $value = $args{v};
    my $ref = ref $value;
    my $db_name = $self->db->{db_name};
    if( ! $ref ) {
        if( $args{do} eq 'insert' ) {
            $self->db->_insert_kv(
                oid      => $oid,
                key      => $key,
                value    => $value,
                datatype => $args{datatype} || 'v',
                seq      => $args{seq} || 1,
                version  => $args{version} || 1
            );
        } 
        elsif( $args{do} eq 'update' ) {
            $self->db->_update_kv( oid=>$oid, key=>$key, value=>$value );
        }
        else {
           die "Invalid do operation $args{do}"; 
        }
    } elsif( $ref eq 'ARRAY' ) {
        my $cnt = 0;
        for( @$value ) {
            $self->_serialdo( do=>$args{do}, oid=>$oid, k=>$key, v=>$_, seq=>$cnt, datatype=>'a', prefix=>'' ); 
        }
    } elsif( $ref eq 'HASH' ) {
        while( my ($k,$v) = each %$value ) {
            $self->_serialdo( do=>$args{do}, oid=>$oid, k=>$k, v=>$v, prefix=>$key ); 
        }
    } else {
        die "data type $ref not supported";
    }
}

1;

