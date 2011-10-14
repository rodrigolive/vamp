package Vamp::Collection;
use Mouse;
use Try::Tiny;
use YAML;

has 'name'      => is => 'ro', isa => 'Str',            required => 1;
has 'db'        => is => 'ro', isa => 'Vamp::Database', required => 1;
has 'serialize' => is => 'ro', isa => 'Bool',           default  => 1;

sub drop {
    my $self = shift;
    $self->db->drop_collection( $self->name ); 
}

sub insert {
    my $self = shift;
    my $data = ref $_[0] eq 'HASH' ? shift : \%{ @_ };
    die 'invalid data' unless ref $data eq 'HASH';
    my $oid = $self->_obj( data=>$data );
    die 'no last oid' unless defined $oid;
    try {
        while( my ($k,$v) = each %$data ) {
            $self->_serialdo( do=>'insert', oid=>$oid, k=>$k, v=>$v ); 
        }
    } catch {
        $self->rollback( oid=>$oid );
    };
}

sub find {
    my $self = shift;
    my ($where, $opts ) = ref $_[0] eq 'HASH' 
        ? ( shift(), shift() )
        : ( \%{ @_ || {} } , {} );
    my $query = $self->db->build_query_findall( $self->name, $where, $opts );
    my $rs = Vamp::ResultSet->new( db=>$self->db, query=>$query );
    return wantarray ? $rs->all : $rs;
}

*query = \&find;

# this is based on the older oid intersect
sub find_all {
    my $self = shift;
    my $where = ref $_[0] eq 'HASH' ? shift : \%{ @_ };
    #warn Dump [ $self->_flatten( { age=>[20, 30], name=>{ first=>['joe', 'ana'] } }) ];
    #warn Dump [ $self->_flatten( $where ) ]; 
    #TODO need to transmit paging and order to this query
    my $oids = $self->db->query_find_abs( $where );
    #TODO need to allow paging here, by reading all oids at once
    #   $self->db->query_oids( $oids );
    my @objs = map { $self->_get( $_ ) } @$oids;
    wantarray ? @objs : \@objs;
}

sub find_one {
    my $self = shift;
    my $rs = $self->find( @_ );
    $rs->next;
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


sub _obj {
    my ($self , %args ) = @_;
    my $db_name = $self->db->{db_name};
    if ( $self->serialize && exists $args{data} ) {
        $self->db->query( qq{insert into ${db_name}_obj ( collection, document ) values (?,?) },
            $self->name, $self->_dump( $args{data} ) );
    } else {
        $self->db->query( qq{insert into ${db_name}_obj ( collection ) values (?) }, $self->name );
    }
    return $self->db->last_insert_id('','','','$self->{db_name}_obj');
}

sub _dump {
    my ($self , $data ) = @_;
    use YAML::XS;
    return YAML::XS::Dump $data;
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
        $self->db->query( qq{insert into ${db_name}_kv ( oid, key, value, datatype, seq, version ) values (??) },
            $oid, $key, $value, $args{datatype} || 'v', $args{seq} || 1, $args{version} || 1
        );
    } elsif( $ref eq 'ARRAY' ) {
        my $cnt = 0;
        for( @$value ) {
            $self->_serialdo( do=>'insert', oid=>$oid, k=>$key, v=>$_, seq=>$cnt, datatype=>'a', prefix=>'' ); 
        }
    } elsif( $ref eq 'HASH' ) {
        while( my ($k,$v) = each %$value ) {
            $self->_serialdo( do=>'insert', oid=>$oid, k=>$k, v=>$v, prefix=>$key ); 
        }
    } else {
        die "data type $ref not supported";
    }
}

1;

__END__
=pod unused

sub _inflate_row {
    my $self = shift;
    my %row;
    for( @_ ) {
        my $oid = $_->{oid};
        $row{ id } ||= $oid;
        my @keys = split /\./, $_->{key}; 
        if( $_->{datatype} eq 'a' ) {
            my $x = $self->_deepen( \%row, @keys );
            push @{ $$x }, $_->{value}; 
        } else {
            my $x = $self->_deepen( \%row, @keys );
            $$x = $_->{value}; 
        }
    }
    \%row;
}

sub _inflate_rows {
    my $self = shift;
    my %row;
    for( @_ ) {
        my $oid = $_->{oid};
        $row{ $oid } ||= {};
        my @keys = split /\./, $_->{key}; 
        if( $_->{datatype} eq 'a' ) {
            my $x = $self->_deepen( $row{ $oid }, @keys );
            push @{ $$x }, $_->{value}; 
        } else {
            my $x = $self->_deepen( $row{ $oid }, @keys );
            $$x = $_->{value}; 
        }
    }
    \%row;
}

=cut
