package Vamp::ResultSet;
use strict;
use warnings;
use Vamp;

sub new {
    my ($class, %args ) = @_;
    bless \%args, $class;
}

sub all {
    my $self = shift;
    my @rows;
    my @ret;
    my $lastid;
    $self->{rs} = $self->{db}->query( @{ $self->{query} } ); 
    for my $r ( $self->{rs}->hashes ) {
        my $oid = $r->{oid};
        if( defined $lastid && $oid != $lastid ) {
            push @ret, $self->_inflate_row( @rows ); 
            @rows = ();
        } 
        push @rows, $r; 
        $lastid = $oid;
    }
    @rows and push @ret, $self->_inflate_row( @rows );
    return @ret;
}

sub as_query {
    my $self = shift;
    return $self->{query};
}

sub first {
    my $self = shift;
    my $obj = $self->next;
    defined $obj ? $obj : {};
}

sub next {
    my $self = shift;
    my @rows;
    my $lastid = $self->{lastid};
    push @rows, delete $self->{lastrow} if $self->{lastrow};
    $self->{rs} = $self->{db}->query( @{ $self->{query} } ); 
    while( my $r = $self->{rs}->hash ) {
        my $oid = $r->{oid};
        #warn ">>>>>>>" . YAML::Dump( $r );
        if( defined $lastid && $oid ne $lastid ) {
            $self->{lastrow} = $r;
            $self->{lastid} = $oid;
            last;
        } else {
            push @rows, $r; 
            $lastid = $oid;
        }
    }
    @rows and return $self->_inflate_row( @rows );
    return undef;
}

sub count {
    my $self = shift;
    my ($sql, @binds ) =  @{ $self->{query} };
    $sql = "SELECT COUNT(*) FROM ( SELECT distinct oid FROM ( $sql ) )";
    $self->{rs} = $self->{db}->query( $sql, @binds );
    return $self->{rs}->list;
}

=head2 _inflate_row

Turns an array of kv table rows into a plain
key-value Perl hash.

    [
        { oid=>1, key=>'user.name', value=>'Bob' }, 
        { oid=>1, key=>'user.age', value=>'20' }, 
    ]

Into:

    { 
        id   =>1,
        user => {
            name => 'Bob',
            age  => '20',
        }
    }

=cut
sub _inflate_row {
    my $self = shift;
    my %row;
    #warn YAML::Dump( \@_ );
    # for each db kv row
    for my $kv_row ( @_ ) {
        my $oid = $kv_row->{oid};
        $row{ id } ||= $oid;
        my @keys = split /\./, $kv_row->{key}; 
        if( defined $kv_row->{datatype} && $kv_row->{datatype} eq 'a' ) {
            my $x = $self->_deepen( \%row, @keys );
            push @{ $$x }, $kv_row->{value}; 
        } else {
            my $x = $self->_deepen( \%row, @keys );
            $$x = $kv_row->{value}; 
        }
    }
    \%row;
}

=head2 _deepen

Recurses through a nested key array and turns it into
sub-keys of a hash. 

Returns a reference to the value slot of the hash.

    my $value_ref = $self->_deepen( \%hash, 'user', 'name' );

    # value_ref references $hash{user}{name}
    
    $$value_ref = 'Bob';

    # now $hash{user}{name} eq 'Bob'

=cut
sub _deepen {
    my ($self, $row, @keys) = @_; 
    my $k = shift @keys;
    !@keys and return \($row->{$k});
    $row->{$k} ||= {};
    $self->_deepen( $row->{$k}, @keys );
}

1;

