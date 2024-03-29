package Vamp::DBI::Database;
use Any::Moose 'Role';
use Vamp;
use Vamp::Util;
use DBIx::Simple;

with 'Vamp::Database';

has engine => (
    is      => 'rw',
    isa     => 'Object',
    handles => [ qw/query abstract dbh/ ]
);

sub BUILD {
    my $self = shift;
    $self->engine( DBIx::Simple->new( @{ $self->args } ) );
}

sub drop_collection {
    my ($self, $collname) = @_;
    my $db_name = $self->db_name;
    $self->query("delete from ${db_name}_obj where collection = ?", $collname );
}

sub _dump {
    my ($self , $data ) = @_;
    require YAML::XS;
    return YAML::XS::Dump( $data );
}

sub _is_special {
    my ($self,$v)=@_;
    my $ref = ref $v;
    return ($v, undef) unless $ref;
    if( $ref eq 'HASH' ) {
        my %vals;
        my %conds;
        for( keys %$v ) {
            /-like|-not_like|\>|\<|\!|\=/
                ? $conds{ $_ } = $v->{$_}
                : $vals{ $_ } = $v->{$_};
        }
        return ( %vals ? \%vals : undef, %conds ? \%conds : undef );
    }
    return ($v,undef);
}

sub _flatten_as_hash {
    my $self = shift;
    my @flat = $self->_flatten( @_ );
    # turn array into hash
    my $flat_hash = {};
    $flat_hash->{ $_->{key} } = $_->{value} for @flat;
    return $flat_hash;
}

sub _quote_keys {
    my ($self, $hash) = @_;
    return {} unless ref $hash eq 'HASH' && keys %$hash;
    my %ret;
    $ret{ '"' . $_ . '"' } = $hash->{$_} for keys %$hash;
    \%ret;
}

sub _flatten {
    my ($self, $where, $prefix) = @_;
    my @flat;
    my $ref = ref $where;
    if( $ref eq '' ) {
        return { oid => $where };
    }
    elsif( $ref eq 'HASH' ) {
        while( my ($k,$v) = each %$where ) {
            $prefix and $k = "$prefix.$k";
            if( ref $v ) {
                my ($vals, $conds) = $self->_is_special( $v );
                push @flat, $self->_flatten($vals, $k) if defined $vals;
                push @flat, { key=>$k, value=>$conds } if defined $conds;
            } else {
                push( @flat, { key=>$k, value=>$v } );
            }
        }
        return @flat;
    }
    elsif( $ref eq 'ARRAY' ) {
        if( $prefix =~ /-and|-or/ ) {
            return { $prefix => $self->_flatten($where) };
        } else {
            return { key=>$prefix, value=>$where };
        }
    }
    else {
        die 'invalid type';
    }
    return @flat;
}

sub _abstract {
    my ($self,@where) = @_;
    my ($where,@binds) = $self->abstract->where({ -and => \@where });
    return $where, @binds;
}

sub drop_database { 
    my $self = shift;
    my $db_name = $self->db_name;

    for my $suffix ( qw/_kv _obj _rel/ ) {
        $self->drop_table( ${db_name} . $suffix );
    }
}

sub drop_table { 
    my ($self,$table) = @_;
    eval { $self->query("drop table $table") }; 
    $ENV{VAMP_DEBUG} && $@ and warn $@;
}

1;
