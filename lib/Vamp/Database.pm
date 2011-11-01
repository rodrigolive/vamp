=pod

Base class for all backends.

=cut
package Vamp::Database;
#use Mouse;
use strict;
#use Mouse;
use Try::Tiny;
use Vamp;
use Vamp::Util;
use Vamp::ResultSet;
use Vamp::Collection;
use base 'DBIx::Simple' ;

use YAML; # XXX

sub collection {
    my ($self, $collname ) = @_;
    Vamp::Collection->new( name=>$collname, db=>$self ); 
}

sub recreate {
    my $self = shift;
    $self->drop_database;
    $self->deploy;
}

# XXX unused
sub query_find_id {
    my ($self, %args) = @_;
    my $db_name = $self->{db_name};
    my $oids = $self->query("select distinct ${db_name}_obj.id from ${db_name}_obj, ${db_name}_kv
        where ${db_name}_obj.id=${db_name}_kv.oid and key = ? and value = ?", 
        $args{k}, $args{v}
    );
}

sub query_find_plain {
    my $self = shift;
    my $db_name = $self->{db_name};
    my $query = "select distinct ${db_name}_obj.id from ${db_name}_obj, ${db_name}_kv
        where ${db_name}_obj.id=${db_name}_kv.oid";
    my @binds;
    for my $and ( @_ ) {
        $query .= " and ( key = ? and value = ? )";
        push @binds, $and->{k}, $and->{v};
    }
    $self->query( $query, @binds );
}

sub drop_collection {
    my ($self, $collname) = @_;
    my $db_name = $self->{db_name};
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

sub _shorten_keys {
    my ($self, $hash) = @_;
    return {} unless ref $hash eq 'HASH' && keys %$hash;
    my %ret;
    #$ret{ short($_) } = $hash->{$_} for keys %$hash;
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
        #return [ map { +{ k=>$prefix, v=>$_ } } @$where ];
    }
    else {
        die 'invalid type';
    }
    return @flat;
}

sub _abstract {
    my ($self,@where) = @_;
    #warn Dump \@where;
    my ($where,@binds) = $self->abstract->where({ -and => \@where });
    #warn $where;
    #warn join ',',@binds;
    return $where, @binds;
}

sub drop_database { 
    my $self = shift;
    my $db_name = $self->{db_name};

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
