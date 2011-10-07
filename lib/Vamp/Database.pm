package Vamp::Database;
#use Mouse;
use strict;
use Vamp::Collection;
use Try::Tiny;
use Vamp::Util;
use YAML;
use base 'DBIx::Simple' ;

sub collection {
    my ($self, $collname ) = @_;
    Vamp::Collection->new( name=>$collname, db=>$self ); 
}

sub recreate {
    my $self = shift;
    $self->drop;
    $self->deploy;
}

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

sub query_find_abs {
    my ($self, $where, @binds ) = @_;
    my $db_name = $self->{db_name};
    my $query_head = "SELECT DISTINCT oid FROM ${db_name}_kv ";
    my @wh = $self->_flatten( $where );
    #push @wh, \[ "${db_name}_obj.id = ${db_name}_kv.oid" ];
    #my ($where,@binds) = $self->_abstract( -or => \@wh );
    #warn join',',$self->query( $query_head . $where, @binds )->flat;
    my @all_oids;
    for my $wh ( @wh ) {
        my ($where,@binds) = $self->_abstract( $wh );
        my $query = $query_head . $where;
        #warn $query;
        my @oids = $self->query( $query, @binds )->flat;
        push @all_oids, \@oids;
    }
    my @res = intersect(@all_oids);
    wantarray ? @res : \@res;
}

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
    #warn $sql;
    $sql = "SELECT oid,key,value FROM ${db_name}_kv vamp1 WHERE EXISTS ( $sql ) ORDER BY vamp1.oid"; 
    #warn Dump $self->query( $sql, @all_binds )->hashes;
    $self->query( $sql, @all_binds );
}

sub _is_special {
    my ($self,$v)=@_;
    my $ref = ref $v;
    return ($v, undef) unless $ref;
    if( $ref eq 'HASH' ) {
        my %vals;
        my %conds;
        for( keys %$v ) {
            /-like|-not_like/
                ? $conds{ $_ } = $v->{$_}
                : $vals{ $_ } = $v->{$_};
        }
        return ( %vals ? \%vals : undef, %conds ? \%conds : undef );
    }
    return ($v,undef);
}

sub _flatten {
    my ($self, $where, $prefix) = @_;
    my @flat;
    my $ref = ref $where;
    if( $ref eq '' ) {
        return [{ oid => $where }];
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
}

sub _abstract {
    my ($self,@where) = @_;
    #warn Dump \@where;
    my ($where,@binds) = $self->abstract->where({ -and => \@where });
    #warn $where;
    #warn join ',',@binds;
    return $where, @binds;
}

package Vamp::ResultSet;
use strict;
use warnings;
use base 'DBIx::Simple::Result';

sub all {
    my $self = shift;
    my @rows;
    my @ret;
    my $lastid;
    for my $r ( $self->hashes ) {
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
    while( my $r = $self->hash ) {
        my $oid = $r->{oid};
        #warn YAML::Dump( $r );
        if( defined $lastid && $oid != $lastid ) {
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

sub _inflate_row {
    my $self = shift;
    my %row;
    #warn YAML::Dump( \@_ );
    for( @_ ) {
        my $oid = $_->{oid};
        $row{ id } ||= $oid;
        my @keys = split /\./, $_->{key}; 
        if( defined $_->{datatype} && $_->{datatype} eq 'a' ) {
            my $x = $self->_deepen( \%row, @keys );
            push @{ $$x }, $_->{value}; 
        } else {
            my $x = $self->_deepen( \%row, @keys );
            $$x = $_->{value}; 
        }
    }
    \%row;
}

sub _deepen {
    my ($self, $row, @keys) = @_; 
    my $k = shift @keys;
    !@keys and return \($row->{$k});
    $row->{$k} ||= {};
    $self->_deepen( $row->{$k}, @keys );
}

1;
