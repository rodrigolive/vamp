package Vamp::Query;
use strict;
use warnings;
use SQL::Abstract;

sub parse {
    my ($self, $query)=@_;
    my @st;
    my %kv;
    while( my ($k,$v) = each %$query ) {
        for( $k ) {
            /^-or$/ and do {
                $self->parse( $v );
                next;
            };
            /^-and$/ and do {
                $self->parse( $v );
                next;
            };
            my $ref = ref $v;
            if( $ref eq 'ARRAY' ) { 
                push @{ $kv{ $k } }, @$v;
                push @st, { key=>$k, value=>$_ } for @$v;
            } else {
                push @{ $kv{ $k } }, $v;
                push @st, { key=>$k, value=>$v };
            }
        }
    }
    #generate(@st);
    generatekv( %kv );
}

sub generate {
    my $sql='';
    my @binds;
    for(@_) {
       $sql .= "( key=$_->{key} and value=$_->{value} ) ";
    }
    warn $sql;
}

sub generatekv {
    my %kv=@_;
    my @sqls;
    my $sa = SQL::Abstract->new;
    while( my ($k,$v) = each %kv ) {
        my $sql = "select oid from kv ";
        $sql .= $sa->where({ key=>$k, value=>$v });
        push @sqls, $sql;
        warn $sql;
    }
}

sub gen {
    my ($self, $where) = @_;
    my $sa = SQL::Abstract->new;
    my $db_name = 'vamp';
    my $query_head = "SELECT DISTINCT oid FROM ${db_name}_kv ";
    my @sqls;
    my @all_binds;
    for my $st ( $self->flatten( $where ) ) {
       my ($where, @binds ) = $sa->where( $st );
       my $sql = $query_head . $where;
       push @sqls, $sql; 
       push @all_binds, @binds;
    }
    my $from = join ' INTERSECT ', @sqls;
    my $sql = "SELECT DISTINCT oid FROM ( $from )";
}

sub flatten {
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
                my ($vals, $conds) = $self->is_special( $v );
                push @flat, $self->flatten($vals, $k) if defined $vals;
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

sub is_special {
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


1;
