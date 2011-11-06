package Vamp::Backend::Oracle::Base;
use strict;
use warnings;
use base 'Vamp::Database';

sub drop_table { 
    my ($self,$table) = @_;
    eval { $self->query("drop table $table cascade constraints") }; 
    $ENV{VAMP_DEBUG} && $@ and warn $@;
}

sub _insert_kv {
    my ($self, %args) = @_;
    my $db_name = $self->{db_name};
    $self->query( qq{INSERT INTO ${db_name}_kv ( id, oid, key, value, datatype, seq, version )
        VALUES (${db_name}_kv_seq.NEXTVAL, ?,?,?,?,?,?) }, 
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
        if ( $args{serialize} && exists $args{data} ) {
            $self->query( qq{INSERT INTO ${db_name}_obj ( id, collection, document )
                VALUES ( ${db_name}_obj_seq.NEXTVAL, ?,? ) },
                $args{collection}, $self->_dump( $args{data} ) );
        } else {
            $self->query( qq{INSERT INTO ${db_name}_obj ( id, collection ) 
                VALUES (${db_name}_obj_seq.NEXTVAL,?) }, $args{collection} );
        }
        return $self->last_insert_id('','','','$self->{db_name}_obj');
    }
}

1;

