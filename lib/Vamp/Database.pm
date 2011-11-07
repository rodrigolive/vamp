package Vamp::Database;
=head1 NAME

Vamp::Database - Role for all backend Databases.

=cut
use Any::Moose 'Role';

requires 'deploy';
requires 'drop_database';
requires 'collection_class';
requires 'edge_class';

has args    => ( is => 'ro', isa => 'ArrayRef', required => 1 );
has db_name => ( is => 'ro', isa => 'Str',      required => 1 );

sub collection {
    my ($self, $collname ) = @_;
    my $collection_class = $self->collection_class;
    $collection_class->new( name=>$collname, db=>$self ); 
}

sub recreate {
    my $self = shift;
    $self->drop_database;
    $self->deploy;
}

sub edge {
    my ($self, $edge_name ) = ( shift, shift );
    die "missing argument: edge_name" unless defined $edge_name;
    my $class = $self->edge_class;
    my $table = $self->db_name . '_rel';
    return $class->new( db=>$self, edge_name => $edge_name, table_name=>$table );
}

1;

