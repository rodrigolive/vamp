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

around 'BUILDARGS' => sub {
    my %opts = ref $_[0] eq 'ARRAY' ? @$_[0] : @_;
    $opts{args}=[$opts{args}] unless ref $opts{args} eq 'ARRAY';
    \%opts;
};

=head2 collection

Returns a collection object.

=cut
sub collection {
    my ($self, $collname ) = @_;
    my $collection_class = $self->collection_class;
    $collection_class->new( name=>$collname, db=>$self ); 
}

=head2 recreate

Drops and recreates the database. Caution: all data will be lost.

=cut
sub recreate {
    my $self = shift;
    $self->drop_database;
    $self->deploy;
}

=head2 edge

Returns the edge-type collection. 

=cut
sub edge {
    my ($self, $edge_name ) = ( shift, shift );
    die "missing argument: edge_name" unless defined $edge_name;
    my $class = $self->edge_class;
    my $table = $self->db_name . '_rel';
    return $class->new( db=>$self, edge_name => $edge_name, table_name=>$table );
}

sub add_edge {
    my ($self, $from, $edge_name, $to ) = @_;
    $self->edge( $edge_name )->add( $from => $to );
}

sub delete_edge {
    my ($self, $from, $edge_name, $to ) = @_;
    $self->edge( $edge_name )->delete( $from => $to );
}

1;

