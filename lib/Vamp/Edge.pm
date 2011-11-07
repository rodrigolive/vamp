package Vamp::Edge;
use Any::Moose 'Role';

has db         => ( is => 'ro', does => 'Vamp::Database', required => 1, weak_ref=>1 );
has edge_name  => is => 'rw', isa => 'Str',            required => 1;
has directed   => is => 'rw', isa => 'Bool',           default  => 1;

with 'Vamp::Edge';

requires 'add';
requires 'delete';
requires 'find';

1;
