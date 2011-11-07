package Vamp::ResultSet;
use Any::Moose 'Role';

has db => ( is => 'ro', does => 'Vamp::Database', required => 1, weak_ref => 1 );
has rs => qw(is rw isa Object);

requires 'all';
requires 'as_query';
requires 'first';
requires 'next';
requires 'count';

# last? reset?

1;
