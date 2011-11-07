package Vamp::Collection;
use Any::Moose 'Role';
use Try::Tiny;

has 'name'      => is => 'ro', isa => 'Str',            required => 1;
has 'db'        => is => 'ro', does => 'Vamp::Database', required => 1;
has 'serialize' => is => 'ro', isa => 'Bool',           default  => 1;

requires 'drop';
requires 'insert';
requires 'update';
requires 'upsert';
requires 'find';
requires 'get';
requires 'all';
requires 'query';
requires 'find_one';

sub first { return shift->find->first }

1;
