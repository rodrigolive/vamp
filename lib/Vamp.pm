package Vamp;
#use Mouse;
use strict;
use warnings;
use DBIx::Simple;
use Vamp::Database;
use Try::Tiny;

use constant DEBUG => $ENV{VAMP_TRACE} || $ENV{VAMP_DEBUG} || 0;

sub connect {
    my ($self, %args ) = @_;
    #my $db = DBIx::Simple->new( @_ );
    #$db->query( 'select * from vamp_kv' );
    my @connection = ref $args{args} eq 'ARRAY' ? @{$args{args}} : $args{args}; 
    my $db = Vamp::Database->new( @connection ); 
    $db->{driver_name} = $db->dbh->{Driver}->{Name};
    my $backend = "Vamp::Backend::$db->{driver_name}";
    eval "require $backend" or $backend = "Vamp::Backend::Generic";
    warn $@ if $@;
    bless $db, $backend;
    $db->{db_name} = $args{db} || 'vamp';
    $db->deploy;
    return $db;
}

=head1 NAME

Vamp - NoSQL Document-Graph DB on top of plain-old DBI

=head1 SYNOPSIS

    my $db = Vamp->connect( db=>'vamp', args=>['dbi:Oracle://locahost:1521', 'user', 'pass' ] );
    my $collection = $db->collection('employee');
    $collection->insert({ name=>'Gregory', age=>25 });
    my $rs = $collection->find({ age=>[ '>', 20 ] });
    
    while( my $rec = $rs->next ) {
        say $rec->{name};  
    }

=head1 DESCRIPTION

This module mimics the interface and functionality of popular document
databases such as MongoDB and CouchDB, giving similar functionality
on top of a standard DBI connection (currently L<DBD::Oracle> only).

The idea is to make it easier to transition
your app from a SQL backend to a NoSQL one. 

=cut
1;
