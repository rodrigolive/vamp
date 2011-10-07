package Vamp;
#use Mouse;
use strict;
use warnings;
use DBIx::Simple;
use Vamp::Database;
use Try::Tiny;

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

1;
