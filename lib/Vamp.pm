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
    $args{deploy} //= 1;
    #my $db = DBIx::Simple->new( @_ );
    #$db->query( 'select * from vamp_kv' );
    my @connection = ref $args{args} eq 'ARRAY' ? @{$args{args}} : $args{args}; 
    my $db = Vamp::Database->new( @connection ); 
    $db->{driver_name} = $args{backend} || $db->dbh->{Driver}->{Name};
    my $backend = "Vamp::Backend::$db->{driver_name}";
    eval "require $backend" or $backend = "Vamp::Backend::Generic";
    warn $@ if $@;
    bless $db, $backend;
    $db->{db_name} = $args{db} || 'vamp';
    $db->deploy if $args{deploy};
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

=head1 FEATURES

=over

=item One DBI database may hold several Vamp databases. 

=item Vamp databases are a set of tables, sequences, indexes and constraints prefixed by the C<db_name> parameter.

=item search is done with L<SQL::Abstract> notation.

=item order_by: supported

=item paging is supported with the keywords C<start> and C<limit>

=cut

=head1 USAGE

        # connects to a dbi db, deploying tables if necessary 
        my $db = Vamp->connect( db=>'vamp', args=>[ split /,/, $conn ], deploy=>0 );

        # get collection, creating if needed
        my $person = $db->collection('people');

        # basic insert
        my $id = $people->insert({ name=>'Joe', age=>55 });

        # arrays, ok
        my $id = $people->insert({ name=>'Joe', hobbies=>['golf', 'fortran'] });

        # updates
        my $id = $person->{id}
        $people->update( $id => { age=>56 } );
        $people->upsert( $id => { age=>77 } );

        # large objects ok
        $people->insert({ name=>'Bob', cv=>$file->slurp });

        # find_one returns the document
        my $person = $people->find_one({ name=>'joe' });

        # or with the id
        my $person = $people->find_one( $id );

        # find returns a result set always
        my $rs = $people->find({ name=>{ -like=>'%j%' } }, { order_by=>'age' });
        my $first = $rs->first;
        while( my $person = $rs->next ) {
            say $person->{name}; 
            say $person->{hobbies}->[0]; 
        }

        # all rows at once
        say $_->{name} for $rs->all;

=head1 RELATIONSHIPS

Relationships are held in a many-to-many fashion. 
They may have a type, called C<edge>, and properties.

        # create a relationship
        my $rel = $db->relation({ from=>$joe->{id}, to=>$bob->{id}, edge=>'friends' });

        # delete
        $rel->delete;

        # add edge properties 
        $rel->insert({ friends_since=>1985 }); 

        # find all edges
        my $rs = $db->relation({ from=>$joe->{id} }); 
        my $rs = $db->relation({ from=>$joe->{id}, depth=>1 }); 

=head1 DESIGN

Document hashes are deeply stored as key-value pairs, adding dots 
to identify nested keys.

For instance, the following hash document:

    {  
        name    => 'Susan',
        home    => { street => '25 Elm St.', phone => '555-1234' },
        hobbies => [qw/surfing biking/]
    }

Gets stored as:

    ID  OID    KEY               VALUE
    --- ---  -------           -------
     1    1  name              Susan
     2    1  home.street       25 Elm St.
     3    1  home.phone        555-1234
     4    1  hobbies           surfing
     5    1  hobbies           biking

=head1 TODO

This module is in B<alpha> state. API may change at will.

=cut
1;
