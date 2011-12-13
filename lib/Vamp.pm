package Vamp;
use Any::Moose;
use strict;
use warnings;
use Try::Tiny;

use constant DEBUG => $ENV{VAMP_TRACE} || $ENV{VAMP_DEBUG} || 0;

sub db {
    my ( $self, %args ) = @_;
    $args{deploy} //= 1;

    # load backend database
    my $backend = $args{backend} or die "Missing backend";

    # connect to db
    my $db_class = 'Vamp::Backend::' . $backend;

    #$db_class .= '::Database' unless $backend =~ /::/;
    eval "require $db_class" or $db_class = "Vamp::Backend::SQLite";
    warn $@ if $@;
    my $db = $db_class->new( args => $args{args}, db_name => $args{db} || 'vamp' );
    $db->deploy if $args{deploy};
    return $db;
} 

sub _to_ids {
     map { 
        ref $_ eq 'HASH'
        ? $_->{id}
        : $_;
    } @_;
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
on top of a standard DBI connection
(currently L<DBD::Oracle> and L<DBD::SQLite>).

The idea is to make it easier to transition
your app from a SQL backend to a NoSQL one, while keeping the interface. 

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

        # edge collection (or group)
        my $edge_group = $db->edge('friends');
        map { say $_->{from} , $_->{to} } $edge_group->all_nodes;

        # create an edge
        my $edge = $db->add_edge( $joe->{id} => 'friends' => $bob->{id} );
        my $edge = $db->edge('friends')->add( $joe->{id} => $bob->{id} ); # ditto

        # edge properties
        $edge->data( met=>'11/10/1999' );
        $edge->save;

        # delete
        $rel->delete;

        # add edge properties 
        $rel->insert({ friends_since=>1985 }); 

        # find all edges
        my $rs = $db->edge('kids');
        while( my $edge = $rs->next ) {
            ->add({ from=>$joe->{id} }); 
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
