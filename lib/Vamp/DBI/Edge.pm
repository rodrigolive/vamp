package Vamp::DBI::Edge;
use Any::Moose 'Role';
use Carp;
use namespace::autoclean;

has table_name => is => 'rw', isa => 'Str',            required => 1;
has rs         => is => 'rw', isa => 'Object';

with 'Vamp::Edge';

sub _find {
    my $self = shift;
    my $table = $self->table_name;
    if( my @ids = Vamp::_to_ids( @_ ) ) {
        my $places = join ',', ( '?' x @ids );
        $self->rs( 
            $self->directed 
            ? $self->db->query( qq{SELECT id1,id2 FROM $table WHERE edge_name = ? AND id1 IN ( $places )},
                $self->edge_name, @ids )
            : $self->db->query( qq{SELECT id1,id2 FROM $table WHERE edge_name = ? AND id1 IN ( $places ) OR id2 IN ( $places )},
                $self->edge_name, @ids, @ids)

        );
    } 
    else {
        $self->rs( 
            $self->db->query( qq{SELECT id1,id2 FROM $table WHERE edge_name = ?}, $self->edge_name )
        );
    }
}

sub find_ids {
    my $self = shift;
    $self->_find( @_ );
    return $self->rs->arrays;
}

sub find {
    my $self = shift;
    $self->_find( @_ );
    #return $self->rs->hashes;
    #my $rs = $rs_class->new( db=>$self->db, query=>$query );
    my $rs_class = $self->rs_class;
    my $rs = $rs_class->new( db=>$self->db, rs=>$self->rs );
}

sub add {
    my $self = shift;
    my ($from, @to) = Vamp::_to_ids( @_ );
    die "source edge undefined" unless defined $from;
    die "dest edge undefined" unless ( grep defined, @to ) == @to;
    my $table = $self->table_name;
    my @edges;
    for( @to ) {
        $self->db->query( qq{INSERT INTO $table (id1,id2,edge_name) VALUES (?,?,?)}, $from, $_, $self->edge_name );
        # push @edges, last_insert
    }
    return @edges; # edge objects
}

=head2 delete

Delete edges.

    $edge->delete( $id1 );  # all from id1
    $edge->delete( undef => $id2 );  # any to id2
    $edge->delete( undef => $id2,$id3,$id4 );  # any to id2,id3,id4
    $edge->delete( $id1 => $id2 ); # just this one

=cut
sub delete {
    my ($self, @ids) = @_;
    carp "Missing id(s) to delete" unless @ids>0; 
    my ($from, @to ) = @ids;
    my $table = $self->table_name;
    if( @to ) {
        if( defined $to[0] ) {
            my $places = join ',', ( '?' x @to );
            $self->db->query( qq{DELETE FROM $table WHERE edge_name=? AND id1=? AND id2 IN ($places)}, $self->edge_name, $from, @to );
        }
        else {  # undef => $id1, $id2, $id3
            shift @to;
            my $places = join ',', ( '?' x @to );
            $self->db->query( qq{DELETE FROM $table WHERE edge_name=? AND id2 IN ($places)}, $self->edge_name, @to );
        }
    }
    else {
        $self->db->query( qq{DELETE FROM $table WHERE edge_name=? AND id1=?}, $self->edge_name, $from );
    }
}

=head2 delete_all

Delete all edges of edge type.

=cut
sub delete_all {
    my ($self) = @_;
    my $table = $self->table_name;
    $self->db->query( qq{DELETE FROM $table WHERE edge_name = ? }, $self->edge_name );
}

1;
