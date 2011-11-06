package Vamp::Edge;
use Any::Moose;

has edge_name  => is => 'rw', isa => 'Str',            required => 1;
has table_name => is => 'rw', isa => 'Str',            required => 1;
has db         => is => 'ro', isa => 'Vamp::Database', required => 1;
has rs         => is => 'rw', isa => 'Object';
has directed   => is => 'rw', isa => 'Bool',           default  => 1;

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
    return $self->rs->arrays;
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

1;
