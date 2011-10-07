use rig red;
use DBIx::NoSQLite;
use Benchmark;
 
my $store = DBIx::NoSQLite->connect( 'test.db' );
#my $store = DBIx::NoSQL->connect( 'dbi:Oracle://localhost:1521/SCM','gbp','gbp' );
 
my $k = 0;
timethis( 1000, sub{
    $store->set( 'person' => "me" . $k++ => { age=>$k } );
});
$k = 0;
timethis( 1000, sub{
    my $p = $store->get( 'person' => "me" . $k++ );
});
 
 
#$store->get( ... );
#$store->exists( ... );
#$store->delete( ... );
#$store->search( ... );
 

