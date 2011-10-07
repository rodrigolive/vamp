use rig red;
use MongoDB;
use Benchmark;
use Test::More;

my $conn = MongoDB::Connection->new;
my $db = $conn->foo;
my $coll = $db->bar;

my $k = 0;
timethis( 10000, sub{
    $coll->insert({ name=>"me" . $k++, age=>$k }); 
});
$k = 0;
timethis( 10000, sub{
    my $p = $coll->find_one({ name => "me" . $k++ });
    #is $p->{age}, $k, 'age ok';
});

