use strict;
use warnings;

use Test::More;

use Vamp;
use lib 't';
use VampTest;

my $db = test_db();

$db->recreate;
my $coll = $db->collection('person');
$coll->drop;

use Benchmark;

my $k = 0;
timethis( 1000, sub{
   $k++;
   $coll->insert({ name=>"me" . $k, age=>$k });
});
$k = 0;
timethis( 300, sub{
   my $r = $coll->find_one({ name=>"me" . $k++ }); 
   #my $r = $coll->find({ name=>"me" . $k++ })->first;  # ultraslow
   #is $r->{age}, $k, 'age ok';
});

{
    my $rs = $coll->query({}, { order_by=>'name', rows=>5 });
}

done_testing;


