use strict;
use warnings;

use Test::More;

use Vamp;
use lib 't';
use VampTest;
#use Test::LeakTrace;

# connect and drop
my $db = test_db();
$db->recreate;
my $coll = $db->collection('person');
$coll->drop;
{
    my $h = { name=>'lots' };
    $h->{ $_ } = $_ for 1..990;
    $coll->insert( $h );
    my $obj = $coll->find_one({ name=>'lots' });
    is $obj->{990}, 990, '1000 keys';
    $obj = $coll->find_one( $h );
    is $obj->{990}, 990, '1000 keys - find all keys';
}
#{
#    my $rs = $coll->query({}, { order_by=>'name', rows=>5 });
#}

done_testing;


