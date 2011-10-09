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

my $company = $db->collection('company');

# base data
$company->insert({ name=>'joe', age=>55 });
$company->insert({ name=>'joe', age=>55 });

$coll->insert({ name=>'joe', age=>20 });
$coll->insert({ name=>'jack', age=>33 });
$coll->insert({ name=>{ first=>'Susie', last=>'Doe' }, age=>20 });
$coll->insert({ name=>'listy', age=>20, belongings=>[qw/house car boat/] });

{
    my $p = $coll->find_one({ name=>'joe' });
    is $p->{age}, 20, 'find_one';
}
{
    my $p = $coll->find_one({ name=>'listy' });
    yy $p;
    is $p->{belongings}->[0], 'house', 'array find';
}
{
    my $objs = $coll->find({ age=>33 });
    is $objs->first->{name},  'jack', 'arr find';
}
{
    my $objs = $coll->find({ age=>[33,20], name=>{ '-like' => '%jac%' } });
    is $objs->first->{name},  'jack', 'age arr find';
}
{
    $coll->insert({ name=>{ first=>'bob', last=>'baz'}, age=>75, family=>{ kids=>['kyle','lucy'] } });
    $coll->insert({ name=>{ first=>'helen', last=>'baz'}, age=>71 });
    my $objs = $coll->find({ name=>{ last=>'baz' } });
    my $row = $objs->first;
    yy $row;
    is $row->{name}->{first}, 'bob', 'hash deep find';
    is_deeply $row->{family}->{kids}, ['kyle', 'lucy'], 'arr deep find';
}
#{
#    my $rs = $coll->query({}, { order_by=>'name', rows=>5 });
#}

done_testing;

