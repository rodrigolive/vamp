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
$coll->insert({ name=>{ first=>'Susie', last=>'Doe' }, age=>25 });
$coll->insert({ name=>'listy', age=>20, belongings=>[qw/house car boat/] });

{
    my $p = $coll->find_one({ name=>'joe' });
    is $p->{age}, 20, 'find_one';
}
{
    my $rs = $coll->find({ age => { '>=', 25 } });
    is $rs->count, 2, 'where compare with count';
}
{
    my $rs = $coll->find({ age => { '>=', 25 } }, { order_by=>'age' });
    is $rs->count, 2, 'where compare with count';
}
{
    my $p = $coll->find_one({ name=>'listy' });
    yy $p;
    is ref( $p->{belongings} ), 'ARRAY', 'array ok';
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
{
    $coll->insert({ name=>'longy', long_field_name_hard_to_store_cos_it_has_more_than_30_chars => 99 });
    my $obj = $coll->find_one({ name=>'longy' });
    is $obj->{long_field_name_hard_to_store_cos_it_has_more_than_30_chars}, 99, 'long field';
}

done_testing;

