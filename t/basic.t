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
my $people = $db->collection('people');
$people->drop;

my $company = $db->collection('company');
my $nums = $db->collection('numbers');

# base data
$company->insert({ name=>'joe', age=>55 });
$company->insert({ name=>'joe', age=>55 });

$people->insert({ name=>'joe', age=>20 });
$people->insert({ name=>'jack', age=>33 });
$people->insert({ name=>{ first=>'Susie', last=>'Doe' }, age=>25 });
$people->insert({ name=>'listy', age=>20, belongings=>[qw/house car boat/] });

{
    my $p = $people->find_one({ name=>'joe' });
    is $p->{age}, 20, 'find_one';
    ok defined $p->{id}, 'id returned';
}
{
    my $rs = $people->find({ age => { '>=', 25 } });
    is ref $rs->as_query, 'ARRAY', 'as_query ok';
    is $rs->count, 2, 'where compare with count';
}
{
    my $rs = $people->find({ age => { '>=', 25 } }, { order_by=>'age' });
    is $rs->count, 2, 'where compare with count and order_by';
}
{
    $nums->insert({ num=>$_ }) for 1..100;
    my $rs = $nums->find({}, { start => 50, order_by=>'num', hint=>{ num=>'number' } });
    #warn $rs->as_query->[0];
    is $rs->first->{num}, 50, 'start ok';
}
{
    my @all = $nums->find->all;
    is scalar @all, 100, 'array all';
}
{
    my $rs = $nums->find({}, { start => 10, limit=>9, order_by=>'num', hint=>{ num=>'number' } });
    #yy $rs->as_query;
    my @all = $rs->all;
    is $all[0]->{num}, 10, 'start limit ok';
    is $all[8]->{num}, 18, 'start + limit ok';
    is $all[9], undef, 'start + limit +1 ok';
    is scalar @all, 9, 'limit at 10';
}
{
    my $p = $people->find_one({ name=>'listy' });
    is ref( $p->{belongings} ), 'ARRAY', 'array ok';
    is $p->{belongings}->[0], 'house', 'array find';
}
{
    my $objs = $people->find({ age=>33 });
    is $objs->first->{name},  'jack', 'arr find';
}
{
    my $objs = $people->find({ age=>[33,20], name=>{ '-like' => '%jac%' } });
    is $objs->first->{name},  'jack', 'age arr find';
}
{
    $people->insert({ name=>{ first=>'bob', last=>'baz'}, age=>75, family=>{ kids=>['kyle','lucy'] } });
    $people->insert({ name=>{ first=>'helen', last=>'baz'}, age=>71 });
    my $objs = $people->find({ name=>{ last=>'baz' } });
    my $row = $objs->first;
    is $row->{name}->{first}, 'bob', 'hash deep find';
    is_deeply $row->{family}->{kids}, ['kyle', 'lucy'], 'arr deep find';
}
{
    my $f = $people->find({ name=>{ first=>'bob' } }, { select=>['name.first'] })->first;
    is $f->{name}->{first}, 'bob', 'select one field data';
    is scalar keys %$f, 2, 'select one field'; # 2, one is the id
}
{
    $people->insert({ name=>'longy', long_field_name_hard_to_store_cos_it_has_more_than_30_chars => 99 });
    my $obj = $people->find_one({ name=>'longy' });
    is $obj->{long_field_name_hard_to_store_cos_it_has_more_than_30_chars}, 99, 'long field';
}
{
    $people->insert({ id=>"reggie", gender=>"male", name=>'Reggie', age=>76 });
    my $f = $people->find_one('reggie');
    is $f->{gender}, 'male', 'custom id';
}
{
    $people->update( 'reggie' => { age => 22 } ); 
    my $f = $people->find_one('reggie');
    is $f->{age}, 22, 'update';
}
{
    $people->upsert( 'naomi' => { age => 20 } ); 
    my $f = $people->find_one('naomi');
    is $f->{age}, 20, 'upsert';
    $people->upsert( 'naomi' => { age => 33 } ); 
    my $f2 = $people->find_one('naomi');
    is $f2->{age}, 33, 'upsert again';
}

done_testing;

