use strict;
use warnings;

use Test::More;
use YAML;

use Vamp;
#my $db = Vamp->connect(db=>'vamp', args=>"dbi:SQLite:dbname=:memory:" );
#my $db = Vamp->connect(db=>'vamp', args=>'dbi:SQLite:test.db' );
#my $db = Vamp->connect(db=>'vamp', args=>['dbi:Oracle://localhost:1521/SCM','gbp','gbp']  );
my $db = Vamp->connect(db=>'vamp', args=>['dbi:Oracle://orades:1550/ddboltp.gbp','uharvest','uharvest']  );
$db->recreate;
my $coll = $db->collection('person');
$coll->drop;
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
    warn Dump $row;
    is $row->{name}->{first}, 'bob', 'hash deep find';
    is_deeply $row->{family}->{kids}, ['kyle', 'lucy'], 'arr deep find';
}
use Benchmark;
my $k = 0;
timethis( 1000, sub{
   $coll->insert({ name=>"me" . $k++, age=>$k }); 
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

