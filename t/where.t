use strict;
use warnings;

use Test::More;
use YAML;

use Vamp;
#my $db = Vamp->connect(db=>'vamp', args=>"dbi:SQLite:dbname=:memory:" );
my $db = Vamp->connect(db=>'vamp', args=>'dbi:SQLite:test.db' );
#my $db = Vamp->connect(db=>'vamp', args=>['dbi:Oracle://localhost:1521/SCM','gbp','gbp']  );
$db->recreate;
my $coll = $db->collection('person');
$coll->drop;
$coll->insert({ name=>{ first=>'Susie', last=>'Doe' }, age=>20 });
$coll->insert({ name=>{ first=>'Jack', last=>'Doe' }, age=>33 });
$coll->insert({ name=>{ first=>'James', last=>'Doe' }, age=>33 });

{
    my $objs = $coll->find({ age=>[33,20], name=>{ first=>{ '-like' => 'J%'} } });
    is $objs->[0]->{name}->{first},  'Jack', 'age arr find';
}
{
    my $objs = $coll->find({ -or => [ age=>{ '>=' => 20 }, name=>{ first=>{ '-like' => 'J%'} } ] });
    {
        -or => [ { key=>'age', value=>{ '>=' => 20 } }, { key=>'name.first' => value=>{ -like=>'J%' } } ]
    } # then get unique instead of intersect
    is $objs->[0]->{name}->{first},  'Susie', '-or arr find';
}

done_testing;


