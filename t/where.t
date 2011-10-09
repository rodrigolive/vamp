use strict;
use warnings;

use Test::More;
use YAML;

# connect and drop
use lib 't';
use_ok 'Vamp';
use_ok 'VampTest';
my $db = test_db();
$db->recreate;
my $coll = $db->collection('person');
$coll->drop;
$coll->insert({ name=>{ first=>'Susie', last=>'Doe' }, age=>20 });
$coll->insert({ name=>{ first=>'Jack', last=>'Doe' }, age=>33 });
$coll->insert({ name=>{ first=>'James', last=>'Doe' }, age=>21 });

{
    my $objs = $coll->find({ age=>[33,20], name=>{ first=>{ '-like' => 'J%'} } });
    is $objs->first->{name}->{first},  'Jack', 'age arr find';
}
{
    my $objs = $coll->find({ -or =>[ age=>[21], name=>{ first=>{ '-like' => 'J%'} } ] });
    is $objs->first->{name}->{first},  'James', '-or arr find';
    #my $objs = $coll->find({ -or => [ age=>{ '>=' => 20 }, name=>{ first=>{ '-like' => 'J%'} } ] });
    #{
    #    -or => [ { key=>'age', value=>{ '>=' => 20 } }, { key=>'name.first' => value=>{ -like=>'J%' } } ]
    #} # then get unique instead of intersect
    #is $objs->first->{name}->{first},  'Susie', '-or arr find';
}

done_testing;

