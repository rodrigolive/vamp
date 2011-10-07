use strict;
use warnings;

use Test::More;
use YAML;

use Vamp;
my $db = Vamp->connect(db=>'vamp', args=>'dbi:SQLite:test.db' );
#my $db = Vamp->connect(db=>'vamp', args=>['dbi:Oracle://localhost:1521/SCM','gbp','gbp']  );
my $coll = $db->collection('person');
use Vamp::Query;
if(0) {
    Vamp::Query->parse({ name=>['jack','anna'] }); 
    # select oid from kv where key='name' and value='jack' or value='anna'
    # 
    Vamp::Query->parse({ name=>['jack','anna'], age=>20 }); 
    # select oid from kv where key='name' and value='jack' or value='anna'
    # intersect
    # select oid from kv where key='age' and value=20 
    print YAML::Dump( Vamp::Query->flatten({ name=>['jack','anna'], age=>20, names=>{ first=>'bob' } }) ); 
    Vamp::Query->gen({ name=>['jack','anna'], age=>20, names=>{ first=>'bob' } });
}
#{
    #$coll->db->query_findall('person' => { name=>{ first=>['Susie','Jack'] } });
#}
{
    my $rs = $coll->find({ name=>{ first =>[ 'Susie', 'Jack' ] } }); 
    while( my $r = $rs->next ) {
        warn Dump $r;
    }
    my @r = $coll->find({ name=>{ first =>[ 'Susie', 'Jack' ] } })->all;
    for( @r ) {
        warn ">" .  Dump $_;
    };
}

use Benchmark;
my $k = 0;
timethis( 600, sub{
    $coll->find({ name=>{ first =>[ 'Susie', 'Jack' ] } })->all; 
   #is $r->{age}, $k, 'age ok';
});
$k = 0;
timethis( 600, sub{
    #warn Dump $coll->find_all({ name=>{ first =>[ 'Susie', 'Jack' ] } }); 
    #map { print $_->{name}{first} } $coll->find_all({ name=>{ first =>[ 'Susie', 'Jack' ] } }); 
    $coll->find_all({ name=>{ first =>[ 'Susie', 'Jack' ] } }); 
});

done_testing;



