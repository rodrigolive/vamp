use strict;
use warnings;

use Test::More;

use Vamp;
use lib 't';
use VampTest;

my $db = test_db();
#$db->recreate;
my $jobs = $db->collection('jobs');
#$jobs->drop;

use Benchmark qw/:hireswallclock/;

print "Dump table tests\n";
my $k = 0;
if(0) { timethis( 1, sub{
    $jobs->insert_from_query( 
        query=>'select * from bali_job@gbp'
    );
});
};

warn "Optimize...";
{
    my $k = 0;
    $jobs->{db}->dbh->{LongReadLen} = 999999999;
    #$jobs->{db}->dbh->{LongTrunkOK} = 1;
    timethis( 30, sub{
       my $rs = $jobs->{db}->query(q{
            select id,document from vamp_obj
             WHERE collection='jobs' and rownum < 128
            order by vamp_obj.id
       });
       #warn $rs->text('box');
       my @rows = map { YAML::XS::Load $_->{document} } $rs->hashes;
       warn join ', ', keys %{ $rows[0] };
       $k += scalar @rows;
       warn $k;
    });
    warn "Total rows: $k";
}

{
    my $k = 0;
    timethis( 50, sub{
       my $rs = $jobs->{db}->query(q{select * from bali_job@gbp where name like '%3__'});
       my @rows = $rs->hashes;
       #$k += scalar map { keys %$_ } @rows;
       $k += scalar @rows;
       warn $k;
    });
    warn "Total rows: $k";
}


$k = 0;
print "Find_one tests\n";
timethis( 5, sub{
   my $rs = $jobs->find({ name=>{ -like => '%3__' } }); 
   #die yy $rs->all;
   my @rows = $rs->all;
   warn join ', ', keys %{ $rows[0] };
   $k += scalar @rows;
   # while( my $r = $rs->next ) {
   #     warn $r->{name};
   # }
   #warn $r->{name};
   #my $r = $jobs->find({ name=>"me" . $k++ })->first;  # ultraslow
   #is $r->{age}, $k, 'age ok';
});

warn "Total rows: $k";

{
    my $rs = $jobs->query({}, { order_by=>'name', rows=>5 });
}

done_testing;



