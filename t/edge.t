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
my $entries = $db->collection('entries');
my $tags = $db->collection('tags');
$entries->drop;
$tags->drop;

$entries->insert({ name=>'article1', author=>'jones' });
$tags->insert([
    { label=>'perl', owner=>'bob' },
    { label=>'scala', owner=>'bob' },
    { label=>'lisp', owner=>'bob' },
]);

{
    my $first = $entries->find_one;
    # first: $db->edge( tags => $first => $tags->all );
    my $edge = $db->edge( 'has_tag' );
    for my $tag ( $tags->all ) {
        #yy $first;
        #yy $tag;
        $edge->add( $first => $tag );
    }

    my $edges = $db->edge( 'has_tag' )->find_ids( $first );
    is_deeply $edges, [[1,2],[1,3],[1,4]], 'find_ids';
}
{
    my $people = $db->collection('people');
    my $id1 = $entries->insert({ name=>'things', author=>'yolanda' });
    my $id2 = $people->insert({ name=>'lucy' });
    my $e = $db->edge( 'children' );
    my $rel = $e->add( $id1 => $id2 );
    my $rs = $e->find;
    while( my $r = $rs->next ) {
        warn ">>>>>" . $r;
    }
    #$rel->data( priority=>10 ); 
    #is_deeply $e->find( $id1 ) => [$id2] => 'found edge child';
    #ok ! $e->find( 9999 ) => 'not found';
    
}

done_testing;
