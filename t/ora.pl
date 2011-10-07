use strict;
use DBI;
use DBD::Oracle qw(:ora_types);

my $dbh = DBI->connect( 'dbi:Oracle://localhost:1521/SCM','gbp','gbp' );
my $sth = $dbh->prepare("select * from voodoo_kv where value = 'joe'" );
$sth->execute;
