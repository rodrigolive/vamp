use strict;
sub main::test_db {
    if( my $conn = $ENV{VAMP_TEST_CONNECT} ) {
        return Vamp->connect( db=>'vamp', args=>[ split /,/, $conn ] );
    };
    #return Vamp->connect(db=>'vamp', args=>"dbi:SQLite:dbname=:memory:" );
    #return Vamp->connect(db=>'vamp', args=>'dbi:SQLite:test.db' );
    return Vamp->connect(db=>'vamp', args=>['dbi:Oracle://localhost:1521/SCM','gbp','gbp']  );
    #return Vamp->connect(db=>'vamp', args=>['dbi:Oracle://orades:1550/ddboltp.gbp','uharvest','uharvest']  );
}

sub main::yy {
    require YAML;
    warn "~" x 40, "\n", YAML::Dump( @_ ), "^" x 40, "\n";
}
1;
