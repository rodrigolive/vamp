use strict;
sub main::test_db {
    if( my $conn = $ENV{VAMP_TEST_CONNECT} ) {
        my @conn = split /,/, $conn; 
        return Vamp->db( db=>'vamp', backend=>shift(@conn), args=>\@conn, deploy=>0 );
    };
    return Vamp->db(db=>'vamp', backend=>'SQLite', args=>"dbi:SQLite:dbname=:memory:" );
    #return Vamp->connect(db=>'vamp', args=>'dbi:SQLite:test.db' );
    #return Vamp->db(db=>'vamp', backend=>'Oracle', args=>['dbi:Oracle://localhost:1521/SCM','vamp','vamp'], deploy=>0  );
    #return Vamp->connect(db=>'vamp', args=>['dbi:Oracle://orades:1550/ddboltp.gbp','uharvest','uharvest']  );
}

sub main::yy {
    require YAML;
    warn "~" x 40, "\n", YAML::Dump( @_ ), "^" x 40, "\n";
}
1;
