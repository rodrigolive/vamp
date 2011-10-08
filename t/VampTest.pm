
sub main::test_db {
    #return Vamp->connect(db=>'vamp', args=>"dbi:SQLite:dbname=:memory:" );
    #return Vamp->connect(db=>'vamp', args=>'dbi:SQLite:test.db' );
    return Vamp->connect(db=>'vamp', args=>['dbi:Oracle://localhost:1521/SCM','gbp','gbp']  );
    #return Vamp->connect(db=>'vamp', args=>['dbi:Oracle://orades:1550/ddboltp.gbp','uharvest','uharvest']  );
}

1;
