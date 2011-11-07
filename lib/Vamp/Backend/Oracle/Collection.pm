package Vamp::Backend::Oracle::Collection;
use Any::Moose;
with 'Vamp::DBI::Collection';

use constant rs_class => 'Vamp::Backend::Oracle::ResultSet';

1;
