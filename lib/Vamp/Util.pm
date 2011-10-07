package Vamp::Util;
use strict;
use Exporter::Tidy default=>[ qw/intersect/ ];

sub intersect {
    return @{ shift() } if @_ == 1;
    my %e = map { $_ => undef } @{ shift() };
    my @ret;
    for my $arr ( @_ ) {
        push @ret, grep { exists $e{$_} } @$arr;  
    }
    @ret;
}

1;
