use Test::More;
my $tests;
BEGIN { $tests = 0 }
use lib "lib";
use File::Rsync::Mirror::Recentfile;

{
    BEGIN { $tests += 5 }
    my $rf = File::Rsync::Mirror::Recentfile->new;
    for my $x ([-12, undef],
               [0, 0],
               [undef, undef],
               [(12)x2],
               [(12000000000000)x2],
              ) {
        my $ret = eval { $rf->interval_secs(defined $x->[0] ? $x->[0] . "s" : ()); };
        is $ret, $x->[1];
    }
}

{
    BEGIN { $tests += 5 }
    my $rf = File::Rsync::Mirror::Recentfile->new;
    for my $x (["12s" => 12],
               ["12m" => 720],
               ["2h"  => 7200],
               ["1d"  => 86400],
               ["4Q"  => 31104000],
              ) {
        my $ret = $rf->interval_secs ( $x->[0] );
        is $ret, $x->[1];
    }
}

BEGIN { plan tests => $tests }

# Local Variables:
# mode: cperl
# cperl-indent-level: 4
# End:
