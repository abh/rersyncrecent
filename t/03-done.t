use Data::Dumper;
use File::Rsync::Mirror::Recentfile::Done;
use List::Util qw(sum);
use Test::More;
use strict;
my $tests;
BEGIN { $tests = 0 }
use lib "lib";

my @recent_events = map { +{ epoch => $_ } }
    (
     "1216706557.63601",
     "1216706557.5279",
     "1216706557.23439",
     "1216706555.44193",
     "1216706555.17699",
     "1216706554.23419",
     "1216706554.12319",
     "1216706553.47884",
     "1216706552.9627",
     "1216706552.661",
    );

{
    my @t;
    BEGIN {
        @t =
            (
             [[0,1,2],[3,4,5],[6,7,8,9]],
             [[9,8],[7,6,5],[4,3,2,1,0]],
             [[0,1,5],[3,4],[2,6,7,8,9]],
            );
        my $sum = sum map { my @cnt = @$_; scalar @cnt; } @t;
        $tests += $sum;
    }
    for my $t (@t) {
        my $done = File::Rsync::Mirror::Recentfile::Done->new;
        my @sessions = @$t;
        for my $i (0..$#sessions) {
            my $session = $sessions[$i];
            $done->register ( \@recent_events, $session );
            my $boolean = $done->covered ( map {$_->{epoch}} @recent_events[0,-1] );
            is 0+$boolean, $i==$#sessions ? 1 : 0 or
                die Dumper({boolean=>$boolean,i=>$i,done=>$done});
        }
    }
}

BEGIN { plan tests => $tests }

# Local Variables:
# mode: cperl
# cperl-indent-level: 4
# End:
