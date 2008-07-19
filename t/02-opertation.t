use Test::More;
my $tests;
BEGIN { $tests = 0 }
use lib "lib";
use File::Rsync::Mirror::Recentfile;

{
    BEGIN { $tests += 1 }
    my $rf = File::Rsync::Mirror::Recentfile->new_from_file("t/RECENT-6h.yaml");
    my $recent_events = @{$rf->recent_events};
    is(56, $recent_events, "found $recent_events events");
}

BEGIN { plan tests => $tests }

# Local Variables:
# mode: cperl
# cperl-indent-level: 4
# End:
