use Test::More;
my $tests;
BEGIN { $tests = 0 }
use lib "lib";
use File::Basename qw(dirname);
use File::Copy qw(cp);
use File::Path qw(mkpath rmtree);
use File::Rsync::Mirror::Recentfile;
use Storable;
use Time::HiRes qw(time);

my $root_from = "t/ta";
my $root_to = "t/tb";

{
    BEGIN { $tests += 2 }
    my $rf = File::Rsync::Mirror::Recentfile->new_from_file("t/RECENT-6h.yaml");
    my $recent_events = $rf->recent_events;
    my $recent_events_cnt = scalar @$recent_events;
    is(56, $recent_events_cnt, "found $recent_events_cnt events");
    my $rf2 = Storable::dclone($rf);
    $rf2->interval("1m");
    $rf2->localroot($root_from);
    $rf2->comment("produced during the test 02-operation.t");
    $rf2->aggregator([qw(1h Z)]);
    $rf2->verbose(0);
    my $start = Time::HiRes::time;
    for my $e (@$recent_events) {
        my $file_from = sprintf "%s/%s", $root_from, $e->{path};
        mkpath dirname $file_from;
        open my $fh, ">", $file_from or die "Could not open '$file_from': $!";
        print $fh time, ":", $file_from, "\n";
        close $fh or die "Could not close '$file_from': $!";
        $rf2->update($file_from,$e->{type});
    }
    $rf2->aggregate;
    my $took = Time::HiRes::time - $start;
    ok $took > 0, "creating the tree and aggregate took $took seconds";
}

{
    BEGIN { $tests += 1 }
    my $rf = File::Rsync::Mirror::Recentfile->new
        (
         filenameroot => "RECENT",
         interval => q(1m),
         remote_dir => $root_from,
         localroot => $root_to,
         rsync_options => {
                           compress => 0,
                           'rsync-path' => '/usr/bin/rsync',
                           links => 1,
                           times => 1,
                           'omit-dir-times' => 1,
                           checksum => 0,
                          },
        );
    my $success = $rf->mirror;
    ok($success, "mirrored with success");
}


rmtree [$root_from, $root_to];

BEGIN { plan tests => $tests }

# Local Variables:
# mode: cperl
# cperl-indent-level: 4
# End:
