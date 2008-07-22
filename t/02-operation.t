use Test::More;
my $tests;
BEGIN { $tests = 0 }
use lib "lib";
use File::Basename qw(dirname);
use File::Copy qw(cp);
use File::Path qw(mkpath rmtree);
use File::Rsync::Mirror::Recentfile;
use Storable;
use Time::HiRes qw(time sleep);
use YAML::Syck;

my $root_from = "t/ta";
my $root_to = "t/tb";

{
    BEGIN { $tests += 6 }
    my $rf = File::Rsync::Mirror::Recentfile->new_from_file("t/RECENT-6h.yaml");
    my $recent_events = $rf->recent_events;
    my $recent_events_cnt = scalar @$recent_events;
    is(56, $recent_events_cnt, "found $recent_events_cnt events");
    my $rf2 = Storable::dclone($rf);
    $rf2->interval("10s");
    $rf2->localroot($root_from);
    $rf2->comment("produced during the test 02-operation.t");
    $rf2->aggregator([qw(1m 2m 1h Z)]);
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
    my $dagg1 = $rf2->_debug_aggregate;
    sleep 1.5;
    my $file_from = "$root_from/anotherfile";
    open my $fh, ">", $file_from or die "Could not open: $!";
    print $fh time, ":", $file_from;
    close $fh or die "Could not close: $!";
    $rf2->update($file_from,"new");
    $rf2->aggregate;
    my $dagg2 = $rf2->_debug_aggregate;
    ok($dagg1->[0][1] < $dagg2->[0][1], "The 10s file size larger: $dagg1->[0][1] < $dagg2->[0][1]");
    ok($dagg1->[1][2] < $dagg2->[1][2], "The 1m file timestamp larger: $dagg1->[1][2] < $dagg2->[1][2]");
    is $dagg1->[2][1], $dagg2->[2][1], "The 2m file size unchanged";
    is $dagg1->[3][2], $dagg2->[3][2], "The 1h file timestamp unchanged";
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
