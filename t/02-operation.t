use Test::More;
use strict;
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
for my $root ($root_from, $root_to) {
    for my $lockfile (<$root/*.lock>) {
        rmdir $lockfile or die "Could not unlink '$lockfile': $!";
    }
}

{
    BEGIN { $tests += 38 }
    my $rf = File::Rsync::Mirror::Recentfile->new_from_file("t/RECENT-6h.yaml");
    my $recent_events = $rf->recent_events;
    my $recent_events_cnt = scalar @$recent_events;
    is (
        92,
        $recent_events_cnt,
        "found $recent_events_cnt events",
       );
    $rf->interval("10s");
    $rf->localroot($root_from);
    $rf->comment("produced during the test 02-operation.t");
    $rf->aggregator([qw(30s 1m 2m 1h Z)]);
    $rf->verbose(0);
    my $start = Time::HiRes::time;
    for my $e (@$recent_events) {
        for my $pass (0,1) {
            my $file = sprintf
                (
                 "%s/%s",
                 $pass==0 ? $root_from : $root_to,
                 $e->{path},
                );
            mkpath dirname $file;
            open my $fh, ">", $file or die "Could not open '$file': $!";
            print $fh time, ":", $file, "\n";
            close $fh or die "Could not close '$file': $!";
            if ($pass==0) {
                $rf->update($file,$e->{type});
            }
        }
    }
    $rf->aggregate;
    my $took = Time::HiRes::time - $start;
    ok $took > 0, "creating the tree and aggregate took $took seconds";
    my $dagg1 = $rf->_debug_aggregate;
    sleep 1.5;
    my $file_from = "$root_from/anotherfilefromtesting";
    open my $fh, ">", $file_from or die "Could not open: $!";
    print $fh time, ":", $file_from;
    close $fh or die "Could not close: $!";
    $rf->update($file_from,"new");
    $rf->aggregate;
    my $dagg2 = $rf->_debug_aggregate;
    ok($dagg1->[0][1] < $dagg2->[0][1], "The 10s file size larger: $dagg1->[0][1] < $dagg2->[0][1]");
    ok($dagg1->[1][2] < $dagg2->[1][2], "The 1m file timestamp larger: $dagg1->[1][2] < $dagg2->[1][2]");
    is $dagg1->[2][1], $dagg2->[2][1], "The 2m file size unchanged";
    is $dagg1->[3][2], $dagg2->[3][2], "The 1h file timestamp unchanged";
    ok -l "t/ta/RECENT.recent", "found the symlink";
    my $have_slept = my $have_worked = 0;
    $start = Time::HiRes::time;
    for my $i (0..30) {
        my $file = sprintf
            (
             "%s/secscnt%03d",
             $root_from,
             $i,
            );
        open my $fh, ">", $file or die "Could not open '$file': $!";
        print $fh time, ":", $file, "\n";
        close $fh or die "Could not close '$file': $!";
        $rf->update($file,"new");
        $rf->aggregate;
        my $rf2 = File::Rsync::Mirror::Recentfile->new_from_file("$root_from/RECENT-30s.yaml");
        my $rece = $rf2->recent_events;
        my $rececnt = @$rece;
        my $span = $rece->[0]{epoch} - $rece->[-1]{epoch};
        $have_worked = Time::HiRes::time - $start - $have_slept;
        ok($rececnt > 0 && $span < 30, "i[$i] cnt[$rececnt] span[$span] worked[$have_worked]");
        $have_slept += Time::HiRes::sleep 1;
    }
}

{
    BEGIN { $tests += 2 }
    my $rf = File::Rsync::Mirror::Recentfile->new
        (
         filenameroot   => "RECENT",
         interval       => q(1m),
         remote_dir     => $root_from,
         localroot      => $root_to,
         # verbose        => 1,
         rsync_options  => {
                           compress          => 0,
                           'rsync-path'      => '/usr/bin/rsync',
                           links             => 1,
                           times             => 1,
                           'omit-dir-times'  => 1,
                           checksum          => 0,
                          },
        );
    my $somefile_epoch;
    for my $pass (0,1) {
        my $success;
        if (0 == $pass) {
            $success = $rf->mirror;
            my $re = $rf->recent_events;
            $somefile_epoch = $re->[24]{epoch};
        } elsif (1 == $pass) {
            $success = $rf->mirror(after => $somefile_epoch);
        }
        ok($success, "mirrored with success");
    }
}


rmtree [$root_from, $root_to];

BEGIN { plan tests => $tests }

# Local Variables:
# mode: cperl
# cperl-indent-level: 4
# End:
