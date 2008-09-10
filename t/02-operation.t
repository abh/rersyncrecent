use Test::More;
use strict;
my $tests;
BEGIN { $tests = 0 }
use lib "lib";


my $HAVE;
BEGIN {
    # neither LibMagic nor MMagic tell them apart
    for my $package (
                     # "File::LibMagic",
                     "File::MMagic",
                    ) {
        $HAVE->{$package} = eval qq{ require $package; };
    }
}

use Dumpvalue;
use File::Basename qw(dirname);
use File::Copy qw(cp);
use File::Path qw(mkpath rmtree);
use File::Rsync::Mirror::Recentfile;
use Storable;
use Time::HiRes qw(time sleep);
use YAML::Syck;

my $root_from = "t/ta";
my $root_to = "t/tb";
rmtree [$root_from, $root_to];

{
    my @serializers;
    BEGIN {
        @serializers = (
                        ".yaml",
                        ".json",
                        ".sto",
                        ".dd",
                       );
        $tests += @serializers;
        if ($HAVE->{"File::LibMagic"}||$HAVE->{"File::MMagic"}) {
            $tests += @serializers;
        }
    }
    mkpath $root_from;
    my $ttt = "$root_from/ttt";
    open my $fh, ">", $ttt or die "Could not open: $!";
    print $fh time;
    close $fh or die "Could not close: $!";
    my $fm;
    if ($HAVE->{"File::LibMagic"}) {
        $fm = File::LibMagic->new();
    } elsif ($HAVE->{"File::MMagic"}) {
        $fm = File::MMagic->new();
    }
    for my $s (@serializers) {
        my $rf = File::Rsync::Mirror::Recentfile->new
            (
             filenameroot   => "RECENT",
             interval       => q(1m),
             localroot      => $root_from,
             serializer_suffix => $s,
            );
        $rf->update($ttt,"new");
        if ($fm) {
            my $magic = $fm->checktype_filename("$root_from/RECENT-1m$s");
            ok($magic, sprintf
               ("Got a magic[%s] for s[%s]: [%s]",
                ref $fm,
                $s,
                $magic,
               ));
        }
        my $content = do {open my $fh, "$root_from/RECENT-1m$s";local $/;<$fh>};
        $content = Dumpvalue->new()->stringify($content);
        my $want_length = 42; # or maybe 3 more
        substr($content,$want_length) = "..." if length $content > 3+$want_length;
        ok($content, "Got a substr for s[$s]: [$content]");
    }
}

rmtree [$root_from, $root_to];

{
    my @intervals;
    BEGIN {
        $tests += 13;
        @intervals = qw( 2s 4s 8s 16s 32s Z );
    }
    is 6, scalar @intervals, "array has 6 elements: @intervals";
    my $rf0 = File::Rsync::Mirror::Recentfile->new
        (
         aggregator     => [@intervals[1..$#intervals]],
         interval       => $intervals[0],
         localroot      => $root_from,
         rsync_options  => {
                            compress          => 0,
                            links             => 1,
                            times             => 1,
                            checksum          => 0,
                           },
        );
    for my $iv (@intervals) {
        for my $i (0..3) {
            my $file = sprintf
                (
                 "%s/A%s-%02d",
                 $root_from,
                 $iv,
                 $i,
                );
            mkpath dirname $file;
            open my $fh, ">", $file or die "Could not open '$file': $!";
            print $fh time, ":", $file, "\n";
            close $fh or die "Could not close '$file': $!";
            $rf0->update($file,"new");
        }
    }
    my $recent_events = $rf0->recent_events;
    # faking internals as if the contents were wide-spread in time
    for my $evi (0..$#$recent_events) {
        my $ev = $recent_events->[$evi];
        $ev->{epoch} -= 2**($evi*.25);
    }
    $rf0->write_recent($recent_events);
    $rf0->aggregate;
    my $filesize_threshold = 1750; # XXX may be system dependent
    for my $iv (@intervals) {
        my $rf = "$root_from/RECENT-$iv.yaml";
        my $filesize = -s $rf;
        # now they have $filesize_threshold+ bytes because they were merged for the
        # first time ever and could not be truncated for this reason.
        ok($filesize_threshold < $filesize, "file $iv has good size[$filesize]");
        utime 0, 0, $rf; # so that the next aggregate isn't skipped
    }
    open my $fh, ">", "$root_from/finissage" or die "Could not open: $!";
    print $fh "fin";
    close $fh or die "Could not close: $!";
    $rf0->update("$root_from/finissage","new");
    $rf0 = File::Rsync::Mirror::Recentfile->new_from_file("$root_from/RECENT-2s.yaml");
    $rf0->aggregate;
    for my $iv (@intervals) {
        my $filesize = -s "$root_from/RECENT-$iv.yaml";
        # now they have <$filesize_threshold bytes because the second aggregate could
        # truncate them
        ok($iv eq "Z" || $filesize_threshold > $filesize, "file $iv has good size[$filesize]");
    }
}

rmtree [$root_from, $root_to];

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
    $rf->interval("5s");
    $rf->localroot($root_from);
    $rf->comment("produced during the test 02-operation.t");
    $rf->aggregator([qw(10s 30s 1m 1h Z)]);
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
    for my $i (1..5) {
        my $file_from = "$root_from/anotherfilefromtesting$i";
        open my $fh, ">", $file_from or die "Could not open: $!";
        print $fh time, ":", $file_from;
        close $fh or die "Could not close: $!";
        $rf->update($file_from,"new");
    }
    $rf->aggregate;
    my $dagg2 = $rf->_debug_aggregate;
    undef $rf;
    # $DB::single=1;
    ok($dagg1->[0][1] < $dagg2->[0][1], "The second 5s file size larger: $dagg1->[0][1] < $dagg2->[0][1]");
    ok($dagg1->[1][2] <= $dagg2->[1][2], "The second 30s file timestamp larger: $dagg1->[1][2] < $dagg2->[1][2]");
    is $dagg1->[2][1], $dagg2->[2][1], "The 1m file size unchanged";
    is $dagg1->[3][2], $dagg2->[3][2], "The 1h file timestamp unchanged";
    ok -l "t/ta/RECENT.recent", "found the symlink";
    my $have_slept = my $have_worked = 0;
    $start = Time::HiRes::time;
    for my $i (0..30) {
        my $file = sprintf
            (
             "%s/secscnt%03d",
             $root_from,
             $i % 12,
            );
        open my $fh, ">", $file or die "Could not open '$file': $!";
        print $fh time, ":", $file, "\n";
        close $fh or die "Could not close '$file': $!";
        my $another_rf = File::Rsync::Mirror::Recentfile->new
            (
             interval => "5s",
             localroot => $root_from,
             aggregator => [qw(10s 30s 1m 1h Z)],
            );
        $another_rf->update($file,"new");
        $another_rf->aggregate;
        my $rf2 = File::Rsync::Mirror::Recentfile->new_from_file("$root_from/RECENT-10s.yaml");
        my $rece = $rf2->recent_events;
        my $rececnt = @$rece;
        my $span = $rece->[0]{epoch} - $rece->[-1]{epoch};
        $have_worked = Time::HiRes::time - $start - $have_slept;
        ok($rececnt > 0 && $span < 30, "i[$i] cnt[$rececnt] span[$span] worked[$have_worked]");
        $have_slept += Time::HiRes::sleep 0.2;
    }
}

{
    BEGIN { $tests += 2 }
    my $rf = File::Rsync::Mirror::Recentfile->new
        (
         filenameroot   => "RECENT",
         interval       => q(30s),
         localroot      => $root_to,
         max_rsync_errors  => 0,
         remote_dir     => $root_from,
         # verbose        => 1,
         rsync_options  => {
                            compress          => 0,
                            links             => 1,
                            times             => 1,
                            # not available in rsync 3.0.3: 'omit-dir-times'  => 1,
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
