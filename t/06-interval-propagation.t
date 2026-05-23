#!/usr/bin/perl

use strict;
use warnings;

=head1 NAME

06-interval-propagation.t - mid-tier RECENT index files must keep
advancing on a client while the slow C<RECENT-Z> file is behind

=head1 DESCRIPTION

Reproduces a production bug in C<rmirror(loop =E<gt> 1)> (the daemon mode
used by C<bin/rrr-client>).

A client mirrors a chain of recentfiles smallest-interval-first. Each loop
re-fetches the principal and processes the chain up to C<RECENT-Z.yaml>.
C<_rmirror_cleanup> re-seeds mid-tier interval files (so the client keeps
re-fetching them after they have reached uptodateness and unseeded), but it
is called only:

    if ($rfs->[-1]->uptodate) {        # $rfs->[-1] is RECENT-Z
        $self->_rmirror_cleanup;
    }

When C<RECENT-Z> is large and the per-loop time budget
(C<minimum_time_per_loop>) is small, a loop cannot bring C<Z> up to date.
Uptodateness for C<Z> is therefore never reached, the gate stays false and
C<_rmirror_cleanup> never runs. Meanwhile the smaller interval files reach
uptodateness, unseed, and -- with no re-seed -- stop being re-fetched.

The observable effect: as events age out of the principal into a mid-tier
(e.g. C<RECENT-5s.yaml>) on the server, the server's mid-tier index keeps
advancing, but the I<client's> copy of that index freezes. A mirror that
mirrors downstream of this client reads the frozen index and permanently
misses those events.

=head2 What this test does

  1. Build a server tree with a small working set plus a large Z-only
     backlog so that, under a tight budget, RECENT-Z can never finish.
  2. Run rmirror(loop => 1) in a child process.
  3. Wait until the runstatusfile shows the bug-triggering steady state:
     a mid-tier (5s) has reached uptodateness and unseeded, while Z has
     NOT reached uptodateness. (This proves we are exercising the gate,
     not a setup error or an over-starved chain.)
  4. Record the frozen max-epoch of the client's RECENT-5s.yaml, then keep
     churning the server so its RECENT-5s.yaml advances.
  5. Assert the client's RECENT-5s.yaml advances too.

Currently step 5 FAILS: the client's mid-tier index stays frozen because
_rmirror_cleanup is gated behind the never-true "Z is uptodate" condition.

C<minimum_time_per_loop> is a non-behavioral test seam (read/write
accessor / constructor option, defaulting to 20 so production is
unchanged).

=cut

use FindBin;
use lib "$FindBin::Bin/../lib";

use File::Path qw(mkpath rmtree);
use Time::HiRes qw(time sleep);
use Cwd ();
use File::Spec ();
use POSIX ":sys_wait_h";
use YAML::Syck ();

use Test::More;

use File::Rsync::Mirror::Recent;
use File::Rsync::Mirror::Recentfile;

my $root_from  = "t/serv";
my $root_to    = "t/mirr";
my $tmpdir     = "t/tmp";
my $statusfile = "t/recent-rmirror-state.yml";

sub cleanup {
    rmtree [$root_from, $root_to, $tmpdir];
    unlink $statusfile;
    unlink "$statusfile.new";
    rmdir "$statusfile.lock";
}

cleanup();
mkpath $_ for $root_from, $root_to, $tmpdir;
my $cwd = Cwd::cwd;

# Smallest interval first. Small steps so events age out of the principal
# quickly; Z is the slow full-history file.
my @intervals = qw( 2s 3s 5s 8s Z );
my $midtier   = "5s";

my $rf0 = File::Rsync::Mirror::Recentfile->new
    (
     aggregator    => [@intervals[1..$#intervals]],
     interval      => $intervals[0],
     localroot     => $root_from,
     rsync_options => {
                       compress => 0,
                       links    => 1,
                       times    => 1,
                       checksum => 0,
                      },
    );

my $fc = 0;
sub serv_new {
    my($dir) = @_;
    my $rel  = sprintf "%s/f%05d.txt", $dir, ++$fc;
    my $file = "$root_from/$rel";
    mkpath "$root_from/$dir";
    open my $fh, ">", $file or die "Could not open '$file': $!";
    print $fh "x" x 1024;
    close $fh or die "Could not close '$file': $!";
    $rf0->update($file, "new");
    return $rel;
}

# max epoch present in a recentfile, or undef
sub rf_maxepoch {
    my($root, $interval) = @_;
    my $f = "$root/RECENT-$interval.yaml";
    return undef unless -e $f;
    my $rf = File::Rsync::Mirror::Recentfile->new_from_file($f);
    my $re = $rf->recent_events;
    return @$re ? $re->[0]{epoch} : undef;
}

# read the per-interval reduced state from the runstatusfile
sub status_rfs {
    return undef unless -e $statusfile;
    my $y = eval { YAML::Syck::LoadFile($statusfile) };
    return undef unless $y;
    my %by_interval;
    for my $rf (@{ $y->{reduced_rfs} || [] }) {
        my $iv = $rf->{'-_interval'};
        next unless defined $iv;
        $by_interval{$iv} = {
            uptodate => $rf->{'-_uptodateness_ever_reached'} ? 1 : 0,
            seeded   => $rf->{'-_seeded'}                    ? 1 : 0,
        };
    }
    return \%by_interval;
}

# ---- Build the server tree ---------------------------------------------

# Small working set, aged into Z once so the chain has history.
serv_new("init") for 1..4;
$rf0->aggregate;
sleep 9;
$rf0->aggregate;

# Large Z-only backlog so a tight-budget loop can never finish RECENT-Z.
serv_new("bulk") for 1..150;
$rf0->aggregate;
sleep 9;
$rf0->aggregate;

# A little fresh content in the small intervals.
for (1..3) { serv_new("work"); $rf0->aggregate; sleep 0.4; }

# ---- Client: tight per-loop budget so RECENT-Z cannot finish -----------

my $rrr = File::Rsync::Mirror::Recent->new
    (
     ignore_link_stat_errors  => 1,
     localroot                => $root_to,
     remote                   => "$root_from/RECENT.recent",
     max_files_per_connection => 2,
     _runstatusfile           => $statusfile,
     minimum_time_per_loop    => 1,   # the seam: tight budget
     rsync_options            => {
                                  compress   => 0,
                                  links      => 1,
                                  times      => 1,
                                  checksum   => 0,
                                  'temp-dir' => "$cwd/$tmpdir",
                                 },
    );
is($rrr->minimum_time_per_loop, 1, "seam: minimum_time_per_loop is injectable");

# Per-connection sleeps keep RECENT-Z's sync time above the budget in a
# way that does not depend on raw rsync speed.
for my $rf (@{$rrr->recentfiles}) {
    $rf->sleep_per_connection(0.12);
}
$rrr->_rmirror_sleep_per_connection(0.001);

# ---- Run the daemon loop in a child ------------------------------------

my $pid = fork;
die "fork failed: $!" unless defined $pid;
if (!$pid) {
    # child: the rmirror daemon. Under a tight budget the loop repeatedly
    # probes RECENT files that do not exist yet and emits benign
    # "Could not stat ... No such file" warnings; silence them so the TAP
    # stream stays readable. The behaviour under test is unaffected.
    open STDERR, ">", File::Spec->devnull or warn "cannot reopen STDERR: $!";
    $rrr->rmirror(loop => 1);
    POSIX::_exit(0);
}

# Wait for the bug-triggering steady state. The condition is: the mid-tier
# has reached uptodateness while RECENT-Z has NOT, and the client has
# actually installed its copy of the mid-tier index (so we have a baseline
# to watch). In that state the cleanup gate ($rfs->[-1]->uptodate) is false.
# We do not require the mid-tier to be currently unseeded: a working fix
# re-seeds it via cleanup, so the unseeded snapshot is not always
# observable -- but "mid-tier uptodate, Z not uptodate" holds either way.
my $bug_state_deadline = time + 70;
my $reached_bug_state  = 0;
my $client_frozen;
while (time < $bug_state_deadline) {
    # keep the server's principal (and thus mid-tiers) moving
    serv_new("churn");
    $rf0->aggregate;
    my $st = status_rfs();
    my $c  = rf_maxepoch($root_to, $midtier);
    if ($st
        && $st->{$midtier} && $st->{$midtier}{uptodate}
        && $st->{Z} && !$st->{Z}{uptodate}
        && defined $c) {
        $reached_bug_state = 1;
        $client_frozen     = $c;   # baseline at onset
        last;
    }
    # A steady ~1s cadence keeps the server churn from starving the daemon
    # of CPU; churning as fast as possible makes the chain thrash.
    sleep 1;
}

ok($reached_bug_state,
   "reached bug-triggering state: $midtier uptodate while Z not uptodate")
    or diag "status: " . YAML::Syck::Dump(status_rfs());

my $server_at_freeze = rf_maxepoch($root_from, $midtier);
diag sprintf "at bug-state onset: client %s max=%s  server %s max=%s",
    $midtier, (defined $client_frozen ? $client_frozen : "undef"),
    $midtier, (defined $server_at_freeze ? $server_at_freeze : "undef");

# Keep churning so the server's mid-tier index advances well past the
# frozen client value, then check whether the client catches up. Without
# the fix the client's mid-tier index stays frozen; with cleanup re-seeding
# the mid-tier it advances.
my $advance_deadline = time + 45;
my $client_advanced  = 0;
my $client_latest    = $client_frozen;
while (time < $advance_deadline) {
    serv_new("churn");
    $rf0->aggregate;
    $client_latest = rf_maxepoch($root_to, $midtier);
    if (defined $client_latest && defined $client_frozen
        && $client_latest > $client_frozen) {
        $client_advanced = 1;
        last;
    }
    sleep 1;
}

my $server_latest = rf_maxepoch($root_from, $midtier);
diag sprintf "after churn: client %s max=%s  server %s max=%s",
    $midtier, (defined $client_latest ? $client_latest : "undef"),
    $midtier, (defined $server_latest ? $server_latest : "undef");

# Stop the daemon and reap.
kill 'TERM', $pid;
kill 'KILL', $pid;
waitpid($pid, 0);
1 while waitpid(-1, WNOHANG) > 0;

ok($client_advanced,
   "client's RECENT-$midtier index advanced (mid-tier kept fresh by cleanup)")
    or diag "client RECENT-$midtier stayed frozen at "
        . (defined $client_frozen ? $client_frozen : "undef")
        . " while the server's advanced -- mid-tier re-seed is gated behind "
        . "RECENT-Z uptodateness";

cleanup();
done_testing();

# Local Variables:
# mode: cperl
# cperl-indent-level: 4
# End:
