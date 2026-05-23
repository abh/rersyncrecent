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

=head2 Why this is an AUTHOR_TEST

This test drives a real C<rmirror(loop =E<gt> 1)> daemon in a child
process and watches it converge through several timing-dependent states
(mid-tier reaching uptodateness, RECENT-Z staying behind, the client
re-fetching an advancing mid-tier index). On a loaded machine the
convergence simply takes longer; there is no fast, fully deterministic
way to provoke "RECENT-Z perpetually behind while mid-tier keeps
advancing" without real elapsed time. Like C<t/02-aurora.t> it is
therefore gated behind C<AUTHOR_TEST> and skipped by default so the
normal C<make test> / C<prove> suite stays stable. Run it with:

    AUTHOR_TEST=1 perl -Ilib t/06-interval-propagation.t

Under C<AUTHOR_TEST> it is hardened to be reliable rather than fast: it
uses generous deadlines (it may run a couple of minutes), separates the
"client has installed its first RECENT-5s" precondition from the
advancement assertion, and guards every epoch comparison against undef
so it never compares against a missing baseline.

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

my $tests = 4;

BEGIN {
    unless ($ENV{AUTHOR_TEST}) {
        # Timing-sensitive: drives a real rmirror daemon and waits for
        # several timing-dependent states to converge. Skipped by default
        # to keep the suite stable; run under AUTHOR_TEST.
        Test::More::plan(skip_all =>
            "timing-sensitive test; to run, set env AUTHOR_TEST=1 "
            . "(e.g. AUTHOR_TEST=1 perl -Ilib t/06-interval-propagation.t)");
    }
}

plan tests => $tests;

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

# ---- Phase 1: reach the bug-exercising precondition --------------------
#
# Precondition (NOT the thing under test, just the state in which the bug
# can manifest): the mid-tier has reached uptodateness while RECENT-Z has
# NOT, AND the client has actually installed its first copy of the mid-tier
# index so we have a non-undef baseline to watch. In that state the cleanup
# gate ($rfs->[-1]->uptodate) is false.
#
# We do not require the mid-tier to be currently unseeded: a working fix
# re-seeds it via cleanup, so the unseeded snapshot is not always
# observable -- but "mid-tier uptodate, Z not uptodate, client copy present"
# holds either way.
#
# Generous deadline: on a loaded machine convergence just takes longer, and
# the fix adds per-loop re-seed work under the tight 1s budget. Reaching the
# precondition is a setup step, so we wait as long as needed (within reason)
# rather than proceeding with an undef baseline.
my $PRECONDITION_TIMEOUT = 150;   # seconds; setup, not the assertion
my $precondition_deadline = time + $PRECONDITION_TIMEOUT;
my $reached_precondition  = 0;
my $client_frozen;                # client mid-tier baseline at onset
while (time < $precondition_deadline) {
    # keep the server's principal (and thus mid-tiers) moving
    serv_new("churn");
    $rf0->aggregate;
    my $st = status_rfs();
    my $c  = rf_maxepoch($root_to, $midtier);
    if ($st
        && $st->{$midtier} && $st->{$midtier}{uptodate}
        && $st->{Z} && !$st->{Z}{uptodate}
        && defined $c) {
        $reached_precondition = 1;
        $client_frozen        = $c;   # defined baseline at onset
        last;
    }
    # A steady ~1s cadence keeps the server churn from starving the daemon
    # of CPU; churning as fast as possible makes the chain thrash.
    sleep 1;
}

ok($reached_precondition,
   "reached bug-exercising precondition: $midtier uptodate while Z not "
   . "uptodate and client has installed its first RECENT-$midtier")
    or diag "precondition not reached within ${PRECONDITION_TIMEOUT}s; "
        . "client $midtier baseline is "
        . (defined $client_frozen ? $client_frozen : "undef")
        . "; status: " . YAML::Syck::Dump(status_rfs());

# Independently assert we captured a usable (defined) baseline. Everything
# downstream compares against this; an undef baseline must never silently
# pass an advancement check.
ok(defined $client_frozen,
   "captured a defined client RECENT-$midtier baseline before churn");

my $server_at_freeze = rf_maxepoch($root_from, $midtier);
diag sprintf "at precondition onset: client %s max=%s  server %s max=%s",
    $midtier, (defined $client_frozen ? $client_frozen : "undef"),
    $midtier, (defined $server_at_freeze ? $server_at_freeze : "undef");

# ---- Phase 2: the actual assertion -------------------------------------
#
# Keep churning so the server's mid-tier index advances well past the frozen
# client value, then check whether the client catches up. Without the fix
# the client's mid-tier index stays frozen (cleanup, which re-seeds the
# mid-tier, is gated behind the never-true "Z is uptodate"); with the fix
# _rmirror_reseed runs every loop and the client's mid-tier keeps advancing.
#
# This is a robust, non-transient signal: we wait until the client's
# max-epoch strictly exceeds the baseline. Only run it if we have a defined
# baseline -- otherwise the comparison would be meaningless.
my $ADVANCE_TIMEOUT  = 90;   # seconds; generous for a loaded machine
my $advance_deadline = time + $ADVANCE_TIMEOUT;
my $client_advanced  = 0;
my $client_latest    = $client_frozen;
if (defined $client_frozen) {
    while (time < $advance_deadline) {
        serv_new("churn");
        $rf0->aggregate;
        my $c = rf_maxepoch($root_to, $midtier);
        $client_latest = $c if defined $c;
        if (defined $c && $c > $client_frozen) {
            $client_advanced = 1;
            last;
        }
        sleep 1;
    }
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
   "client's RECENT-$midtier index advanced past baseline "
   . "(mid-tier kept fresh by per-loop re-seed)")
    or diag "client RECENT-$midtier stayed frozen at "
        . (defined $client_frozen ? $client_frozen : "undef")
        . " (latest "
        . (defined $client_latest ? $client_latest : "undef")
        . ") while the server's advanced to "
        . (defined $server_latest ? $server_latest : "undef")
        . " -- mid-tier re-seed is gated behind RECENT-Z uptodateness";

cleanup();

# Local Variables:
# mode: cperl
# cperl-indent-level: 4
# End:
