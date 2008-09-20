use Data::Dumper;
use File::Rsync::Mirror::Recentfile::Done;
use List::Util qw(sum);
use Storable qw(dclone);
use Test::More;
our $HAVE_YAML_SYCK;
BEGIN { $HAVE_YAML_SYCK = eval { require YAML::Syck; 1; }; }
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

# lm = long mantissa
my @recent_events_lm = map { +{ epoch => $_ } }
    (
     "100.0000000000000001116606557906601",
     "100.0000000000000001116606557806690",
     "100.0000000000000001116606557706639",
     "100.0000000000000001116606557606693",
     "100.0000000000000001116606557506699",
      "99.9999999999999991116606557406619",
      "99.9999999999999991116606557306619",
      "99.9999999999999991116606557206684",
      "99.9999999999999991116606557106670",
      "99.9999999999999991116606557006600",
    );

my @snapshots;

{
    my @t;
    BEGIN {
        @t =
            (
             [[0,1,2],[3,4,5],[6,7,8,9]],
             [[9,8],[7,6,5],[4,3,2,1,0]],
             [[0,1,5],[3,4],[2,6,7,8,9]],
             [[1,5],[3,4,5,7],[2,0,6,7,9,8]],
            );
        my $sum = sum map { my @cnt = @$_; scalar @cnt; } @t;
        $tests += 2 * $sum;
    }
    for my $t (@t) {
        my $done = File::Rsync::Mirror::Recentfile::Done->new;
        my $done_lm = File::Rsync::Mirror::Recentfile::Done->new;
        my @sessions = @$t;
        for my $i (0..$#sessions) {
            my $session = $sessions[$i];

            $done->register ( \@recent_events, $session );
            my $boolean = $done->covered ( map {$_->{epoch}} @recent_events[0,-1] );
            is 0+$boolean, $i==$#sessions ? 1 : 0, $recent_events[$session->[0]]{epoch} or
                die Dumper({boolean=>$boolean,i=>$i,done=>$done});

            $done_lm->register ( \@recent_events_lm, $session );
            my $boolean_lm = $done_lm->covered ( map {$_->{epoch}} @recent_events_lm[0,-1] );
            is 0+$boolean_lm, $i==$#sessions ? 1 : 0, $recent_events_lm[$session->[0]]{epoch}  or
                die Dumper({boolean_lm=>$boolean_lm,i=>$i,done_lm=>$done_lm});

            push @snapshots, dclone $done, dclone $done_lm;
        }
    }
}

{
    BEGIN {
        $tests += 1;
        if ($HAVE_YAML_SYCK) {
            $tests += 1;
        }
    }
    my $snapshots = scalar @snapshots;
    ok $snapshots>=24, "enough snapshots[$snapshots]";
    my $ok = 0;
    for my $i (0..$#snapshots) {
        my($a) = [@snapshots[$i-1,$i]];
        my $b = dclone $a;
        $a->[0]->merge($a->[1]);
        $b->[1]->merge($b->[0]);
        if ($HAVE_YAML_SYCK) {
            $ok++ if YAML::Syck::Dump($a->[0]) eq YAML::Syck::Dump($b->[1]);
        }
    }
    if ($HAVE_YAML_SYCK) {
        is $ok, $snapshots, "all merge operations OK";
    }
}

BEGIN { plan tests => $tests }

# Local Variables:
# mode: cperl
# cperl-indent-level: 4
# End:
