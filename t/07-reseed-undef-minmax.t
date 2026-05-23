use strict;
use warnings;

# Regression test for _rmirror_reseed dying when the larger (next)
# recentfile in the chain has an undefined minmax->{max} while the
# smaller file already carries a defined merged->{epoch}.
#
# Before the undef guard in _rmirror_reseed, _bigfloatlt(undef, <def>)
# was reached and died with "but both must be defined", which happened
# in the forked daemon child and made rmirror(loop=>1) spin silently.

use Test::More;
use lib "lib";
use File::Rsync::Mirror::Recent;
use File::Rsync::Mirror::Recentfile;

# Build a tiny two-element chain by hand. We avoid setting interval so
# that merged()'s into_interval sanity checks stay quiet, and set the
# accessor-backed fields (_merged / minmax / _recentfiles) directly.
sub make_chain {
    my(%args) = @_;
    my $small = File::Rsync::Mirror::Recentfile->new;
    my $large = File::Rsync::Mirror::Recentfile->new;
    $small->merged($args{small_merged});
    # The larger file was never (successfully) fetched this run, so its
    # minmax has no max (Recentfile only sets min/max if there were
    # entries).
    $large->minmax($args{large_minmax});
    my $rec = File::Rsync::Mirror::Recent->new(_recentfiles => [$small, $large]);
    return ($rec, $small, $large);
}

{
    # Smaller file has a defined merged.epoch, larger file has undef
    # minmax.max -> must not die, and must (re)seed the larger file.
    my($rec, $small, $large) = make_chain(
        small_merged => { epoch => "1716000000.5" },
        large_minmax => { mtime => 1716000000 }, # no max key -> undef
    );
    is $large->seeded, 0, "larger file starts unseeded";
    my $ok = eval { $rec->_rmirror_reseed; 1 };
    ok $ok, "_rmirror_reseed survives undef minmax.max with defined merged.epoch"
        or diag "died: $@";
    is $large->seeded, 1, "larger file got (re)seeded because it is behind";
}

{
    # Smaller file has undef merged.epoch -> original behavior, the
    # first branch short-circuits and we still seed (and do not die).
    my($rec, $small, $large) = make_chain(
        small_merged => { epoch => undef },
        large_minmax => { mtime => 1716000000 },
    );
    my $ok = eval { $rec->_rmirror_reseed; 1 };
    ok $ok, "_rmirror_reseed survives undef merged.epoch";
    is $large->seeded, 1, "larger file got seeded when merged.epoch is undef";
}

done_testing;
