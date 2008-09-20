package File::Rsync::Mirror::Recentfile::Done;

# use warnings;
use strict;

# _bigfloat
sub _bigfloatcmp ($$);
sub _bigfloatgt ($$);
sub _bigfloatlt ($$);
sub _bigfloatmax ($$);
sub _bigfloatmin ($$);

=encoding utf-8

=head1 NAME

File::Rsync::Mirror::Recentfile::Done - intervals of already rsynced timespans

=head1 VERSION

Version 0.0.1

=cut

use version; our $VERSION = qv('0.0.1');

=head1 SYNOPSIS

 my $done = File::Rsync::Mirror::Recentfile::Done->new;
 $done->register ( $recent_events, [3,4,5,9] ); # registers elements 3-5 and 9
 my $boolean = $done->covered ( $epoch );

=head1 EXPORT

No exports.

=head1 CONSTRUCTORS

=head2 my $obj = CLASS->new(%hash)

Constructor. On every argument pair the key is a method name and the
value is an argument to that method name.

=cut

sub new {
    my($class, @args) = @_;
    my $self = bless {}, $class;
    while (@args) {
        my($method,$arg) = splice @args, 0, 2;
        $self->$method($arg);
    }
    return $self;
}

=head1 ACCESSORS

=cut

my @accessors;

BEGIN {
    @accessors = (
                  "_intervals",
                 );

    my @pod_lines =
        split /\n/, <<'=cut'; push @accessors, grep {s/^=item\s+//} @pod_lines; }

=over 4

=item verbose

Boolean to turn on a bit verbosity.

=back

=cut

use accessors @accessors;

=head1 METHODS

=head2 $boolean = $obj->covered ( $epoch1, $epoch2 )

=head2 $boolean = $obj->covered ( $epoch )

The first form returns true if both timestamps $epoch1 and $epoch2 in
floating point notation have been registered, otherwise false.

The second form returns true if this timestamp has been registered.

=cut

sub covered {
    my($self, $epoch_high, $epoch_low) = @_;
    my $intervals = $self->_intervals || [];
    return unless @$intervals;
    if (defined $epoch_low) {
        ($epoch_high,$epoch_low) = ($epoch_low,$epoch_high) if $epoch_low > $epoch_high;
    }
    for my $iv (@$intervals) {
        my($upper,$lower) = @$iv; # may be the same
        if (defined $epoch_low) {
            my $goodbound = 0;
            for my $e ($epoch_high,$epoch_low) {
                $goodbound++ if
                    $e eq $upper || $e eq $lower || ($e < $upper && $e > $lower);
            }
            return 1 if $goodbound > 1;
        } else {
            return 1 if $epoch_high eq $upper || $epoch_high eq $lower || ($epoch_high < $upper && $epoch_high > $lower);
        }
    }
    return 0;
}

=head2 (void) $obj1->merge ( $obj2 )

Integrates all intervals in $obj2 into $obj1. Overlapping intervals
are conflated. Sort order is preserved as decreasing.

=cut
sub merge {
    my($self, $other) = @_;
    my $intervals = $self->_intervals;
    my $ointervals = $other->_intervals;
  OTHER: for my $oiv (@$ointervals) {
        my $splicepos;
      SELF: for my $i (0..$#$intervals) {
            my $iv = $intervals->[$i];
            if ( _bigfloatlt ($oiv->[0],$iv->[1]) ) {
                # both oiv lower than iv => next
                next SELF;
            }
            if ( _bigfloatgt ($oiv->[1],$iv->[0]) ) {
                # both oiv greater than iv => insert
                $splicepos = $i;
                last SELF;
            }
            # larger(left-iv,left-oiv) becomes left, smaller(right-iv,right-oiv) becomes right
            $iv->[0] = _bigfloatmax ($oiv->[0],$iv->[0]);
            $iv->[1] = _bigfloatmin ($oiv->[1],$iv->[1]);
            next OTHER;
        }
        unless (defined $splicepos) {
            if ( _bigfloatlt ($oiv->[0], $intervals->[-1][1]) ) {
                $splicepos = @$intervals;
            } else {
                die "Panic: left-oiv[$oiv->[0]] should be smaller than smallest[$intervals->[-1][1]]";
            }
        }
        splice @$intervals, $splicepos, 0, [@$oiv];
    }
}

=head2 (void) $obj->register ( $recent_events_arrayref, $register_arrayref )

=head2 (void) $obj->register ( $recent_events_arrayref )

The first arrayref is a list fo hashes that contain a key called
C<epoch> which is a string looking like a number. The second arrayref
is a list if integers which point to elements in the first arrayref to
be registered.

The second form registers all events in $recent_events_arrayref.

=cut

sub register {
    my($self, $re, $reg) = @_;
    my $intervals = $self->_intervals || [];
    $self->_intervals ($intervals);
    unless ($reg) {
        $reg = [0..$#$re];
    }
  REGISTRANT: for my $i (@$reg) {
        die sprintf "Panic: illegal i[%d] larger than number of events[%d]", $i, $#$re
            if $i > $#$re;
        my $epoch = $re->[$i]{epoch};
        next REGISTRANT if $self->covered ( $epoch );
        if (@$intervals) {
            my $registered = 0;
            for my $iv (@$intervals) {
                my($upper,$lower) = @$iv; # may be the same
                if ($i > 0) {
                    if ($re->[$i-1]{epoch} eq $lower) {
                        $iv->[1] = $epoch;
                        $registered++;
                    }
                }
                if ($i < $#$re) {
                    if ($re->[$i+1]{epoch} eq $upper) {
                        $iv->[0] = $epoch;
                        $registered++;
                    }
                }
            }
            if ($registered == 2) {
                my $splicepos;
                for my $i (0..$#$intervals) {
                    if (   $epoch eq $intervals->[$i][1]
                        && $intervals->[$i][1] eq $intervals->[$i+1][0]) {
                        $intervals->[$i+1][0] = $intervals->[$i][0];
                        $splicepos = $i;
                        last;
                    }
                }
                if (defined $splicepos) {
                    splice @$intervals, $splicepos, 1;
                } else {
                    die "Panic: Could not find an interval position to insert '$epoch'";
                }
            } elsif ($registered == 1) {
            } else {
                my $splicepos;
                for my $i (0..$#$intervals) {
                    if (_bigfloatgt ($epoch, $intervals->[$i][0])) {
                        $splicepos = $i;
                        last;
                    }
                }
                unless (defined $splicepos) {
                    if (_bigfloatlt ($epoch,   $intervals->[-1][1])) {
                        $splicepos = @$intervals;
                    } else {
                        die "Panic: epoch[$epoch] should be smaller than smallest[$intervals->[-1][1]]";
                    }
                }
                splice @$intervals, $splicepos, 0, [($epoch)x2];
            }
        } else {
            $intervals->[0] = [($epoch)x2];
        }
    }
}

=head1 INTERNAL FUNCTIONS

These functions are not part of the public interface and can be
changed and go away any time without prior notice.

=head2 _bigfloatcmp ( $l, $r )

Cmp function for floating point numbers that have a longer
mantissa than can be handled by native perl floats.

=cut
sub _bigfloatcmp ($$) {
    my($l,$r) = @_;
    my $native = $l <=> $r;
    return $native if $native;
    for ($l, $r){
        $_ .= ".0" unless /\./;
    }
    $l =~ s/^/0/ while index($l,".") < index($r,".");
    $r =~ s/^/0/ while index($r,".") < index($l,".");
    $l cmp $r;
}

=head2 _bigfloatgt ( $l, $r )

Same for gt

=cut
sub _bigfloatgt ($$) {
    my($l,$r) = @_;
    _bigfloatcmp($l,$r) > 0;
}

=head2 _bigfloatlt ( $l, $r )

Same for lt

=cut
sub _bigfloatlt ($$) {
    my($l,$r) = @_;
    _bigfloatcmp($l,$r) < 0;
}

=head2 _bigfloatmax ( $l, $r )

Same for max (of two arguments)

=cut
sub _bigfloatmax ($$) {
    my($l,$r) = @_;
    return _bigfloatcmp($l,$r) >= 0 ? $l : $r;
}

=head2 _bigfloatmin ( $l, $r )

Same for min (of two arguments)

=cut
sub _bigfloatmin ($$) {
    my($l,$r) = @_;
    return _bigfloatcmp($l,$r) <= 0 ? $l : $r;
}

=head1 COPYRIGHT & LICENSE

Copyright 2008 Andreas König.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut

1; # End of File::Rsync::Mirror::Recentfile

# Local Variables:
# mode: cperl
# cperl-indent-level: 4
# End:
