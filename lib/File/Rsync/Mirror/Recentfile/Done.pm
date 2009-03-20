package File::Rsync::Mirror::Recentfile::Done;

# use warnings;
use strict;

use File::Rsync::Mirror::Recentfile::FakeBigFloat qw(:all);

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

=head1 DESCRIPTION

Keeping track of already rsynced timespans.

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
                  "__intervals",
                  "_logfile", # undocced: a small yaml dump appended on every change
                  "_rfinterval", # undocced: the interval of the holding rf
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
floating point notation have been registered within one interval,
otherwise false.

The second form returns true if this timestamp has been registered.

=cut

sub covered {
    my($self, $epoch_high, $epoch_low) = @_;
    die "Alert: covered() called without or with undefined first argument" unless defined $epoch_high;
    my $intervals = $self->_intervals;
    return unless @$intervals;
    if (defined $epoch_low) {
        ($epoch_high,$epoch_low) = ($epoch_low,$epoch_high) if _bigfloatgt($epoch_low,$epoch_high);
    }
    for my $iv (@$intervals) {
        my($upper,$lower) = @$iv; # may be the same
        $DB::single = $DB::single = not(defined $upper and defined $lower);
        if (defined $epoch_low) {
            my $goodbound = 0;
            for my $e ($epoch_high,$epoch_low) {
                $goodbound++ if
                    $e eq $upper || $e eq $lower || (_bigfloatlt($e,$upper) && _bigfloatgt($e,$lower));
            }
            return 1 if $goodbound > 1;
        } else {
            return 1 if _bigfloatle($epoch_high,$upper) && _bigfloatge($epoch_high, $lower); # "between"
        }
    }
    return 0;
}

=head2 (void) $obj1->merge ( $obj2 )

Integrates all intervals in $obj2 into $obj1. Overlapping intervals
are conflated/folded/consolidated. Sort order is preserved as decreasing.

=cut
sub merge {
    my($self, $other) = @_;
    my $intervals = $self->_intervals;
    my $ointervals = $other->_intervals;
  OTHER: for my $oiv (@$ointervals) {
        my $splicepos;
        if (@$intervals) {
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
        } else {
            $intervals->[0] = [@$oiv];
        }
    }
}

=head2 (void) $obj->register ( $recent_events_arrayref, $register_arrayref )

=head2 (void) $obj->register ( $recent_events_arrayref )

The first arrayref is a list of hashes that contain a key called
C<epoch> which is a string looking like a number. The second arrayref
is a list if integers which point to elements in the first arrayref to
be registered.

The second form registers all events in $recent_events_arrayref.

=cut

sub register {
    my($self, $re, $reg) = @_;
    my $intervals = $self->_intervals;
    unless ($reg) {
        $reg = [0..$#$re];
    }
  REGISTRANT: for my $i (@$reg) {
        my $logfile = $self->_logfile;
        if ($logfile) {
            require YAML::Syck;
            open my $fh, ">>", $logfile or die "Could not open '$logfile': $!";
            print $fh YAML::Syck::Dump({
                                        At => "before",
                                        Brfinterval => $self->_rfinterval,
                                        Ci => $i,
                                        ($i>0 ? ("Dre-1" => $re->[$i-1]) : ()),
                                        "Dre-0" => $re->[$i],
                                        ($i<$#$re ? ("Dre+1" => $re->[$i+1]) : ()),
                                        Eintervals => $intervals,
                                       });
        }
        $self->_register_one
            ({
              i => $i,
              re => $re,
              intervals => $intervals,
             });
        if ($logfile) {
            require YAML::Syck;
            open my $fh, ">>", $logfile or die "Could not open '$logfile': $!";
            print $fh YAML::Syck::Dump({
                                        At => "after",
                                        intervals => $intervals,
                                       });
        }
    }
}

sub _register_one {
    my($self, $one) = @_;
    my($i,$re,$intervals) = @{$one}{qw(i re intervals)};
    die sprintf "Panic: illegal i[%d] larger than number of events[%d]", $i, $#$re
        if $i > $#$re;
    my $epoch = $re->[$i]{epoch};
    $DB::single = "900644040" eq $epoch || "1233701831.34486" eq $epoch; # XXX
    return if $self->covered ( $epoch );
    if (@$intervals) {
        my $registered = 0;
        for my $iv (@$intervals) {
            my($ivupper,$ivlower) = @$iv; # may be the same
            if ($i > 0
                && _bigfloatge($re->[$i-1]{epoch}, $ivlower)
                && _bigfloatle($re->[$i-1]{epoch}, $ivupper)
               ) {
                $iv->[1] = $epoch;
                $registered++;
            }
            if ($i < $#$re
                && _bigfloatle($re->[$i+1]{epoch}, $ivupper)
                && _bigfloatge($re->[$i+1]{epoch}, $ivlower)
               ) {
                $iv->[0] = $epoch;
                $registered++;
            }
        }
        if ($registered == 2) {
            $self->_register_one_fold2
                (
                 $intervals,
                 $epoch,
                );
        } elsif ($registered == 1) {
        } else {
            $self->_register_one_fold0
                (
                 $intervals,
                 $epoch,
                );
        }
    } else {
        $intervals->[0] = [($epoch)x2];
    }
}

sub _register_one_fold0 {
    my($self,
       $intervals,
       $epoch,
      ) = @_;
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

sub _register_one_fold2 {
    my($self,
       $intervals,
       $epoch,
      ) = @_;
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
        $DB::single++;
        die "Panic: Could not find an interval position to insert '$epoch'";
    }
}

=head2 reset

Forgets everything ever done and gives way for a new round of
mirroring. Usually called when the dirtymark on upstream has changed.

=cut

sub reset {
    my($self) = @_;
    $self->_intervals(undef);
}

=head1 PRIVATE METHODS

=head2 _intervals

=cut
sub _intervals {
    my($self,$set) = @_;
    if (@_ >= 2) {
        $self->__intervals($set);
    }
    my $x = $self->__intervals;
    unless (defined $x) {
        $x = [];
        $self->__intervals ($x);
    }
    return $x;
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
