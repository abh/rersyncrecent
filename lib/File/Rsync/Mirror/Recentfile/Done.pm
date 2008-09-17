package File::Rsync::Mirror::Recentfile::Done;

# use warnings;
use strict;

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

=head2 (void) $obj->register ( $recent_events_arrayref, $register_arrayref )

The first arrayref is a list fo hashes that contain a key called
C<epoch> which is a string looking like a number. The second arrayref
is a list if integers which point to elements in the first arrayref to
be registered.

=cut

sub register {
    my($self, $re, $reg) = @_;
    my $intervals = $self->_intervals || [];
    $self->_intervals ($intervals);
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
                    die "Panic";
                }
            } elsif ($registered == 1) {
            } else {
                my $splicepos = @$intervals;
                for my $i (0..$#$intervals) {
                    if ($epoch > $intervals->[$i][0]) {
                        $splicepos = $i;
                        last;
                    }
                }
                splice @$intervals, $splicepos, 0, [($epoch)x2];
            }
        } else {
            $intervals->[0] = [($epoch)x2];
        }
    }
}

=head1 COPYRIGHT & LICENSE

Copyright 2008 Andreas König, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut

1; # End of File::Rsync::Mirror::Recentfile

# Local Variables:
# mode: cperl
# cperl-indent-level: 4
# End:
