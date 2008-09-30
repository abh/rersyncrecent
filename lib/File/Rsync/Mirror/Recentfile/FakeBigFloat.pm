package File::Rsync::Mirror::Recentfile::FakeBigFloat;

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

File::Rsync::Mirror::Recentfile::FakeBigFloat - very limited bigfloat support

=head1 VERSION

Version 0.0.1

=cut

use version; our $VERSION = qv('0.0.1');

use Exporter;
use base qw(Exporter);
our %EXPORT_TAGS;
our @EXPORT_OK = qw( _bigfloatcmp _bigfloatmin _bigfloatmax _bigfloatlt _bigfloatgt );
$EXPORT_TAGS{all} = \@EXPORT_OK;

=head1 SYNOPSIS

  use File::Rsync::Mirror::Recentfile::FakeBigFloat qw(:all);

=head1 EXPORT

All functions are exported in the C<:all> tag.

=head1 (ONLY) INTERNAL FUNCTIONS

These functions are not part of a public interface and can be
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
