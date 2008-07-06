package File::Rsync::Mirror::Recentfile;

use warnings;
use strict;

=encoding utf-8

=head1 NAME

File::Rsync::Mirror::Recentfile - mirroring via rsync made efficient

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';


=head1 SYNOPSIS

B<!!!! ALPHA ALERT !!!!>

Nothing in here is meant for public consumption. The plan is to
provide a script in one of the next releases that acts as a frontend
for all the backend functionality. Option and method names will very
likely change.

Just for developers of the (yet to be named) script(s) we document
here the alpha quality interface.

Writer:

    use File::Rsync::Mirror::Recentfile;
    my $fr = File::Rsync::Mirror::Recentfile->new
      (
       interval => q(6h),
       filenameroot => "RECENT",
       comment => "These 'RECENT' files are part of a test of a new CPAN mirroring concept. Please ignore them for now.",
       localroot => "/home/ftp/pub/PAUSE/authors/",
       aggregator => [qw(1d 1W 1M 1Q 1Y Z)],
      );
    $rf->update("/home/ftp/pub/PAUSE/authors/id/A/AN/ANDK/CPAN-1.92_63.tar.gz","new");

Reader:

    my $rf = File::Rsync::Mirror::Recentfile->new
      (
       filenameroot => "RECENT",
       ignore_link_stat_errors => 1,
       interval => q(6h),
       localroot => "/home/ftp/pub/PAUSE/authors",
       remote_dir => "",
       remote_host => "pause.perl.org",
       remote_module => "authors",
       rsync_options => {
                         compress => 1,
                         'rsync-path' => '/usr/bin/rsync',
                         links => 1,
                         times => 1,
                         'omit-dir-times' => 1,
                         checksum => 1,
                        },
       verbose => 1,
      );
    my $trecentfile = eval {$rf->get_remote_recentfile_as_tempfile();};
    die $@ if $@;
    my ($recent_data) = $rf->recent_events_from_tempfile();

=head1 EXPORT

No exports.

=head1 METHODS

=head2 

=cut

=head1 BACKGROUND

When the rsync people announced 3.0 they promised a batch mode for
large rsynced clusters. The batch mode should solve a common problem
of clusters and ftp mirrors and otherwise replicated datasets like
CPAN: to transfer only the diff between two hosts it usually takes
along time to determine the diff itself.

For many years the best solution for this was csync2 which solves the
problem by maintining a sqlite database on both ends and talking a
highly sophisticated protocol to quickly determine which files to send
at any given point in time. Csync2 is often inconvenient because the
act of syncing demands quite an intimate relationship between the
sender and the receiver and suffers when the number of syncing sites
is large or connections are unreliable.

Rsync 3.0 batch mode works around these problems by providing
rsync-able batch files which allow receiving nodes to replay the
history of the other nodes. This reduces the need to have an
incestuous relation but it has the disadvantage that these batch files
replicate the contents of the involved files. This seems inappropriate
when the nodes already have a means of communicating over rsync.

rersyncrecent solves this problem with a couple of (usually 2-5) index
files which cover different overlapping time intervals. The master
writes these files and the clients can construct the full tree from
the information contained in them. The most recent index file usually
covers the last seconds or minutes or even hours of the tree and
depending on the needs, slaves can rsync every few seconds and then
bring their trees in full sync.

The rersyncrecent mode was developed for CPAN but it seems the most
convenient and economic solution in many other areas too. I'm looking
forwar to the first FUSE based CPAN filesystem. Anyone?

=head1 AUTHOR

Andreas König, C<< <andreas.koenig.7os6VVqR at franz.ak.mind.de> >>

=head1 BUGS

Please report any bugs or feature requests to
C<bug-file-rsync-mirror-recentfile at rt.cpan.org>, or through the web
interface at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=File-Rsync-Mirror-Recentfile>.
I will be notified, and then you'll automatically be notified of
progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc File::Rsync::Mirror::Recentfile

You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=File-Rsync-Mirror-Recentfile>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/File-Rsync-Mirror-Recentfile>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/File-Rsync-Mirror-Recentfile>

=item * Search CPAN

L<http://search.cpan.org/dist/File-Rsync-Mirror-Recentfile>

=back


=head1 ACKNOWLEDGEMENTS

Thanks to RJBS for module-starter.

=head1 COPYRIGHT & LICENSE

Copyright 2008 Andreas König, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of File::Rsync::Mirror::Recentfile
