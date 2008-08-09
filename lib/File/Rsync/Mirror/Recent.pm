package File::Rsync::Mirror::Recent;

# use warnings;
use strict;
use File::Rsync::Mirror::Recentfile;

=encoding utf-8

=head1 NAME

File::Rsync::Mirror::Recent - mirroring via rsync made efficient

=head1 VERSION

Version 0.0.1

=cut

package File::Rsync::Mirror::Recent;

use Data::Serializer;
use File::Basename qw(dirname fileparse);
use File::Copy qw(cp);
use File::Path qw(mkpath);
use File::Rsync;
use File::Temp;
use List::Util qw(first);
use Scalar::Util qw(reftype);
use Storable;
use Time::HiRes qw();
use YAML::Syck;

use version; our $VERSION = qv('0.0.1');

=head1 SYNOPSIS

B<!!!! PRE-ALPHA ALERT !!!!>

Nothing in here is believed to be stable, nothing yet intended for
public consumption. The plan is to provide a script in one of the next
releases that acts as a frontend for all the backend functionality.
Option and method names will very likely change.

File::Rsync::Mirror::Recent is acting at a higher level than
File::Rsync::Mirror::Recentfile. File::Rsync::Mirror::Recent
establishes a view on a collection of recentfile objects and provides
abstractions spanning multiple intervals associated with those.

B<Unimplemented as of yet>. Will need to shift some accessors from
recentfile to recent.

Reader/mirrorer:

    my $rr = File::Rsync::Mirror::Recent->new
      (
       ignore_link_stat_errors => 1,
       localroot => "/home/ftp/pub/PAUSE/authors",
       remote => "pause.perl.org::authors/RECENT.recent",
       rsync_options => {
                         compress => 1,
                         links => 1,
                         times => 1,
                         checksum => 1,
                        },
       verbose => 1,
      );
    $rr->rmirror;

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
                  "_rsync",
                 );

    my @pod_lines =
        split /\n/, <<'=cut'; push @accessors, grep {s/^=item\s+//} @pod_lines; }

=over 4

=item ignore_link_stat_errors

as in F:R:M:Recentfile

=item localroot

XXX: this is (ATM) different from localroot in Recentfile!!!

=item loopinterval

When mirror_loop is called, this accessor can specify how much time
every loop shall at least take. If the work of a loop is done before
that time has gone, sleeps for the rest of the time. Defaults to
arbitrary 42 seconds.

=item max_files_per_connection

Maximum number of files that are transferred on a single rsync call.
Setting it higher means higher performance at the price of holding
connections longer and potentially disturbing other users in the pool.
Defaults to the arbitrary value 42.

=item remote

TBD

=item remotebase

XXX: this is (ATM) different from Recentfile!!!

=item remote_recentfile

Rsync address of the remote C<RECENT.recent> symlink or whichever name
the main driving remote recentfile has.

=item rsync_options

Things like compress, links, times or checksums. Passed in to the
File::Rsync object used to run the mirror.

=item verbose

Boolean to turn on a bit verbosity.

=back

=cut

use accessors @accessors;

=head1 METHODS

=head2 $success = $obj->rmirror ( %options )

Mirrors all recentfiles of the I<remote> address.

XXX WORK IN PROGRESS XXX

XXX Afterwards it should work through all of them, but not blindly,
rather by merging first and mirroring then XXX

Testing this ATM with:

  perl -Ilib -e '
  use File::Rsync::Mirror::Recent;
  my $rrr = File::Rsync::Mirror::Recent->new(
         ignore_link_stat_errors => 1,
         localroot => "/home/ftp/pub/PAUSE/authors",
         remote => "pause.perl.org::authors/RECENT.recent",
         rsync_options => {
                           compress => 1,
                           links => 1,
                           times => 1,
                           checksum => 1,
                          },
         verbose => 1,

  );
  $rrr->rmirror
  '


=cut

sub rmirror {
    my($self, %options) = @_;

    # get the remote remotefile
    my $rrfile = $self->remote;
    my($remotebase,$recentfile_basename) = $rrfile =~ m{(.+)/([^/]+)};
    $self->remotebase($remotebase);
    my @need_args =
        (
         "localroot",
         "remotebase",
         "rsync_options",
         "verbose",
        );
    my $rf0 = File::Rsync::Mirror::Recentfile->new (map {($_ => $self->$_)} @need_args);
    my $lfile = $rf0->get_remotefile ($recentfile_basename);
    # while it is a symlink, resolve it
    while (-l $lfile) {
        my $symlink = readlink $lfile;
        if ($symlink =~ m|/|) {
            die "FIXME: filenames containing '/' not supported, got $symlink";
        }
        $lfile = $rf0->get_remotefile ($symlink);
    }
    $rf0 = File::Rsync::Mirror::Recentfile->new_from_file ( $lfile );
    for my $need_arg (@need_args) {
        $rf0->$need_arg ( $self->$need_arg );
    }
    my $aggregator = $rf0->aggregator;
    my @rf = $rf0;
    for my $agg (@$aggregator) {
        my $nrf = Storable::dclone ( $rf0 );
        $nrf->interval ( $agg );
        my $nlfile = $rf0->get_remotefile ( $nrf->recentfile_basename );
        push @rf, $nrf;
    }
    warn sprintf "Got %d recentfiles\n", scalar @rf;
    for my $rf (@rf) {
        die "FIXME: merge and then mirror";
        $rf->mirror ( ); # XXX needs "before", not "after"
        my $re = $rf->recent_events;
        warn sprintf
            (
             "Mirrored from %s up to %s/%s\n",
             $rf->rfile,
             $re->[0]{path},
             $re->[0]{epoch},
            );
    }
}

=head2 (void) $obj->rmirror_loop

(TBD) Run rmirror in an endless loop.

=cut

sub rmirror_loop {
    my($self) = @_;
    die "FIXME";
}

=head2 $hash = $obj->verify

(TBD) Runs find on the tree, collects all existing files from
recentfiles, compares their names. The returned hash contains the keys
C<todelete> and C<toadd>.

=cut

sub verify {
    my($self) = @_;
    die "FIXME";
}

=head1 AUTHOR

Andreas König

=head1 BUGS

Please report any bugs or feature requests through the web interface
at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=File-Rsync-Mirror-Recent>.
I will be notified, and then you'll automatically be notified of
progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc File::Rsync::Mirror::Recent

You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=File-Rsync-Mirror-Recent>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/File-Rsync-Mirror-Recent>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/File-Rsync-Mirror-Recent>

=item * Search CPAN

L<http://search.cpan.org/dist/File-Rsync-Mirror-Recent>

=back


=head1 ACKNOWLEDGEMENTS

Thanks to RJBS for module-starter.

=head1 COPYRIGHT & LICENSE

Copyright 2008 Andreas König, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of File::Rsync::Mirror::Recent
