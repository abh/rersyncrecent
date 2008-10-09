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

use File::Basename qw(basename dirname fileparse);
use File::Copy qw(cp);
use File::Path qw(mkpath);
use File::Rsync;
use File::Temp;
use List::Util qw(first max);
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

B<Mostly unimplemented as of yet>. Will need to shift some accessors
from recentfile to recent.

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
                  "__pathdb",
                  "_max_one_state",
                  "_principal_recentfile",
                  "_recentfiles",
                  "_rsync",
                  "_runstatusfile",  # frequenty dumps all rfs
                  "_logfilefordone", # turns on _logfile on all DONE systems (disk intensive)
                 );

    my @pod_lines =
        split /\n/, <<'=cut'; push @accessors, grep {s/^=item\s+//} @pod_lines; }

=over 4

=item ignore_link_stat_errors

as in F:R:M:Recentfile

=item local

Option to specify the local principal file for operations with a local
collection of recentfiles.

=item localroot

as in F:R:M:Recentfile

=item max_files_per_connection

as in F:R:M:Recentfile

=item remote

TBD

=item remoteroot

XXX: this is (ATM) different from Recentfile!!!

=item remote_recentfile

Rsync address of the remote C<RECENT.recent> symlink or whichever name
the principal remote recentfile has.

=item rsync_options

Things like compress, links, times or checksums. Passed in to the
File::Rsync object used to run the mirror.

=item ttl

Minimum time before fetching the principal recentfile again.

=item secondaryttl

During normal, healthy operation, only the principal recentfile is
needed on slaves. But sometimes it is desireable to fetch them all,
even if we have downloaded the whole lot of ordinary files. If
secondaryttl is set, we will update the unneeded recentfiles after
this interval.

=item verbose

Boolean to turn on a bit verbosity.

=back

=cut

use accessors @accessors;

=head1 METHODS

=head2 $arrayref = $obj->news ( %options )

XXX WORK IN PROGRESS XXX

Testing this ATM with:

  perl -Ilib bin/rrr-news \
       -after 1217200539 \
       -max 12 \
       -local /home/ftp/pub/PAUSE/authors/RECENT.recent

  perl -Ilib bin/rrr-news \
       -after 1217200539 \
       -rsync=compress=1 \
       -rsync=links=1 \
       -localroot /home/ftp/pub/PAUSE/authors/ \
       -remote pause.perl.org::authors/RECENT.recent
       -verbose

Note: all parameters that can be passed to recent_events can also be specified here.

Note: all data are kept in memory

=cut

sub news {
    my($self, %opt) = @_;
    my $local = $self->local;
    unless ($local) {
        if (my $remote = $self->remote) {
            my $localroot;
            if ($localroot = $self->localroot) {
                # nice, they know what they are doing
            } else {
                die "FIXME: remote called without localroot should trigger File::Temp.... TBD, sorry";
            }
        } else {
            die "Alert: neither local nor remote specified, cannot continue";
        }
    }
    my $rfs = $self->recentfiles;
    my $ret = [];
    my $before;
    for my $rf (@$rfs) {
        my %locopt = %opt;
        $locopt{before} = $before;
        if ($opt{max}) {
            $locopt{max} -= scalar @$ret;
            last if $locopt{max} <= 0;
        }
        $locopt{info} = {};
        my $res = $rf->recent_events(%locopt);
        if (@$res){
            push @$ret, @$res;
        }
        if ($opt{max} && scalar @$ret > $opt{max}) {
            last;
        }
        if ($opt{after}){
            if ( $locopt{info}{last} && $locopt{info}{last}{epoch} < $opt{after} ) {
                last;
            }
            if ( $opt{after} > $locopt{info}{first}{epoch} ) {
                last;
            }
        }
        if (!@$res){
            next;
        }
        $before = $res->[-1]{epoch};
        $before = $opt{before} if $opt{before} && $opt{before} < $before;
    }
    $ret;
}

=head2 overview

returns a string that summarizes the state of all recentfiles
collected in this Recent object.

=cut
sub overview {
    my($self) = @_;
    my $rfs = $self->recentfiles;
    my(@s,%rank);
  RECENTFILE: for my $rf (@$rfs) {
        my $re=$rf->recent_events;
        my $rfsummary;
        if (@$re) {
            my $span = $re->[0]{epoch}-$re->[-1]{epoch};
            $rfsummary =
                [
                 $rf->interval,
                 scalar @$re,
                 sprintf ("%.3f", $re->[0]{epoch}),
                 sprintf ("%.3f", $re->[-1]{epoch}),
                 sprintf ("%.3f", $span),
                 ($rf->interval eq "Z"
                  ?
                  "-"
                  :
                  sprintf ("%5.1f%%", 100 * $span / $rf->interval_secs)
                 ),
                ];
            @rank{@{$rfsummary}[2,3]} = ();
        } else {
            next RECENTFILE;
        }
        push @s, $rfsummary;
    }
    @rank{sort {$b <=> $a} keys %rank} = 1..keys %rank;
    my $maxrank = max values %rank;
    for my $s (@s) {
        my $string = " " x $maxrank;
        my @borders;
        for (2,3) {
            push @borders, $rank{$s->[$_]}-1;
        }
        for ($borders[0],$borders[1]) {
            substr($string,$_,1) = "^";
        }
        push @$s, $string;
    }
    my @sprintf;
    for my $i (0..$#{$s[0]}) {
        my $maxlength = max map { length $_->[$i] } @s;
        push @sprintf, "%" . $maxlength . "s";
    }
    my $sprintf = join " ", @sprintf;
    $sprintf .= "\n";
    my $headline = sprintf $sprintf,
        (
         "",
         "Cnt",
         "Max",
         "Min",
         "Span",
         "Util", # u9n:)
         "Cloud",
        );
    join "", $headline, map { sprintf $sprintf, @$_ } @s;
}

=head2 _pathdb

(Private method, not for public use) Keeping track of already handled
files. Currently it is a hash, will probably become a database with
its own accessors.

=cut

sub _pathdb {
    my($self, $set) = @_;
    if ($set) {
        $self->__pathdb ($set);
    }
    my $pathdb = $self->__pathdb;
    unless (defined $pathdb) {
        $self->__pathdb(+{});
    }
    return $self->__pathdb;
}

=head2 $recentfile = $obj->principal_recentfile ()

returns the principal recentfile of this tree.

=cut

sub principal_recentfile {
    my($self) = @_;
    my $prince = $self->_principal_recentfile;
    return $prince if defined $prince;
    my $local = $self->local;
    if ($local) {
        $prince = File::Rsync::Mirror::Recentfile->new_from_file ($local);
    } else {
        if (my $remote = $self->remote) {
            my $localroot;
            if ($localroot = $self->localroot) {
                # nice, they know what they are doing
            } else {
                die "FIXME: remote called without localroot should trigger File::Temp.... TBD, sorry";
            }
            my $rf0 = $self->_recentfile_object_for_remote;
            $prince = $rf0;
        } else {
            die "Alert: neither local nor remote specified, cannot continue";
        }
    }
    $self->_principal_recentfile($prince);
    return $prince;
}

=head2 $recentfiles_arrayref = $obj->recentfiles ()

returns a reference to the complete list of recentfile objects that
describe this tree. No guarantee is given that the represented
recentfiles exist or have been read. They are just bare objects.

=cut

sub recentfiles {
    my($self) = @_;
    my $rfs        = $self->_recentfiles;
    return $rfs if defined $rfs;
    my $rf0        = $self->principal_recentfile;
    my $pathdb     = $self->_pathdb;
    $rf0->_pathdb ($pathdb);
    my $aggregator = $rf0->aggregator;
    my @rf         = $rf0;
    for my $agg (@$aggregator) {
        my $nrf = $rf0->_sparse_clone;
        $nrf->interval      ( $agg );
        $nrf->have_mirrored ( 0    );
        $nrf->_pathdb       ( $pathdb  );
        push @rf, $nrf;
    }
    $self->_recentfiles(\@rf);
    return \@rf;
}

=head2 $success = $obj->rmirror ( %options )

XXX WORK IN PROGRESS XXX

Mirrors all recentfiles of the I<remote> address working through all
of them, mirroring their contents.

Testing this ATM with:

  use File::Rsync::Mirror::Recent;
  my $rrr = File::Rsync::Mirror::Recent->new(
         ignore_link_stat_errors => 1,
         localroot => "/home/ftp/pub/PAUSE/authors",
         remote => "pause.perl.org::authors/RECENT.recent",
         max_files_per_connection => 5000,
         rsync_options => {
                           compress => 1,
                           links => 1,
                           times => 1,
                           checksum => 0,
                          },
         verbose => 1,
         _runstatusfile => "recent-rmirror-state.yml",
         _logfilefordone => "recent-rmirror-donelog.log",
  );
  $rrr->rmirror ( "skip-deletes" => 1, loop => 1 );

And since the above seems to work, I try now without the llop
parameter:

  use File::Rsync::Mirror::Recent;
  my @rrr;
  for my $t ("authors","modules"){
      my $rrr = File::Rsync::Mirror::Recent->new(
         ignore_link_stat_errors => 1,
         localroot => "/home/ftp/pub/PAUSE/$t",
         remote => "pause.perl.org::$t/RECENT.recent",
         max_files_per_connection => 512,
         rsync_options => {
                           compress => 1,
                           links => 1,
                           times => 1,
                           checksum => 0,
                          },
         verbose => 1,
         _runstatusfile => "recent-rmirror-state-$t.yml",
         _logfilefordone => "recent-rmirror-donelog-$t.log",
      );
      push @rrr, $rrr;
  }
  while (){
    for my $rrr (@rrr){
      $rrr->rmirror ( "skip-deletes" => 1 );
    }
    warn "sleeping 23\n"; sleep 23;
  }


=cut

sub rmirror {
    my($self, %options) = @_;

    # my $rf0 = $self->_recentfile_object_for_remote;
    my $rfs = $self->recentfiles;

    my $_once_per_20s = sub {
        $self->principal_recentfile->seed;
    };
    my $_sigint = sub {
        # XXX exit gracefully (reminder)
    };
    my $minimum_time_per_loop = 20; # XXX needs accessor: warning, if
                                    # set too low, we do nothing but
                                    # mirror the principal!
    if (my $logfile = $self->_logfilefordone) {
        for my $i (0..$#$rfs) {
            $rfs->[$i]->done->_logfile($logfile);
        }
    }
    my $secondary_timestamp = time;
  LOOP: while () {
        my $ttleave = time + $minimum_time_per_loop;
      RECENTFILE: for my $i (0..$#$rfs) {
            my $rf = $rfs->[$i];
            if (my $file = $self->_runstatusfile) {
                require YAML::Syck;
                YAML::Syck::DumpFile
                      (
                       $file,
                       {i => $i,
                        options => \%options,
                        self => $self,
                        time => time,
                        uptodate => {map {($_=>$rfs->[$_]->uptodate)} 0..$#$rfs},
                       });
            }
            if (time > $ttleave){
                # Must make sure that one file can get fetched in any case
                $self->_max_one_state(1);
            }
            if ($rf->uptodate){
                $rfs->[$i+1]->done->merge($rf->done) if $i < $#$rfs;
                next RECENTFILE;
            } else {
              WORKUNIT: while (time < $ttleave) {
                    if ($rf->uptodate) {
                        my $sleep = $rf->sleep_per_connection;
                        $sleep = 0.42 unless defined $sleep; # XXX accessor!
                        Time::HiRes::sleep $sleep;
                        $rfs->[$i+1]->done->merge($rf->done) if $i < $#$rfs;
                        next RECENTFILE;
                    } else {
                        my %locopt = %options;
                        if ($self->_max_one_state) {
                            $locopt{max} = 1;
                        }
                        $locopt{piecemeal} = 1;
                        $rf->mirror (%locopt);
                        if ($rf->_seeded) {
                            $rfs->[$i+1]->seed if $i < $#$rfs;
                        }
                    }
                }
            }
        }
        $self->_max_one_state(0);
        if ($rfs->[-1]->uptodate) {
            if ($options{loop}) {
                my $pathdb = $self->_pathdb();
                for my $k (keys %$pathdb) {
                    delete $pathdb->{$k};
                }
                if (my $ttl = $self->secondaryttl) {
                    if (time > $secondary_timestamp+$ttl) {
                        my @names;
                        for my $xrf (@{$self->recentfiles}) {
                            $xrf->seed;
                            push @names, $xrf->interval;
                        }
                        warn "DEBUG: seeded @names\n";
                        $secondary_timestamp = time;
                    }
                }
            } else {
                last LOOP;
            }
        }
        my $sleep = $ttleave - time;
        if ($sleep > 0.01) {
            printf STDERR
                (
                 "Dorm %d (%s secs)\n",
                 time,
                 $sleep,
                );
            sleep $sleep;
        } else {
            # negative time not invented yet:)
        }
        $_once_per_20s->();
    }
}

# mirrors the recentfile and instantiates the recentfile object
sub _recentfile_object_for_remote {
    my($self) = @_;
    # get the remote recentfile
    my $rrfile = $self->remote or die "Alert: cannot construct a recentfile object without the 'remote' attribute";
    my $splitter = qr{(.+)/([^/]*)};
    my($remoteroot,$rfilename) = $rrfile =~ $splitter;
    $self->remoteroot($remoteroot);
    my $abslfile;
    if (!defined $rfilename) {
        die "Alert: Cannot resolve '$rrfile', does not match $splitter";
    } elsif (not length $rfilename or $rfilename eq "RECENT.recent") {
        ($abslfile,$rfilename) = $self->_resolve_rfilename($rfilename);
    }
    my @need_args =
        (
         "ignore_link_stat_errors",
         "localroot",
         "max_files_per_connection",
         "remoteroot",
         "rsync_options",
         "verbose",
         "ttl",
        );
    my $rf0;
    unless ($abslfile) {
        $rf0 = File::Rsync::Mirror::Recentfile->new (map {($_ => $self->$_)} @need_args);
        $rf0->resolve_recentfilename($rfilename);
        $abslfile = $rf0->get_remote_recentfile_as_tempfile ();
    }
    $rf0 = File::Rsync::Mirror::Recentfile->new_from_file ( $abslfile );
    for my $override (@need_args) {
        $rf0->$override ( $self->$override );
    }
    $rf0->is_slave (1);
    return $rf0;
}

sub _resolve_rfilename {
    my($self, $rfilename) = @_;
    $rfilename = "RECENT.recent" unless length $rfilename;
    my $abslfile = undef;
    if ($rfilename =~ /\.recent$/) {
        # may be a file *or* a symlink, 
        $abslfile = $self->_fetch_as_tempfile ($rfilename);
        while (-l $abslfile) {
            my $symlink = readlink $abslfile;
            if ($symlink =~ m|/|) {
                die "FIXME: filenames containing '/' not supported, got '$symlink'";
            }
            $abslfile = $self->_fetch_as_tempfile ($symlink);
        }
    }
    return ($abslfile, $rfilename);
}

# takes a basename, returns an absolute name, does not delete the
# file, throws the $fh away. Caller must rename or unlink
sub _fetch_as_tempfile {
    my($self, $rfile) = @_;
    my($suffix) = $rfile =~ /(\.[^\.]+)$/;
    $suffix = "" unless defined $suffix;
    my $fh = File::Temp->new
        (TEMPLATE => sprintf(".FRMRecent-%s-XXXX",
                             $rfile,
                            ),
         DIR => $self->localroot,
         SUFFIX => $suffix,
         UNLINK => 0,
        );
    my $rsync = File::Rsync->new($self->rsync_options);
    $rsync->exec
        (
         src => join("/",$self->remoteroot,$rfile),
         dst => $fh->filename,
        ) or die "Could not mirror '$rfile' to $fh\: ".$rsync->err;
    return $fh->filename;
}

=head2 (void) $obj->rmirror_loop

(TBD) Run rmirror in an endless loop.

=cut

sub rmirror_loop {
    my($self) = @_;
    die "FIXME";
}

=head2 $hash = $obj->verify

(TBD) Runs find on the local tree, collects all existing files from
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

Copyright 2008 Andreas König.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of File::Rsync::Mirror::Recent
