package File::Rsync::Mirror::Recentfile;

# use warnings;
use strict;

=encoding utf-8

=head1 NAME

File::Rsync::Mirror::Recentfile - mirroring via rsync made efficient

=head1 VERSION

Version 0.0.1

=cut

package File::Rsync::Mirror::Recentfile;

use File::Basename qw(dirname);
use File::Path qw(mkpath);
use File::Rsync;
use File::Temp;
use Scalar::Util qw(reftype);
use Time::HiRes qw();
use YAML::Syck;

use version; our $VERSION = qv('0.0.1');


use constant MAX_INT => ~0>>1; # anything better?

# cf. interval_secs
my %seconds = (
               s => 1,
               m => 60,
               h => 60*60,
               d => 60*60*24,
               W => 60*60*24*7,
               M => 60*60*30,
               Q => 60*60*90,
               Y => 60*60*365.25,
              );

use accessors (
               "_current_tempfile",
               "_is_locked",
               "_remotebase",
               "_rfile",
               "_rsync",
               "_use_tempfile",
               "aggregator",
               "canonize",
               "comment",
               "filenameroot",
               "ignore_link_stat_errors",
               "interval",
               "localroot",
               "protocol",            # reader/writer modifier
               "remote_dir",
               "remote_host",
               "remote_module",
               "rsync_options",
               "verbose",
              );

=head1 SYNOPSIS

B<!!!! PRE-ALPHA ALERT !!!!>

Nothing in here is meant for public consumption. The plan is to
provide a script in one of the next releases that acts as a frontend
for all the backend functionality. Option and method names will very
likely change.

Just for developers of the (yet to be named) script(s) we document
here the pre-alpha quality interface.

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
    unless (defined $self->protocol) {
        $self->protocol(0);     # default protocol will soon be 1
    }
    unless (defined $self->filenameroot) {
        $self->filenameroot("RECENT");
    }
    return $self;
}

=head2 my $rfile = $obj->rfile

Returns the full path of the recentfile

=cut

sub rfile {
    my($self) = @_;
    if ($self->_use_tempfile) {
        return $self->_current_tempfile;
    } else {
        my $rfile = $self->_rfile;
        return $rfile if defined $rfile;
        $rfile = File::Spec->catfile
            ($self->localroot,
             sprintf ("%s-%s.yaml",
                      $self->filenameroot,
                      $self->interval,
                     )
            );
        $self->_rfile ($rfile);
        return $rfile;
    }
}

=head2 $ret = $obj->update ($path,$type)

Get a file from upstream.

=cut

sub update {
    my($self,$path,$type) = @_;
    if (my $meth = $self->canonize) {
        if (ref $meth && ref $meth eq "CODE") {
            die "FIXME";
        } else {
            $path = $self->$meth($path);
        }
    }
    my $lrd = $self->localroot;
    if ($path =~ s|^\Q$lrd\E||) {
        my $interval = $self->interval;
        my $secs = $self->interval_secs();
        my $epoch = Time::HiRes::time;
        my $oldest_allowed = $epoch-$secs;

        $self->lock;
        my $recent = $self->recent_events;
        $recent ||= [];
      TRUNCATE: while (@$recent) {
            if ($recent->[-1]{epoch} < $oldest_allowed) {
                pop @$recent;
            } else {
                last TRUNCATE;
            }
        }
        # remove older duplicates of this $path, irrespective of $type:
        $recent = [ grep { $_->{path} ne $path } @$recent ];

        unshift @$recent, { epoch => $epoch, path => $path, type => $type };
        $self->write_recent($recent);
        $self->unlock;
    }
}

=head2 (void) $obj->lock

Locking is implemented with an C<mkdir> on a locking directory
(C<.lock> appended to $rfile).

=cut

sub lock {
    my ($self) = @_;
    # not using flock because it locks on filehandles instead of
    # old school ressources.
    my $locked = $self->_is_locked and return;
    my $rfile = $self->rfile;
    # XXX need a way to allow breaking the lock
    while (not mkdir "$rfile.lock") {
        Time::HiRes::sleep 0.01;
    }
    $self->_is_locked (1);
}

=head2 (void) $obj->unlock

Unlocking is implemented with an C<rmdir> on a locking directory
(C<.lock> appended to $rfile).

=cut

sub unlock {
    my($self) = @_;
    return unless $self->_is_locked;
    my $rfile = $self->rfile;
    rmdir "$rfile.lock";
    $self->_is_locked (0);
}

=head2 $obj->write_recent ($recent_files_arrayref)

Writes a recentfile based on the current reflection of the current
state of the tree limited by the current interval.

=cut

sub write_recent {
    my ($self,$recent) = @_;
    my $meth = sprintf "write_%d", $self->protocol;
    $self->$meth($recent);
}

=head2 $obj->write_0 ($recent_files_arrayref)

Delegate of write_recent() on protocol 0

=cut

sub write_0 {
    my ($self,$recent) = @_;
    my $rfile = $self->rfile;
    YAML::Syck::DumpFile("$rfile.new",$recent);
    rename "$rfile.new", $rfile or die "Could not rename to '$rfile': $!";
}

=head2 $obj->write_1 ($recent_files_arrayref)

Delegate of write_recent() on protocol 1

=cut

sub write_1 {
    my ($self,$recent) = @_;
    my $rfile = $self->rfile;
    YAML::Syck::DumpFile("$rfile.new",{
                                       meta => $self->meta_data,
                                       recent => $recent,
                                      });
    rename "$rfile.new", $rfile or die "Could not rename to '$rfile': $!";
}

=head2 $hashref = $obj->meta_data

returns the hashref of metadata that the server wants to add to the
recentfile.

=cut

sub meta_data {
    my($self) = @_;
    my $ret = {};
    for my $m (
               "aggregator",
               "canonize",
               "comment",
               "filenameroot",
               "interval_secs",
               "protocol",
              ) {
        $ret->{$m} = $self->$m;
    }
    return $ret;
}

=head2 $path = $obj->naive_path_normalize ($path)

Takes a local absolute path as argument and canonicalizes frequent
path mistakes like double slashes or C</./>. And removes references to
C<../> directories to get a shorter unambiguos path.

=cut

sub naive_path_normalize {
    my($self,$path) = @_;
    $path =~ s|/+|/|g;
    1 while $path =~ s|/[^/]+/\.\./|/|;
    $path =~ s|/$||;
    $path;
}

=head2 $array_ref = $obj->recent_events_from_tempfile

Reads the file-events in the temporary local mirror of the remote file.

=cut

sub recent_events_from_tempfile {
    my ($self) = @_;
    $self->_use_tempfile(1);
    my $ret = $self->recent_events;
    $self->_use_tempfile(0);
    return $ret;
}

=head2 $array_ref = $obj->recent_events

Note: the code relies on the resource being written atomically. We
cannot lock because we may have no write access.

=cut

sub recent_events {
    my ($self) = @_;
    my $rfile = $self->rfile;
    my ($data) = eval {YAML::Syck::LoadFile($rfile);};
    my $err = $@;
    if ($err or !$data) {
        return [];
    }
    if (reftype $data eq 'ARRAY') { # protocol 0
        return $data;
    } else {
        my $meth = sprintf "read_recent_%d", $data->{meta}{protocol};
        return $self->$meth($data);
    }
}

=head2 $ret = $obj->read_recent_1 ( $recent_data )

Delegate of recent_events() on protocol 1

=cut

sub read_recent_1 {
    my($self,$data) = @_;
    return $data->{recent};
}

=head2 $ret = $obj->local_event_path

Misnomer

=cut

sub local_event_path {
    my($self,$path) = @_;
    my @p = split m|/|, $path; # rsync paths are always slash-separated
    File::Spec->catfile($self->localroot,@p);
}

=head2 $success = $obj->mirror_path ( $path )

Fetches a remote path into the local copy

=cut

sub mirror_path {
    my($self,$path) = @_;
    my $dst = $self->local_event_path($path);
    mkpath dirname $dst;
    unless ($self->rsync->exec
            (
             src => join("/",
                         $self->remotebase,
                         $path
                        ),
             dst => $dst,
            )) {
        my($err) = $self->rsync->err;
        if ($self->ignore_link_stat_errors && $err =~ m{^ rsync: \s link_stat }x ) {
            if ($self->verbose) {
                warn "Info: ignoring link_stat error '$err'";
            }
            return 1;
        }
        die sprintf "Error: %s", $err;
    }
    return 1;
}

=head2 $tempfilename = $obj->get_remote_recentfile_as_tempfile

Stores the remote recentfile locally as a tempfile

=cut

sub get_remote_recentfile_as_tempfile {
    my($self) = @_;
    mkpath dirname $self->localroot;
    my($fh) = File::Temp->new(TEMPLATE => sprintf(".%s-XXXX",
                                                  $self->filenameroot,
                                                 ),
                              DIR => $self->localroot,
                              SUFFIX => ".yaml",
                              UNLINK => 0,
                             );
    my($trecentfile) = $fh->filename;
    unless ($self->rsync->exec(
                               src => join("/",
                                           $self->remotebase,
                                           $self->recentfile_basename),
                               dst => $trecentfile,
                              )) {
        unlink $trecentfile or die "Couldn't unlink '$trecentfile': $!";
        die sprintf "Error while rsyncing: %s", $self->rsync->err;
    }
    my $mode = 0644;
    chmod $mode, $trecentfile or die "Could not chmod $mode '$trecentfile': $!";
    $self->_current_tempfile ($trecentfile);
    return $trecentfile;
}

=head2 $ret = $obj->recentfile

deprecated, use rfile instead

=cut

sub recentfile {
    my($self) = @_;
    my $recent = File::Spec->catfile(
                                     $self->localroot,
                                     $self->recentfile_basename(),
                                    );
    return $recent;
}

=head2 $ret = $obj->recentfile_basename

Status unknown. It seem this needs to be used in rfile too.

=cut

sub recentfile_basename {
    my($self) = @_;
    my $interval = $self->interval;
    my $file = sprintf("%s-%s.yaml",
                       $self->filenameroot,
                       $interval
                      );
    return $file;
}

=head2 $secs = $obj->interval_secs ( $interval_spec )

$interval_spec is a string that either consists of the single letter
C<Z> or is composed from an integer and a letter.

=cut

sub interval_secs {
    my ($self) = @_;
    my $interval = $self->interval;
    my ($n,$t) = $interval =~ /^(\d*)([smhdWMYZ]$)/ or
        die "Could not determine seconds from interval[$interval]";
    if ($interval eq "Z") {
        return MAX_INT;
    } elsif (exists $seconds{$t} and $n =~ /^\d+$/) {
        return $seconds{$t}*$n;
    } else {
        die "Invalid interval specification: n[$n]t[$t]";
    }
}

=head2 $str = $obj->remotebase

Returns the composed prefix needed when rsyncing from a remote module.

=cut

sub remotebase {
    my($self) = @_;
    my $remotebase = $self->_remotebase;
    unless (defined $remotebase) {
        $remotebase = sprintf(
                              "%s::%s%s",
                              $self->remote_host,
                              $self->remote_module,
                              ($self->remote_dir ? ("/".$self->remote_dir) : ""),
                             );
        $self->_remotebase($remotebase);
    }
    return $remotebase;
}

=head2 $rsync_obj = $obj->rsync

The File::Rsync object that this object uses for communicating with an
upstream server.

=cut

sub rsync {
    my($self) = @_;
    my $rsync = $self->_rsync;
    unless (defined $rsync) {
        my $rsync_options = $self->rsync_options || {};
        $rsync = File::Rsync->new($rsync_options);
        $self->_rsync($rsync);
    }
    return $rsync;
}

=head1 BACKGROUND

=head2 NON-COMPETITORS

 File::Mirror        JWU/File-Mirror/File-Mirror-0.10.tar.gz only local trees
 Mirror::YAML        ADAMK/Mirror-YAML-0.03.tar.gz           some sort of inner circle
 Net::DownloadMirror KNORR/Net-DownloadMirror-0.04.tar.gz    FTP sites and stuff
 Net::MirrorDir      KNORR/Net-MirrorDir-0.05.tar.gz         "
 Net::UploadMirror   KNORR/Net-UploadMirror-0.06.tar.gz      "
 Pushmi::Mirror      CLKAO/Pushmi-v1.0.0.tar.gz              something SVK

=head2 COMPETITORS

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
forward to the first FUSE based CPAN filesystem. Anyone?

=head1 AUTHOR

Andreas König

=head1 BUGS

Please report any bugs or feature requests through the web interface
at
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
