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


use constant MAX_INT => ~0>>1; # anything better?

# cf. interval_secs
my %seconds;

# maybe subclass if this mapping is bad?
my %serializers;

=head1 SYNOPSIS

B<!!!! PRE-ALPHA ALERT !!!!>

Nothing in here is believed to be stable, nothing yet intended for
public consumption. The plan is to provide a script in one of the next
releases that acts as a frontend for all the backend functionality.
Option and method names will very likely change.

For the rationale see the section BACKGROUND.

This is published only for developers of the (yet to be named)
script(s).

Writer (of a single file):

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

Reader/mirrorer:

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
    $rf->mirror;

Aggregator (usually the writer):

    my $rf = File::Rsync::Mirror::Recentfile->new_from_file ( $file );
    $rf->aggregate;

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
    unless (defined $self->protocol) {
        $self->protocol(1);
    }
    unless (defined $self->filenameroot) {
        $self->filenameroot("RECENT");
    }
    unless (defined $self->serializer_suffix) {
        $self->serializer_suffix(".yaml");
    }
    return $self;
}

=head2 my $obj = CLASS->new_from_file($file)

Constructor. $file is a I<recentfile>.

=cut

sub new_from_file {
    my($class, $file) = @_;
    my $self = bless {}, $class;
    $self->_rfile($file);
    #?# $self->lock;
    my $serialized = do { open my $fh, $file or die "Could not open '$file': $!";
                           local $/;
                           <$fh>;
                       };
    my($name,$path,$suffix) = fileparse $file, keys %serializers;
    $self->serializer_suffix($suffix);
    $self->localroot($path);
    die "Could not determine file format from suffix" unless $suffix;
    my $serializer = Data::Serializer->new
        (
         serializer => $serializers{$suffix},
         secret     => undef,
         compress   => 0,
         digest     => 0,
         portable   => 0,
         encoding   => "raw",
        );
    my $deserialized = $serializer->deserialize($serialized);
    while (my($k,$v) = each %{$deserialized->{meta}}) {
        next if $k ne lc $k; # "Producers"
        $self->$k($v);
    }
    unless (defined $self->protocol) {
        $self->protocol(1);
    }
    return $self;
}

=head1 ACCESSORS

=cut

my @accessors;

BEGIN {
    @accessors = (
                  "_current_tempfile",
                  "_interval",
                  "_is_locked",
                  "_localroot",
                  "_remotebase",
                  "_rfile",
                  "_rsync",
                  "_use_tempfile",
                 );

    my @pod_lines =
        split /\n/, <<'=cut'; push @accessors, grep {s/^=item\s+//} @pod_lines; }

=over 4

=item aggregator

A list of interval specs that tell the aggregator which I<recentfile>s
are to be produced.

=item canonize

The name of a method to canonize the path before rsyncing. Only
supported value is C<naive_path_normalize>. Defaults to that.

=item comment

A comment about this tree and setup.

=item filenameroot

The (prefix of the) filename we use for this I<recentfile>. Defaults to
C<RECENT>.

=item ignore_link_stat_errors

If set to true, rsync errors are ignored that complain about link stat
errors. These seem to happen only when there are files missing at the
origin. In race conditions this can always happen, so it is
recommended to set this value to true.

=item locktimeout

After how many seconds shall we die if we cannot lock a I<recentfile>?
Defaults to 600 seconds.

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

=item protocol

When the RECENT file format changes, we increment the protocol. We try
to support older protocols in later releases.

=item remote_dir

The directory we are mirroring from.

=item remote_host

The host we are mirroring from. Leave empty for the local filesystem.

=item remote_module

Rsync servers have so called modules to separate directory trees from
each other. Put here the name of the module under which we are
mirroring. Leave empty for local filesystem.

=item rsync_options

Things like compress, links, times or checksums. Passed in to the
File::Rsync object used to run the mirror.

=item serializer_suffix

Untested accessor. The only tested format for I<recentfile>s at the
moment is YAML. It is used with YAML::Syck via Data::Serializer. But
in principle other formats are supported as well. See section
SERIALIZERS below.

=item sleep_per_connection

Sleep that many seconds (floating point OK) after every chunk of rsyncing
has finished. Defaults to arbitrary 0.42.

=item verbose

Boolean to turn on a bit verbosity.

=back

=cut

use accessors @accessors;

=head1 METHODS

=head2 (void) $obj->aggregate

Takes all intervals that are collected in the accessor called
aggregator. Sorts them numerically by actual length of the interval.
Removes those that are shorter than our own interval. Then merges this
object into the next larger object. The merging continues upwards
as long as the next I<recentfile>s is old enough to warrant a merge.

If a merge is warranted is decided according to the interval of the
previous interval so that larger files are not so often updated as
smaller ones.

Here is an example to illustrate the behaviour. Given aggregators

  1h 1d 1W 1M 1Q 1Y Z

then

  1h updates 1d on every call to aggregate()
  1d updates 1W earliest after 1h
  1W updates 1M earliest after 1d
  1M updates 1Q earliest after 1W
  1Q updates 1Y earliest after 1M
  1Y updates  Z earliest after 1Q

=cut

sub aggregate {
    my($self) = @_;
    my @aggs = sort { $a->{secs} <=> $b->{secs} }
        grep { $_->{secs} >= $self->interval_secs }
            map { { interval => $_, secs => $self->interval_secs($_)} }
                $self->interval, @{$self->aggregator || []};
    $aggs[0]{object} = $self;
  AGGREGATOR: for my $i (0..$#aggs-1) {
        my $this = $aggs[$i]{object};
        my $next = Storable::dclone $this;
        $next->interval($aggs[$i+1]{interval});
        my $want_merge = 0;
        if ($i == 0) {
            $want_merge = 1;
        } else {
            my $next_rfile = $next->rfile;
            if (-e $next_rfile) {
                my $prev = $aggs[$i-1]{object};
                local $^T = time;
                my $next_age = 86400 * -M $next_rfile;
                if ($next_age > $prev->interval_secs) {
                    $want_merge = 1;
                }
            } else {
                $want_merge = 1;
            }
        }
        if ($want_merge) {
            $next->merge($this);
            $aggs[$i+1]{object} = $next;
        } else {
            last AGGREGATOR;
        }
    }
}

sub _debug_aggregate {
    my($self) = @_;
    my @aggs = sort { $a->{secs} <=> $b->{secs} }
        map { { interval => $_, secs => $self->interval_secs($_)} }
            $self->interval, @{$self->aggregator || []};
    my $report = [];
    for my $i (0..$#aggs) {
        my $this = Storable::dclone $self;
        $this->interval($aggs[$i]{interval});
        my $rfile = $this->rfile;
        my @stat = stat $rfile;
        push @$report, [$rfile, map {$stat[$_]||"undef"} 7,9];
    }
    $report;
}

# (void) $self->_assert_symlink()
sub _assert_symlink {
    my($self) = @_;
    my $symlink = File::Spec->catfile
        (
         $self->localroot,
         sprintf
         (
          "%s.recent",
          $self->filenameroot
         )
        );
    my $howto_create_symlink; # 0=no need; 1=straight symlink; 2=rename symlink
    if (-l $symlink) {
        my $found_symlink = readlink $symlink;
        if ($found_symlink eq $self->recentfile_basename) {
            return;
        } else {
            $howto_create_symlink = 2;
        }
    } else {
        $howto_create_symlink = 1;
    }
    if (1 == $howto_create_symlink) {
        symlink $self->recentfile_basename, $symlink or die "Could not create symlink '$symlink': $!"
    } else {
        unlink "$symlink.$$"; # may fail
        symlink $self->recentfile_basename, "$symlink.$$" or die "Could not create symlink '$symlink.$$': $!";
        rename "$symlink.$$", $symlink or die "Could not rename '$symlink.$$' to $symlink: $!";    }
}

=head2 $success = $obj->full_mirror

(TBD) Mirrors the whole remote site, starting with the smallest I<recentfile>,
switching to larger ones ...

=cut

sub full_mirror {
    my($self) = @_;
    warn "Not yet implemented";
}

=head2 $tempfilename = $obj->get_remote_recentfile_as_tempfile

Stores the remote I<recentfile> locally as a tempfile

=cut

sub get_remote_recentfile_as_tempfile {
    my($self) = @_;
    mkpath $self->localroot;
    my($fh) = File::Temp->new(TEMPLATE => sprintf(".%s-XXXX",
                                                  $self->recentfile_basename,
                                                 ),
                              DIR => $self->localroot,
                              SUFFIX => $self->serializer_suffix,
                              UNLINK => 0,
                             );
    my($trecentfile) = $fh->filename;
    my $rfile = $self->rfile;
    if (-e $rfile) {
        # saving on bandwidth. Might need to be configurable
        # $self->bandwidth_is_cheap?
        cp $rfile, $trecentfile or die "Could not copy '$rfile' to '$trecentfile': $!"
    }
    while (!$self->rsync->exec(
                               src => join("/",
                                           $self->remotebase,
                                           $self->recentfile_basename),
                               dst => $trecentfile,
                              )) {
        $self->register_rsync_error ($self->rsync->err);
    }
    $self->un_register_rsync_error ();
    my $mode = 0644;
    chmod $mode, $trecentfile or die "Could not chmod $mode '$trecentfile': $!";
    $self->_current_tempfile ($trecentfile);
    return $trecentfile;
}

=head2 $obj->interval ( $interval_spec )

Get/set accessor. $interval_spec is a string and described below in
the section INTERVAL SPEC.

=cut

sub interval {
    my ($self, $interval) = @_;
    if (@_ >= 2) {
        $self->_interval($interval);
        $self->_rfile(undef);
    }
    $interval = $self->_interval;
    unless (defined $interval) {
        # do not ask the $self too much, it recurses!
        die "Alert: interval undefined for '".$self."'. Cannot continue.";
    }
    return $interval;
}

=head2 $secs = $obj->interval_secs ( $interval_spec )

$interval_spec is described below in the section INTERVAL SPEC. If
empty defaults to the inherent interval for this object.

=cut

sub interval_secs {
    my ($self, $interval) = @_;
    $interval ||= $self->interval;
    unless (defined $interval) {
        die "interval_secs() called without argument on an object without a declared one";
    }
    my ($n,$t) = $interval =~ /^(\d*)([smhdWMQYZ]$)/ or
        die "Could not determine seconds from interval[$interval]";
    if ($interval eq "Z") {
        return MAX_INT;
    } elsif (exists $seconds{$t} and $n =~ /^\d+$/) {
        return $seconds{$t}*$n;
    } else {
        die "Invalid interval specification: n[$n]t[$t]";
    }
}

=head2 $obj->localroot ( $localroot )

Get/set accessor. The local root of the tree.

=cut

sub localroot {
    my ($self, $localroot) = @_;
    if (@_ >= 2) {
        $self->_localroot($localroot);
        $self->_rfile(undef);
    }
    $localroot = $self->_localroot;
}

=head2 $ret = $obj->local_event_path

Misnomer, deprecated. Use local_path instead

=cut

sub local_event_path {
    my($self,$path) = @_;
    require Carp;
    Carp::cluck("Deprecated method local_event_path called. Please use local_path instead");
    my @p = split m|/|, $path; # rsync paths are always slash-separated
    File::Spec->catfile($self->localroot,@p);
}

=head2 $ret = $obj->local_path($path_found_in_recentfile)

Combines the path to our local mirror and the path of an object found
in this I<recentfile>. In other words: the target of a mirro operation

=cut

sub local_path {
    my($self,$path) = @_;
    unless (defined $path) {
        return $self->localroot;
    }
    my @p = split m|/|, $path; # rsync paths are always slash-separated
    File::Spec->catfile($self->localroot,@p);
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
    my $start = time;
    my $locktimeout = $self->locktimeout || 600;
    while (not mkdir "$rfile.lock") {
        Time::HiRes::sleep 0.01;
        if (time - $start > $locktimeout) {
            die "Could not acquire lockdirectory '$rfile.lock': $!";
        }
    }
    $self->_is_locked (1);
}

=head2 $ret = $obj->merge ($other)

Bulk update of this object with another one. It's intended (but not
enforced) to only merge smaller and younger $other objects into the
current one. If the other file is a C<Z> file, then we do not merge in
objects of type C<delete>. But if we encounter an object of type
delete we delete the corresponding C<add> object.

=cut

sub merge {
    my($self,$other) = @_;
    my $canonmeth = $self->canonize;
    unless ($canonmeth) {
        $canonmeth = "naive_path_normalize";
    }
    my $lrd = $self->localroot;
    my $other_recent_events = $other->recent_events;
    $self->lock;
    my $interval = $self->interval;
    my $secs = $self->interval_secs();
    my $recent = $self->recent_events;
    unless (@$recent) {
        $recent = [];
        $self->recent_events($recent);
    }
    my($epoch,$oldest_allowed);
    # reverse combined with unshift smells. This can be done by
    # starting with hashifying both lists, concatenation, and removing
    # the duplicates. Need to write better tests to make sure I get it
    # right
    for my $ev (reverse @$other_recent_events) {
        my $path = $ev->{path};
        $path = $self->$canonmeth($path);
        unless ($epoch) {
            $epoch = $ev->{epoch};
            $oldest_allowed = $epoch-$secs;
        }
        # smells of inefficiency
        while (@$recent && $recent->[-1]{epoch} < $oldest_allowed) {
            pop @$recent;
        }
        # more smells:
        $recent = [ grep { $_->{path} ne $path } @$recent ];
        # stinking:
        if ($self->interval eq "Z" and $ev->{type} eq "delete") {
            # a Z file has no deletes, only living objects
        } else {
            unshift @$recent, { epoch => $ev->{epoch}, path => $path, type => $ev->{type} };
        }
    }
    # sort?
    $self->write_recent($recent);
    $self->unlock;
}

=head2 $hashref = $obj->meta_data

Returns the hashref of metadata that the server has to add to the
I<recentfile>.

=cut

sub meta_data {
    my($self) = @_;
    my $ret = $self->{meta};
    for my $m (
               "aggregator",
               "canonize",
               "comment",
               "filenameroot",
               "interval",
               "protocol",
              ) {
        $ret->{$m} = $self->$m;
    }
    # XXX need to reset the Producer if I am a writer, keep it when I
    # am a reader
    $ret->{Producers} ||= {
                           __PACKAGE__, "$VERSION", # stringified it looks better
                          };
    return $ret;
}

=head2 $success = $obj->mirror ( %options )

Mirrors the files in this I<recentfile>. If $options{after} is
specified, only file events after this timestamp are being mirrored.

=cut

sub mirror {
    my($self, %options) = @_;
    my $trecentfile = $self->get_remote_recentfile_as_tempfile();
    my ($recent_data) = $self->recent_events_from_tempfile();
    my $i = 0;
    my @error;
    my @collector;
    my $last_item = $#$recent_data;
    if (defined $options{after}) {
        if ($recent_data->[0]{epoch} > $options{after}) {
            if (
                my $f = first
                        {$recent_data->[$_]{epoch} <= $options{after}}
                        0..$#$recent_data
               ) {
                $last_item = $f-1;
            }
        } else {
            $last_item = -1;
        }
    }
  ITEM: for my $i (0..$last_item) {
        my $recent_event = $recent_data->[$i];
        my $dst = $self->local_path($recent_event->{path});
        if ($recent_event->{type} eq "new"){
            if ($self->verbose) {
                my $doing = -e $dst ? "Syncing" : "Getting";
                printf STDERR
                    (
                     "%s (%d/%d) %s ... ",
                     $doing,
                     1+$i,
                     1+$last_item,
                     $recent_event->{path},
                    );
            }
            my $max_files_per_connection = $self->max_files_per_connection || 42;
            my $success;
            if ($max_files_per_connection == 1) {
                # old code path may go away when the collector has
                # proved useful...
                $success = eval { $self->mirror_path($recent_event->{path}) };
            } else {
                if ($self->verbose) {
                    print STDERR "\n";
                }
                push @collector, $recent_event->{path};
                if (@collector == $max_files_per_connection) {
                    $success = eval { $self->mirror_path(\@collector) };
                    @collector = ();
                    my $sleep = $self->sleep_per_connection;
                    $sleep = 0.42 unless defined $sleep;
                    Time::HiRes::sleep $sleep;
                } else {
                    next ITEM;
                }
            }
            if (!$success || $@) {
                warn "error while mirroring: $@";
                push @error, $@;
                sleep 1;
            }
            if ($self->verbose) {
                print STDERR "DONE\n";
            }
        } elsif ($recent_event->{type} eq "delete") {
            if (-l $dst or not -d _) {
                unlink $dst or warn "error while unlinking '$dst': $!";
            } else {
                rmdir $dst or warn "error on rmdir '$dst': $!";
            }
        } else {
            warn "Warning: invalid upload type '$recent_event->{type}'";
        }
    }
    if (@collector) {
        my $success = eval { $self->mirror_path(\@collector) };
        if (!$success || $@) {
            warn "Warning: Unknown error while mirroring: $@";
            push @error, $@;
            sleep 1;
        }
        if ($self->verbose) {
            print STDERR "DONE\n";
        }
    }
    rename $trecentfile, $self->rfile;
    return !@error;
}

=head2 (void) $obj->mirror_loop

Run mirror in an endless loop. See the accessor loopinterval. XXX What
happens if we miss the interval during a single loop?

=cut

sub mirror_loop {
    my($self) = @_;
    my $iteration_start = time;

    my $Signal = 0;
    $SIG{INT} = sub { $Signal++ };
    my $loopinterval = $self->loopinterval || 42;
    my $after = -999999999;
  LOOP: while () {
        $self->mirror($after);
        last LOOP if $Signal;
        my $re = $self->recent_events;
        $after = $re->[0]{epoch};
        if ($self->verbose) {
            local $| = 1;
            print "($after)";
        }
        if (time - $iteration_start < $loopinterval) {
            sleep $iteration_start + $loopinterval - time;
        }
        if ($self->verbose) {
            local $| = 1;
            print "~";
        }
    }
}

=head2 $success = $obj->mirror_path ( $arrref | $path )

If the argument is a scalar, fetches a remote path into the local
copy. $path is the path found in the I<recentfile>, i.e. it is relative
to the root directory of the mirror.

If $path is an array reference then all elements are treated as a path
below the current tree and all are rsynced with a single command (and
a single connection).

=cut

sub mirror_path {
    my($self,$path) = @_;
    if (ref $path and ref $path eq "ARRAY") {
        my $dst = $self->local_path();
        mkpath dirname $dst;
        my($fh) = File::Temp->new(TEMPLATE => sprintf(".%s-XXXX",
                                                      lc $self->filenameroot,
                                                     ),
                                  TMPDIR => 1,
                                  UNLINK => 0,
                                 );
        for my $p (@$path) {
            print $fh $p, "\n";
        }
        $fh->flush;
        $fh->unlink_on_destroy(1);
        while (!$self->rsync->exec
               (
                src => join("/",
                            $self->remotebase,
                           ),
                dst => $dst,
                'files-from' => $fh->filename,
               )) {
            my($err) = $self->rsync->err;
            if ($self->ignore_link_stat_errors && $err =~ m{^ rsync: \s link_stat }x ) {
                if ($self->verbose) {
                    warn "Info: ignoring link_stat error '$err'";
                }
                return 1;
            }
            $self->register_rsync_error ($err);
        }
        $self->un_register_rsync_error ();
    } else {
        my $dst = $self->local_path($path);
        mkpath dirname $dst;
        while (!$self->rsync->exec
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
            $self->register_rsync_error ($err);
        }
        $self->un_register_rsync_error ();
    }
    return 1;
}

=head2 $path = $obj->naive_path_normalize ($path)

Takes an absolute unix style path as argument and canonicalizes it to
a shorter path if possible, removing things like double slashes or
C</./> and removes references to C<../> directories to get a shorter
unambiguos path. This is used to make the code easier that determines
if a file passed to C<upgrade()> is indeed below our C<localroot>.

=cut

sub naive_path_normalize {
    my($self,$path) = @_;
    $path =~ s|/+|/|g;
    1 while $path =~ s|/[^/]+/\.\./|/|;
    $path =~ s|/$||;
    $path;
}

=head2 $ret = $obj->read_recent_1 ( $recent_data )

Delegate of C<recent_events()> on protocol 1

=cut

sub read_recent_1 {
    my($self,$data) = @_;
    return $data->{recent};
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

=head2 $ret = $obj->recentfile

deprecated, use rfile instead

=cut

sub recentfile {
    my($self) = @_;
    require Carp;
    Carp::cluck("deprecated method recentfile called. Please use rfile instead!");
    my $recent = File::Spec->catfile(
                                     $self->localroot,
                                     $self->recentfile_basename,
                                    );
    return $recent;
}

=head2 $ret = $obj->recentfile_basename

Just the basename of our I<recentfile>, composed from C<filenameroot>,
C<interval>, and C<serializer_suffix>. E.g. C<RECENT-6h.yaml>

=cut

sub recentfile_basename {
    my($self) = @_;
    my $file = sprintf("%s-%s%s",
                       $self->filenameroot,
                       $self->interval,
                       $self->serializer_suffix,
                      );
    return $file;
}

=head2 $str = $obj->remotebase

Returns the composed prefix needed when rsyncing from a remote module.

=cut

sub remotebase {
    my($self) = @_;
    my $remotebase = $self->_remotebase;
    unless (defined $remotebase) {
        $remotebase = sprintf
            (
             "%s%s%s",
             defined $self->remote_host   ? ($self->remote_host."::")  : "",
             defined $self->remote_module ? ($self->remote_module."/") : "",
             defined $self->remote_dir    ? $self->remote_dir          : "",
            );
        $self->_remotebase($remotebase);
    }
    return $remotebase;
}

=head2 my $rfile = $obj->rfile

Returns the full path of the I<recentfile>

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
             $self->recentfile_basename,
            );
        $self->_rfile ($rfile);
        return $rfile;
    }
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

=head2 (void) $obj->register_rsync_error($err)

=head2 (void) $obj->un_register_rsync_error()

Register_rsync_error is called whenever the File::Rsync object fails
on an exec (say, connection doesn't succeed). It issues a warning and
sleeps for an increasing amount of time. Un_register_rsync_error
resets the sleep time.

=cut

{
    my $no_success_count = 0;
    my $no_success_time = 0;
    sub register_rsync_error {
        my($self, $err) = @_;
        $no_success_time = time;
        $no_success_count++;
        my $sleep = 12 * $no_success_count;
        $sleep = 120 if $sleep > 120;
        warn sprintf
            (
             "Warning: %s, Error while rsyncing: '%s', sleeping %d",
             scalar(localtime($no_success_time)),
             $err,
             $sleep,
            );
        sleep $sleep
    }
    sub un_register_rsync_error {
        my($self) = @_;
        $no_success_time = 0;
        $no_success_count = 0;
    }
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

=head2 $ret = $obj->update ($path, $type)

Enter one file into the local I<recentfile>. $path is the (usually
absolute) path. If the path is outside the I<our> tree, then it is
ignored.

$type is one of C<new> or C<delete>.

=cut

sub update {
    my($self,$path,$type) = @_;
    die "write_recent called without path argument" unless defined $path;
    die "write_recent called without type argument" unless defined $type;
    die "write_recent called with illegal type argument: $type" unless $type =~ /(new|delete)/;
    my $meth = $self->canonize;
    unless ($meth) {
        $meth = "naive_path_normalize";
    }
    $path = $self->$meth($path);
    my $lrd = $self->localroot;
    if ($path =~ s|^\Q$lrd\E||) {
        $path =~ s|^/||;
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
        # sort?
        $self->write_recent($recent);
        $self->_assert_symlink;
        $self->unlock;
    }
}

=head2 $obj->write_recent ($recent_files_arrayref)

Writes a I<recentfile> based on the current reflection of the current
state of the tree limited by the current interval.

=cut

sub write_recent {
    my ($self,$recent) = @_;
    die "write_recent called without argument" unless defined $recent;
    my $meth = sprintf "write_%d", $self->protocol;
    $self->$meth($recent);
}

=head2 $obj->write_0 ($recent_files_arrayref)

Delegate of C<write_recent()> on protocol 0

=cut

sub write_0 {
    my ($self,$recent) = @_;
    my $rfile = $self->rfile;
    YAML::Syck::DumpFile("$rfile.new",$recent);
    rename "$rfile.new", $rfile or die "Could not rename to '$rfile': $!";
}

=head2 $obj->write_1 ($recent_files_arrayref)

Delegate of C<write_recent()> on protocol 1

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

BEGIN {
    my @pod_lines = 
        split /\n/, <<'=cut'; %serializers = map { eval } grep {s/^=item\s+C<<(.+)>>$/$1/} @pod_lines; }

=head1 THE ARCHITECTURE OF A COLLECTION OF RECENTFILES

The idea is that we want to have a short file that records really
recent changes. So that a fresh mirror can be kept fresh as long as
the connectivity is given. Then we want longer files that record the
history before. So when the mirror falls behind the update period
reflected in the shortest file, it can switch to the next one. And if
this is not long enough we want another one, again a bit longer. And
we want one that completes the history back to the oldest file. For
practical reasons the timespans of these files must overlap a bit and
to keep the bandwidth necessities low they must not be
updated too frequently. That's the basic idea. The following
example represents a tree that has a few updates every day:

 RECENT-1h.yaml
 RECENT-6h.yaml
 RECENT-1d.yaml
 RECENT-1M.yaml
 RECENT-1W.yaml
 RECENT-1Q.yaml
 RECENT-1Y.yaml
 RECENT-Z.yaml

The last file, the Z file, contains the complementary files that are
in none of the other files. It does never contain C<deletes>. Besides
this it serves the role of a recovery mechanism or spill over pond.
When things go wrong, it's a valuable controlling instance to hold the
differences between the collection of limited interval files and the
actual filesystem.

=head2 A SINGLE RECENTFILE

A I<recentfile> consists of a hash that has two keys: C<meta> and
C<recent>. The C<meta> part has metadata and the C<recent> part has a
list of fileobjects.

=head2 THE META PART

Here we find things that are pretty much self explaining: all
lowercase attributes are accessors and as such explained somewhere
above in this manpage. The uppercase attribute C<Producers> contains
version information about involved software components. Nothing to
worry about as I believe.

=head2 THE RECENT PART

This is the interesting part. Every entry refers to some filesystem
change (with path, epoch, type). The epoch value is the point in time
when some change was I<registered>. Do not be tempted to believe that
the entry has a direct relation to something like modification time or
change time on the filesystem level. The timestamp (I<epoch> element)
is a floating point number and does practically never correspond
exactly to the data recorded in the filesystem but rather to the time
when some process succeeded to report to the I<recentfile> mechanism
that something has changed. This is why many parts of the code refer
to I<events>, because we merely try to record the I<event> of the
discovery of a change, not the time of the change itself.

All these entries can be devided into two types (denoted by the
C<type> attribute): C<new>s and C<delete>s. Changes and creations are
C<new>s. Deletes are C<delete>s.

Another distinction is for objects with an epoch timestamp and others
without. All files that were already existing on the filesystem before
the I<recentfile> mechanism was installed, get recorded with a
timestamp of zero.

Besides an C<epoch> and a C<type> attribute we find a third one:
C<path>. This path is relative to the directory we find the
I<recentfile> in.

The order of the entries in the I<recentfile> is by decreasing epoch
attribute. These are either 0 or a unique floating point number. They
are zero for events that were happening either before the time that
the I<recentfile> mechanism was set up or were left undiscovered for a
while and never handed over to update(). They are floating point
numbers for all events being regularly handed to update(). And when
the server has ntp running correctly, then the timestamps are
actually decreasing and unique.

=head1 CORRUPTION AND RECOVERY

If the origin host breaks the promise to deliver consistent and
complete I<recentfiles> then the way back to sanity shall be achieved
through either the C<zloop> (still TBD) or traditional rsyncing
between the hosts. For example, if the origin server forgets to deploy
ntp and the clock on it jumps backwards some day, then this would
probably go unnoticed for a while and many software components that
rely on the time never running backwards will make wrong decisions.
After some time this accident would probably still be found in one of
the I<recentfiles> but would become meaningless as soon as a mirror
has run through the sanitizing procedures. Same goes for origin hosts
that forget to include or deliberately omit some files.

=head1 SERIALIZERS

The following suffixes are supported and trigger the use of these
serializers:

=over 4

=item C<< ".yaml" => "YAML::Syck" >>

=item C<< ".json" => "JSON" >>

=item C<< ".sto"  => "Storable" >>

=item C<< ".dd"   => "Data::Dumper" >>

=back

=cut

BEGIN {
    my @pod_lines = 
        split /\n/, <<'=cut'; %seconds = map { eval } grep {s/^=item\s+C<<(.+)>>$/$1/} @pod_lines; }

=head1 INTERVAL SPEC

An interval spec is a primitive way to express time spans. Normally it
is composed from an integer and a letter.

As a special case, a string that consists only of the single letter
C<Z>, stands for unlimited time.

The following letters express the specified number of seconds:

=over 4

=item C<< s => 1 >>

=item C<< m => 60 >>

=item C<< h => 60*60 >>

=item C<< d => 60*60*24 >>

=item C<< W => 60*60*24*7 >>

=item C<< M => 60*60*24*30 >>

=item C<< Q => 60*60*24*90 >>

=item C<< Y => 60*60*24*365.25 >>

=back

=cut

=head1 BACKGROUND

This is about speeding up rsync operation on large trees to many
places. Uses a small metadata cocktail and pull technology.

=head2 NON-COMPETITORS

 File::Mirror        JWU/File-Mirror/File-Mirror-0.10.tar.gz only local trees
 Mirror::YAML        ADAMK/Mirror-YAML-0.03.tar.gz           some sort of inner circle
 Net::DownloadMirror KNORR/Net-DownloadMirror-0.04.tar.gz    FTP sites and stuff
 Net::MirrorDir      KNORR/Net-MirrorDir-0.05.tar.gz         dito
 Net::UploadMirror   KNORR/Net-UploadMirror-0.06.tar.gz      dito
 Pushmi::Mirror      CLKAO/Pushmi-v1.0.0.tar.gz              something SVK

=head2 COMPETITORS

The problem to solve which clusters and ftp mirrors and otherwise
replicated datasets like CPAN share: how to transfer only a minimum
amount of data to determine the diff between two hosts.

Normally it takes a long time to determine the diff itself before it
can be transferred. Known solutions at the time of this writing are
csync2, and rsync 3 batch mode.

For many years the best solution was csync2 which solves the
problem by maintining a sqlite database on both ends and talking a
highly sophisticated protocol to quickly determine which files to send
and which to delete at any given point in time. Csync2 is often
inconvenient because the act of syncing demands quite an intimate
relationship between the sender and the receiver and suffers when the
number of syncing sites is large or connections are unreliable.

Rsync 3 batch mode works around these problems by providing rsync-able
batch files which allow receiving nodes to replay the history of the
other nodes. This reduces the need to have an incestuous relation but
it has the disadvantage that these batch files replicate the contents
of the involved files. This seems inappropriate when the nodes already
have a means of communicating over rsync.

rersyncrecent solves this problem with a couple of (usually 2-10)
index files which cover different overlapping time intervals. The
master writes these files and the clients can construct the full tree
from the information contained in them. The most recent index file
usually covers the last seconds or minutes or hours of the tree and
depending on the needs, slaves can rsync every few seconds and then
bring their trees in full sync.

The rersyncrecent mode was developed for CPAN but I hope it is a
convenient and economic general purpose solution. I'm looking forward
to see a CPAN backbone that is only a few seconds behind PAUSE. And
then ... the first FUSE based CPAN filesystem anyone?

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
