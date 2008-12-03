package File::Rsync::Mirror::Recentfile;

# use warnings;
use strict;

=encoding utf-8

=head1 NAME

File::Rsync::Mirror::Recentfile - mirroring via rsync made efficient

=head1 VERSION

Version 0.0.1

=cut

my $HAVE = {};
for my $package (
                 "Data::Serializer",
                 "File::Rsync"
                ) {
    $HAVE->{$package} = eval qq{ require $package; };
}
use Config;
use File::Basename qw(basename dirname fileparse);
use File::Copy qw(cp);
use File::Path qw(mkpath);
use File::Rsync::Mirror::Recentfile::FakeBigFloat qw(:all);
use File::Temp;
use List::Util qw(first min);
use Scalar::Util qw(reftype);
use Storable;
use Time::HiRes qw();
use YAML::Syck;

use version; our $VERSION = qv('0.0.1');


use constant MAX_INT => ~0>>1; # anything better?
use constant DEFAULT_PROTOCOL => 1;

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

=head1 CONSTRUCTORS / DESTRUCTOR

=head2 my $obj = CLASS->new(%hash)

Constructor. On every argument pair the key is a method name and the
value is an argument to that method name.

If a recentfile for this resource already exists, metadata that are
not defined by the constructor will be fetched from there as soon as
it is being read by recent_events().

=cut

sub new {
    my($class, @args) = @_;
    my $self = bless {}, $class;
    while (@args) {
        my($method,$arg) = splice @args, 0, 2;
        $self->$method($arg);
    }
    unless (defined $self->protocol) {
        $self->protocol(DEFAULT_PROTOCOL);
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
    # XXX: we can skip this step when the metadata are sufficient, but
    # we cannot parse the file without some magic stuff about
    # serialized formats
    while (-l $file) {
        my($name,$path) = fileparse $file;
        my $symlink = readlink $file;
        if ($symlink =~ m|/|) {
            die "FIXME: filenames containing '/' not supported, got $symlink";
        }
        $file = File::Spec->catfile ( $path, $symlink );
    }
    my($name,$path,$suffix) = fileparse $file, keys %serializers;
    $self->serializer_suffix($suffix);
    $self->localroot($path);
    die "Could not determine file format from suffix" unless $suffix;
    my $deserialized;
    if ($suffix eq ".yaml") {
        require YAML::Syck;
        $deserialized = YAML::Syck::LoadFile($file);
    } elsif ($HAVE->{"Data::Serializer"}) {
        my $serializer = Data::Serializer->new
            ( serializer => $serializers{$suffix} );
        $deserialized = $serializer->raw_deserialize($serialized);
    } else {
        die "Data::Serializer not installed, cannot proceed with suffix '$suffix'";
    }
    while (my($k,$v) = each %{$deserialized->{meta}}) {
        next if $k ne lc $k; # "Producers"
        $self->$k($v);
    }
    unless (defined $self->protocol) {
        $self->protocol(DEFAULT_PROTOCOL);
    }
    return $self;
}

=head2 DESTROY

A simple unlock.

=cut
sub DESTROY { shift->unlock }

=head1 ACCESSORS

=cut

my @accessors;

BEGIN {
    @accessors = (
                  "_current_tempfile",
                  "_current_tempfile_fh",
                  "_delayed_operations",
                  "_done",
                  "_interval",
                  "_is_locked",
                  "_localroot",
                  "_merged",
                  "_pathdb",
                  "_remember_last_uptodate_call",
                  "_remote_dir",
                  "_remoteroot",
                  "_rfile",
                  "_rsync",
                  "_seeded",
                  "_uptodateness_ever_reached",
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

=item dirtymark

A timestamp. The dirtymark is updated whenever an out of band change
on the origin server is performed that violates the protocol. Say,
they add or remove files in the middle somewhere. Slaves must react
with a devaluation of their C<done> structure which then leads to a
full re-sync of all files.

=item filenameroot

The (prefix of the) filename we use for this I<recentfile>. Defaults to
C<RECENT>.

=item have_mirrored

Timestamp remembering when we mirrored this recentfile the last time.
Only relevant for slaves.

=item ignore_link_stat_errors

If set to true, rsync errors are ignored that complain about link stat
errors. These seem to happen only when there are files missing at the
origin. In race conditions this can always happen, so it is
recommended to set this value to true.

=item is_slave

If set to true, this object will fetch a new recentfile from remote
when the timespan between the last mirror (see have_mirrored) and now
is too large (currently hardcoded arbitrary 420 seconds).

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

=item max_rsync_errors

When rsync operations encounter that many errors without any resetting
success in between, then we die. Defaults to unlimited. A value of
-1 means we run forever ignoring all rsync errors.

=item minmax

Hashref remembering when we read the recent_events from this file the
last time and what the timespan was.

=item protocol

When the RECENT file format changes, we increment the protocol. We try
to support older protocols in later releases.

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

Mostly untested accessor. The only well tested format for
I<recentfile>s at the moment is YAML. It is used with YAML::Syck via
Data::Serializer. But in principle other formats are supported as
well. See section SERIALIZERS below.

=item sleep_per_connection

Sleep that many seconds (floating point OK) after every chunk of rsyncing
has finished. Defaults to arbitrary 0.42.

=item ttl

Time to live. Number of seconds after which this recentfile must be
fetched again from the origin server. Only relevant for slaves.
Defaults to arbitrary 24.2 seconds.

=item verbose

Boolean to turn on a bit verbosity.

=back

=cut

use accessors @accessors;

=head1 METHODS

=head2 (void) $obj->aggregate

Takes all intervals that are collected in the accessor called
aggregator. Sorts them by actual length of the interval.
Removes those that are shorter than our own interval. Then merges this
object into the next larger object. The merging continues upwards
as long as the next I<recentfile> is old enough to warrant a merge.

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

Note that all but the smallest recentfile get updated at an arbitrary
rate and as such are quite useless on their own.

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
        my $next = $this->_sparse_clone;
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

# collect file size and mtime for all files of this aggregate
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
        push @$report, {rfile => $rfile, size => $stat[7], mtime => $stat[9]};
    }
    $report;
}

# (void) $self->_assert_symlink()
sub _assert_symlink {
    my($self) = @_;
    my $recentrecentfile = File::Spec->catfile
        (
         $self->localroot,
         sprintf
         (
          "%s.recent",
          $self->filenameroot
         )
        );
    if ($Config{d_symlink} eq "define") {
        my $howto_create_symlink; # 0=no need; 1=straight symlink; 2=rename symlink
        if (-l $recentrecentfile) {
            my $found_symlink = readlink $recentrecentfile;
            if ($found_symlink eq $self->rfilename) {
                return;
            } else {
                $howto_create_symlink = 2;
            }
        } else {
            $howto_create_symlink = 1;
        }
        if (1 == $howto_create_symlink) {
            symlink $self->rfilename, $recentrecentfile or die "Could not create symlink '$recentrecentfile': $!"
        } else {
            unlink "$recentrecentfile.$$"; # may fail
            symlink $self->rfilename, "$recentrecentfile.$$" or die "Could not create symlink '$recentrecentfile.$$': $!";
            rename "$recentrecentfile.$$", $recentrecentfile or die "Could not rename '$recentrecentfile.$$' to $recentrecentfile: $!";
        }
    } else {
        warn "Warning: symlinks not supported on this system, doing a copy instead\n";
        unlink "$recentrecentfile.$$"; # may fail
        cp $self->rfilename, "$recentrecentfile.$$" or die "Could not copy to '$recentrecentfile.$$': $!";
        rename "$recentrecentfile.$$", $recentrecentfile or die "Could not rename '$recentrecentfile.$$' to $recentrecentfile: $!";
    }
}

=head2 $hashref = $obj->delayed_operations

A hash of hashes containing unlink and rmdir operations which had to
wait until the recentfile got unhidden in order to not confuse
downstream mirrors (in case we have some).

=cut

sub delayed_operations {
    my($self) = @_;
    my $x = $self->_delayed_operations;
    unless (defined $x) {
        $x = {
              unlink => {},
              rmdir => {},
             };
        $self->_delayed_operations ($x);
    }
    return $x;
}

=head2 $done = $obj->done

$done is a reference to a File::Rsync::Mirror::Recentfile::Done object
that keeps track of rsync activities. Only needed and used when we are
a mirroring slave.

=cut

sub done {
    my($self) = @_;
    my $done = $self->_done;
    if (!$done) {
        require File::Rsync::Mirror::Recentfile::Done;
        $done = File::Rsync::Mirror::Recentfile::Done->new();
        $done->_rfinterval ($self->interval);
        $self->_done ( $done );
    }
    return $done;
}

=head2 $tempfilename = $obj->get_remote_recentfile_as_tempfile ()

Stores the remote I<recentfile> locally as a tempfile. The caller is
responsible to remove the file after use.

Note: if you're intending to act as an rsync server for other slaves,
then you must prefer this method to fetch that file with
get_remotefile(). Otherwise downstream mirrors would expect you to
already have mirrored all the files that are in the I<recentfile>
before you have them mirrored.

=cut

sub get_remote_recentfile_as_tempfile {
    my($self) = @_;
    mkpath $self->localroot;
    my $fh;
    my $trfilename;
    if ( $self->_use_tempfile() ) {
        return $self->_current_tempfile if ! $self->ttl_reached;
        $fh = $self->_current_tempfile_fh;
        $trfilename = $self->rfilename;
    } else {
        $trfilename = $self->rfilename;
    }

    my $dst;
    if ($fh) {
        $dst = $self->_current_tempfile;
    } else {
        $fh = $self->_get_remote_rat_provide_tempfile_object ($trfilename);
        $dst = $fh->filename;
        $self->_current_tempfile ($dst);
        my $rfile = eval { $self->rfile; }; # may fail (RECENT.recent has no rfile)
        if (defined $rfile && -e $rfile) {
            # saving on bandwidth. Might need to be configurable
            # $self->bandwidth_is_cheap?
            cp $rfile, $dst or die "Could not copy '$rfile' to '$dst': $!"
        }
    }
    my $src = join ("/",
                    $self->remoteroot,
                    $trfilename,
                   );
    if ($self->verbose) {
        my $doing = -e $dst ? "Sync" : "Get";
        my $display_dst = join "/", "...", basename(dirname($dst)), basename($dst);
        printf STDERR
            (
             "%-4s %d (1/1/%s) temp %s ... ",
             $doing,
             time,
             $self->interval,
             $display_dst,
            );
    }
    my $gaveup = 0;
    my $retried = 0;
    while (!$self->rsync->exec(
                               src => $src,
                               dst => $dst,
                              )) {
        $self->register_rsync_error ($self->rsync->err);
        if (++$retried >= 3) {
            warn "XXX giving up";
            $gaveup = 1;
            last;
        }
    }
    if ($gaveup) {
        printf STDERR "Warning: gave up mirroring %s, will try again later", $self->interval;
    } else {
        $self->_refresh_internals ($dst);
        $self->have_mirrored (Time::HiRes::time);
        $self->un_register_rsync_error ();
    }
    if ($self->verbose) {
        print STDERR "DONE\n";
    }
    my $mode = 0644;
    chmod $mode, $dst or die "Could not chmod $mode '$dst': $!";
    return $dst;
}

sub _get_remote_rat_provide_tempfile_object {
    my($self, $trfilename) = @_;
    my $fh = File::Temp->new
        (TEMPLATE => sprintf(".FRMRecent-%s-XXXX",
                             $trfilename,
                            ),
         DIR => $self->localroot,
         SUFFIX => $self->serializer_suffix,
         UNLINK => $self->_use_tempfile,
        );
    if ($self->_use_tempfile) {
        $self->_current_tempfile_fh ($fh); # delay self destruction
    }
    return $fh;
}

=head2 $localpath = $obj->get_remotefile ( $relative_path )

Rsyncs one single remote file to local filesystem.

Note: no locking is done on this file. Any number of processes may
mirror this object.

Note II: do not use for recentfiles. If you are a cascading
slave/server combination, it would confuse other slaves. They would
expect the contents of these recentfiles to be available. Use
get_remote_recentfile_as_tempfile() instead.

=cut

sub get_remotefile {
    my($self, $path) = @_;
    my $dst = File::Spec->catfile($self->localroot, $path);
    mkpath dirname $dst;
    if ($self->verbose) {
        my $doing = -e $dst ? "Sync" : "Get";
        printf STDERR
            (
             "%-4s %d (1/1/%s) %s ... ",
             $doing,
             time,
             $self->interval,
             $path,
            );
    }
    while (!$self->rsync->exec(
                               src => join("/",
                                           $self->remoteroot,
                                           $path),
                               dst => $dst,
                              )) {
        $self->register_rsync_error ($self->rsync->err);
    }
    $self->un_register_rsync_error ();
    if ($self->verbose) {
        print STDERR "DONE\n";
    }
    return $dst;
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
        require Carp;
        Carp::confess("Alert: interval undefined for '".$self."'. Cannot continue.");
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

=head2 $ret = $obj->local_path($path_found_in_recentfile)

Combines the path to our local mirror and the path of an object found
in this I<recentfile>. In other words: the target of a mirror operation.

Implementation note: We split on slashes and then use
File::Spec::catfile to adjust to the local operating system.

=cut

sub local_path {
    my($self,$path) = @_;
    unless (defined $path) {
        # seems like a degenerated case
        return $self->localroot;
    }
    my @p = split m|/|, $path;
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

=head2 (void) $obj->merge ($other)

Bulk update of this object with another one. It's used to merge a
smaller and younger $other object into the current one. If this file
is a C<Z> file, then we do not merge in objects of type C<delete>. But
if we encounter an object of type delete we delete the corresponding
C<new> object if we have it.

If there is nothing to be merged, nothing is done.

=cut

sub merge {
    my($self, $other) = @_;
    $self->_merge_sanitycheck ( $other );
    $other->lock;
    my $other_recent = $other->recent_events || [];
    $self->lock;
    my $my_recent = $self->recent_events || [];

    # calculate the target time span
    my $myepoch = $my_recent->[0] ? $my_recent->[0]{epoch} : undef;
    my $epoch = $other_recent->[0] ? $other_recent->[0]{epoch} : $myepoch;
    my $oldest_allowed = 0;
    my $something_done;
    unless ($my_recent->[0]) {
        # obstetrics
        $something_done=1;
    }
    if ($epoch) {
        if (my $merged = $self->merged) {
            my $secs = $self->interval_secs();
            $oldest_allowed = min($epoch - $secs, $merged->{epoch});
        }
        # throw away outsiders
        # XXX _bigfloat!
        while (@$my_recent && $my_recent->[-1]{epoch} < $oldest_allowed) {
            pop @$my_recent;
            $something_done=1;
        }
    }

    my %have;
    my $recent = [];
    for my $oev (@$other_recent) {
        my $oevepoch = $oev->{epoch} || 0;
        next if $oevepoch < $oldest_allowed;
        my $path = $oev->{path};
        next if $have{$path}++;
        if (    $self->interval eq "Z"
            and $oev->{type}     eq "delete") {
            # do nothing
        } else {
            if (!$myepoch || $oevepoch > $myepoch) {
                $something_done=1;
            }
            push @$recent, { epoch => $oev->{epoch}, path => $path, type => $oev->{type} };
        }
    }
    if ($something_done) {
        $self->_merge_something_done ($recent, $my_recent, $other_recent, $other, \%have, $epoch);
    }
    $self->unlock;
    $other->unlock;
}

sub _merge_something_done {
    my($self, $recent, $my_recent, $other_recent, $other, $have, $epoch) = @_;
    push @$recent, grep { !$have->{$_->{path}}++ } @$my_recent;
    if (_bigfloatgt($other->dirtymark, $self->dirtymark)) {
        $self->dirtymark ( $other->dirtymark );
    }
    $self->write_recent($recent);
    $other->merged({
                    time => Time::HiRes::time, # not used anywhere
                    epoch => $recent->[0]{epoch},
                    into_interval => $self->interval, # not used anywhere
                   });
    $other->write_recent($other_recent);
}

sub _merge_sanitycheck {
    my($self, $other) = @_;
    if ($self->interval_secs <= $other->interval_secs) {
        die sprintf
            (
             "Alert: illegal merge operation of a bigger interval[%d] into a smaller[%d]",
             $self->interval_secs,
             $other->interval_secs,
            );
    }
}

=head2 merged

Hashref denoting when this recentfile has been merged into some other
at which epoch.

=cut

sub merged {
    my($self, $set) = @_;
    if (defined $set) {
        $self->_merged ($set);
    }
    my $merged = $self->_merged;
    my $into;
    if ($merged and $into = $merged->{into_interval} and defined $self->_interval) {
        if ($into eq $self->interval) {
            require Carp;
            Carp::cluck(sprintf
                (
                 "Warning: into_interval[%s] same as own interval[%s]. Danger ahead.",
                 $into,
                 $self->interval,
                ));
        } elsif ($self->interval_secs($into) < $self->interval_secs) {
            require Carp;
            Carp::cluck(sprintf
                (
                 "Warning: into_interval[%s] smaller than own interval[%s] on interval[%s]. Danger ahead.",
                 $self->interval_secs($into),
                 $self->interval_secs,
                 $self->interval,
                ));
        }
    }
    $merged;
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
               "dirtymark",
               "filenameroot",
               "merged",
               "interval",
               "protocol",
               "serializer_suffix",
              ) {
        my $v = $self->$m;
        if (defined $v) {
            $ret->{$m} = $v;
        }
    }
    # XXX need to reset the Producer if I am a writer, keep it when I
    # am a reader
    $ret->{Producers} ||= {
                           __PACKAGE__, "$VERSION", # stringified it looks better
                           '$0', $0,
                           'time', Time::HiRes::time,
                          };
    $ret->{dirtymark} ||= Time::HiRes::time;
    return $ret;
}

=head2 $success = $obj->mirror ( %options )

Mirrors the files in this I<recentfile> as reported by
C<recent_events>. Options named C<after>, C<before>, C<max>, and
C<skip-deletes> are passed through to the L<recent_events> call. The
boolean option C<piecemeal>, if true, causes C<mirror> to only rsync
C<max_files_per_connection> and keep track of the rsynced files so
that future calls will rsync different files until all files are
brought to sync.

=cut

sub mirror {
    my($self, %options) = @_;
    my $trecentfile = $self->get_remote_recentfile_as_tempfile();
    $self->_use_tempfile (1);
    my %passthrough = map { ($_ => $options{$_}) } qw(before after max skip-deletes);
    my ($recent_events) = $self->recent_events(%passthrough);
    my(@error, @xcollector);
    my $first_item = 0;
    my $last_item = $#$recent_events;
    my $done = $self->done;
    my $pathdb = $self->_pathdb;
  ITEM: for my $i ($first_item..$last_item) {
        my $status = +{};
        $self->_mirror_item
            (
             $i,
             $recent_events,
             $last_item,
             $done,
             $pathdb,
             \@xcollector,
             \%options,
             $status,
             \@error,
            );
        last if $i == $last_item;
        return if $status->{mustreturn};
    }
    if (@xcollector) {
        my $success = eval { $self->_mirror_empty_xcollector (\@xcollector,$pathdb,$recent_events);};
        if (!$success || $@) {
            warn "Warning: Unknown error while mirroring: $@";
            push @error, $@;
            sleep 1;
        }
    }
    if ($self->verbose) {
        print STDERR "DONE\n";
    }
    # once we've gone to the end we consider ourselve free of obligations
    $self->unseed;
    $self->_mirror_unhide_tempfile ($trecentfile);
    $self->_mirror_perform_delayed_ops;
    return !@error;
}

sub _mirror_item {
    my($self,
       $i,
       $recent_events,
       $last_item,
       $done,
       $pathdb,
       $xcollector,
       $options,
       $status,
       $error,
      ) = @_;
    my $recent_event = $recent_events->[$i];
    return if $done->covered ( $recent_event->{epoch} );
    if ($pathdb) {
        my $rec = $pathdb->{$recent_event->{path}};
        if ($rec && $rec->{recentepoch}) {
            if (_bigfloatgt
                ( $rec->{recentepoch}, $recent_event->{epoch} )){
                $done->register ($recent_events, [$i]);
                return;
            }
        }
    }
    my $dst = $self->local_path($recent_event->{path});
    if ($recent_event->{type} eq "new"){
        $self->_mirror_item_new
            (
             $dst,
             $i,
             $last_item,
             $recent_events,
             $recent_event,
             $xcollector,
             $pathdb,
             $status,
             $error,
             $options,
            );
    } elsif ($recent_event->{type} eq "delete") {
        my $activity;
        if ($options->{'skip-deletes'}) {
            $activity = "skipped";
        } else {
            if (! -e $dst) {
                $activity = "not_found";
            } elsif (-l $dst or not -d _) {
                $self->delayed_operations->{unlink}{$dst}++;
                $activity = "deleted";
            } else {
                $self->delayed_operations->{rmdir}{$dst}++;
                $activity = "deleted";
            }
        }
        $done->register ($recent_events, [$i]);
        if ($pathdb) {
            $self->_mirror_register_path($pathdb,[$recent_event],$activity);
        }
    } else {
        warn "Warning: invalid upload type '$recent_event->{type}'";
    }
}

sub _mirror_item_new {
    my($self,
       $dst,
       $i,
       $last_item,
       $recent_events,
       $recent_event,
       $xcollector,
       $pathdb,
       $status,
       $error,
       $options,
      ) = @_;
    if ($self->verbose) {
        my $doing = -e $dst ? "Sync" : "Get";
        printf STDERR
            (
             "%-4s %d (%d/%d/%s) %s ... ",
             $doing,
             time,
             1+$i,
             1+$last_item,
             $self->interval,
             $recent_event->{path},
            );
    }
    my $max_files_per_connection = $self->max_files_per_connection || 42;
    my $success;
    if ($self->verbose) {
        print STDERR "\n";
    }
    push @$xcollector, { rev => $recent_event, i => $i };
    if (@$xcollector >= $max_files_per_connection) {
        $success = eval {$self->_mirror_empty_xcollector ($xcollector,$pathdb,$recent_events);};
        my $sleep = $self->sleep_per_connection;
        $sleep = 0.42 unless defined $sleep;
        Time::HiRes::sleep $sleep;
        if ($options->{piecemeal}) {
            $status->{mustreturn} = 1;
            return;
        }
    } else {
        return;
    }
    if (!$success || $@) {
        warn "Warning: Error while mirroring: $@";
        push @$error, $@;
        sleep 1;
    }
    if ($self->verbose) {
        print STDERR "DONE\n";
    }
}

sub _mirror_empty_xcollector {
    my($self,$xcoll,$pathdb,$recent_events) = @_;
    my $success = $self->mirror_path([map {$_->{rev}{path}} @$xcoll]);
    if ($pathdb) {
        $self->_mirror_register_path($pathdb,[map {$_->{rev}} @$xcoll],"rsync");
    }
    $self->done->register($recent_events, [map {$_->{i}} @$xcoll]);
    @$xcoll = ();
    return $success;
}

sub _mirror_register_path {
    my($self,$pathdb,$coll,$activity) = @_;
    my $time = time;
    for my $item (@$coll) {
        $pathdb->{$item->{path}} =
            {
             recentepoch => $item->{epoch},
             ($activity."_on") => $time,
            };
    }
}

sub _mirror_unhide_tempfile {
    my($self, $trecentfile) = @_;
    my $rfile = $self->rfile;
    if (rename $trecentfile, $rfile) {
        # warn "DEBUG: renamed '$trecentfile' to '$rfile'";
    } else {
        require Carp;
        Carp::confess("Could not rename '$trecentfile' to '$rfile': $!");
    }
    $self->_use_tempfile (0);
    if (my $ctfh = $self->_current_tempfile_fh) {
        $ctfh->unlink_on_destroy (0);
        $self->_current_tempfile_fh (undef);
    }
}

sub _mirror_perform_delayed_ops {
    my($self) = @_;
    my $delayed = $self->delayed_operations;
    for my $dst (keys %{$delayed->{unlink}}) {
        unless (unlink $dst) {
            require Carp;
            Carp::cluck ( "Warning: Error while unlinking '$dst': $!" );
        }
        delete $delayed->{unlink}{$dst};
    }
    for my $dst (keys %{$delayed->{rmdir}}) {
        unless (rmdir $dst) {
            require Carp;
            Carp::cluck ( "Warning: Error on rmdir '$dst': $!" );
        }
        delete $delayed->{rmdir}{$dst};
    }
}

=head2 (void) $obj->mirror_loop

Run mirror in an endless loop. See the accessor C<loopinterval>. XXX
What happens/should happen if we miss the interval during a single loop?

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

If the argument is a scalar it is treated as a path. The remote path
is mirrored into the local copy. $path is the path found in the
I<recentfile>, i.e. it is relative to the root directory of the
mirror.

If the argument is an array reference then all elements are treated as
a path below the current tree and all are rsynced with a single
command (and a single connection).

=cut

sub mirror_path {
    my($self,$path) = @_;
    # XXX simplify the two branches such that $path is treated as
    # [$path] maybe even demand the argument as an arrayref to
    # simplify docs and code. (rsync-over-recentfile-2.pl uses the
    # interface)
    if (ref $path and ref $path eq "ARRAY") {
        my $dst = $self->localroot;
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
        my $gaveup = 0;
        my $retried = 0;
        while (!$self->rsync->exec
               (
                src => join("/",
                            $self->remoteroot,
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
            if (++$retried >= 3) {
                warn "XXX giving up.";
                $gaveup = 1;
                last;
            }
        }
        unless ($gaveup) {
            $self->un_register_rsync_error ();
        }
    } else {
        my $dst = $self->local_path($path);
        mkpath dirname $dst;
        while (!$self->rsync->exec
               (
                src => join("/",
                            $self->remoteroot,
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

sub _my_current_rfile {
    my($self) = @_;
    my $rfile;
    if ($self->_use_tempfile) {
        $rfile = $self->_current_tempfile;
    } else {
        $rfile = $self->rfile;
    }
    return $rfile;
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

=head2 $ret = $obj->read_recent_1 ( $data )

Delegate of C<recent_events()> on protocol 1

=cut

sub read_recent_1 {
    my($self, $data) = @_;
    return $data->{recent};
}

=head2 $array_ref = $obj->recent_events ( %options )

Note: the code relies on the resource being written atomically. We
cannot lock because we may have no write access. If the caller has
write access (eg. aggregate() or update()), it has to care for any
necessary locking and it MUST write atomically.

If $options{after} is specified, only file events after this timestamp
are returned.

If $options{before} is specified, only file events before this
timestamp are returned.

IF $options{'skip-deletes'} is specified, no files-to-be-deleted will
be returned.

If $options{max} is specified only a maximum of this many events is
returned.

If $options{contains} is specified the value must be a hash reference
containing a query. The query may contain the keys C<epoch>, C<path>,
and C<type>. Each represents a condition that must be met. If there is
more than one such key, the conditions are ANDed.

If $options{info} is specified, it must be a hashref. This hashref
will be filled with metadata about the unfiltered recent_events of
this object, in key C<first> there is the first item, in key C<last>
is the last.

=cut

sub recent_events {
    my ($self, %options) = @_;
    my $info = $options{info};
    if ($self->is_slave) {
        $self->get_remote_recentfile_as_tempfile;
    }
    my $rfile_or_tempfile = $self->_my_current_rfile or return [];
    -e $rfile_or_tempfile or return [];
    my $suffix = $self->serializer_suffix;
    my ($data) = eval {
        $self->_try_deserialize
            (
             $suffix,
             $rfile_or_tempfile,
            );
    };
    my $err = $@;
    if ($err or !$data) {
        return [];
    }
    my $re;
    if (reftype $data eq 'ARRAY') { # protocol 0
        $re = $data;
    } else {
        $re = $self->_recent_events_protocol_x
            (
             $data,
             $rfile_or_tempfile,
            );
    }
    return $re unless grep {defined $options{$_}} qw(after before max);
    $self->_recent_events_handle_options ($re, \%options);
}

sub _recent_events_handle_options {
    my($self, $re, $options) = @_;
    my $last_item = $#$re;
    my $info = $options->{info};
    if ($info) {
        $info->{first} = $re->[0];
        $info->{last} = $re->[-1];
    }
    if (defined $options->{after}) {
        if ($re->[0]{epoch} > $options->{after}) {
            if (
                my $f = first
                        {$re->[$_]{epoch} <= $options->{after}}
                        0..$#$re
               ) {
                $last_item = $f-1;
            }
        } else {
            $last_item = -1;
        }
    }
    my $first_item = 0;
    if (defined $options->{before}) {
        if ($re->[0]{epoch} > $options->{before}) {
            if (
                my $f = first
                        {$re->[$_]{epoch} < $options->{before}}
                        0..$last_item
               ) {
                $first_item = $f;
            }
        } else {
            $first_item = 0;
        }
    }
    if (0 != $first_item || -1 != $last_item) {
        @$re = splice @$re, $first_item, 1+$last_item-$first_item;
    }
    if ($options->{'skip-deletes'}) {
        @$re = grep { $_->{type} ne "delete" } @$re;
    }
    if (my $contopt = $options->{contains}) {
        my $seen_allowed = 0;
        for my $allow (qw(epoch path type)) {
            if (exists $contopt->{$allow}) {
                $seen_allowed++;
                my $v = $contopt->{$allow};
                @$re = grep { $_->{$allow} eq $v } @$re;
            }
        }
        if (keys %$contopt > $seen_allowed) {
            require Carp;
            Carp::confess
                    (sprintf "unknown query: %s", join ", ", %$contopt);
        }
    }
    if ($options->{max} && @$re > $options->{max}) {
        @$re = splice @$re, 0, $options->{max};
    }
    $re;
}

sub _recent_events_protocol_x {
    my($self,
       $data,
       $rfile_or_tempfile,
      ) = @_;
    my $meth = sprintf "read_recent_%d", $data->{meta}{protocol};
    # we may be reading meta for the first time
    while (my($k,$v) = each %{$data->{meta}}) {
        next if $k ne lc $k; # "Producers"
        next if defined $self->$k;
        $self->$k($v);
    }
    my $re = $self->$meth ($data);
    my @stat = stat $rfile_or_tempfile or die "Cannot stat '$rfile_or_tempfile': $!";
    my $minmax = { mtime => $stat[9] };
    if (@$re) {
        $minmax->{min} = $re->[-1]{epoch};
        $minmax->{max} = $re->[0]{epoch};
    }
    $self->minmax ( $minmax );
    return $re;
}

sub _try_deserialize {
    my($self,
       $suffix,
       $rfile_or_tempfile,
      ) = @_;
    if ($suffix eq ".yaml") {
        require YAML::Syck;
        YAML::Syck::LoadFile($rfile_or_tempfile);
    } elsif ($HAVE->{"Data::Serializer"}) {
        my $serializer = Data::Serializer->new
            ( serializer => $serializers{$suffix} );
        my $serialized = do
            {
                open my $fh, $rfile_or_tempfile or die "Could not open: $!";
                local $/;
                <$fh>;
            };
        $serializer->raw_deserialize($serialized);
    } else {
        die "Data::Serializer not installed, cannot proceed with suffix '$suffix'";
    }
}

sub _refresh_internals {
    my($self, $dst) = @_;
    my $class = ref $self;
    my $rfpeek = $class->new_from_file ($dst);
    for my $acc (qw(
                    _merged
                    minmax
                   )) {
        $self->$acc ( $rfpeek->$acc );
    }
    my $old_dirtymark = $self->dirtymark;
    my $new_dirtymark = $rfpeek->dirtymark;
    if ($old_dirtymark && $new_dirtymark && _bigfloatgt($new_dirtymark,$old_dirtymark)) {
        $self->done->reset;
        $self->dirtymark ( $new_dirtymark );
        $self->seed;
    }
}

=head2 $ret = $obj->rfilename

Just the basename of our I<recentfile>, composed from C<filenameroot>,
a dash, C<interval>, and C<serializer_suffix>. E.g. C<RECENT-6h.yaml>

=cut

sub rfilename {
    my($self) = @_;
    my $file = sprintf("%s-%s%s",
                       $self->filenameroot,
                       $self->interval,
                       $self->serializer_suffix,
                      );
    return $file;
}

=head2 $str = $self->remote_dir

The directory we are mirroring from.

=cut

sub remote_dir {
    my($self, $set) = @_;
    if (defined $set) {
        $self->_remote_dir ($set);
    }
    my $x = $self->_remote_dir;
    $self->is_slave (1);
    return $x;
}

=head2 $str = $obj->remoteroot

=head2 (void) $obj->remoteroot ( $set )

Get/Set the composed prefix needed when rsyncing from a remote module.
If remote_host, remote_module, and remote_dir are set, it is composed
from these.

=cut

sub remoteroot {
    my($self, $set) = @_;
    if (defined $set) {
        $self->_remoteroot($set);
    }
    my $remoteroot = $self->_remoteroot;
    unless (defined $remoteroot) {
        $remoteroot = sprintf
            (
             "%s%s%s",
             defined $self->remote_host   ? ($self->remote_host."::")  : "",
             defined $self->remote_module ? ($self->remote_module."/") : "",
             defined $self->remote_dir    ? $self->remote_dir          : "",
            );
        $self->_remoteroot($remoteroot);
    }
    return $remoteroot;
}

=head2 (void) $obj->resolve_recentfilename ( $recentfilename )

Inverse method to L<rfilename>. $recentfilename is a plain filename of
the pattern

    $filenameroot-$interval$serializer_suffix

e.g.

    RECENT-1M.yaml

This filename is split into its parts and the parts are fed to the
object itself.

=cut

sub resolve_recentfilename {
    my($self, $rfname) = @_;
    my($splitter) = qr(^(.+)-([^-\.]+)(\.[^\.]+));
    if (my($f,$i,$s) = $rfname =~ $splitter) {
        $self->filenameroot      ($f);
        $self->interval          ($i);
        $self->serializer_suffix ($s);
    } else {
        die "Alert: cannot split '$rfname', doesn't match '$splitter'";
    }
    return;
}

=head2 my $rfile = $obj->rfile

Returns the full path of the I<recentfile>

=cut

sub rfile {
    my($self) = @_;
    my $rfile = $self->_rfile;
    return $rfile if defined $rfile;
    $rfile = File::Spec->catfile
        ($self->localroot,
         $self->rfilename,
        );
    $self->_rfile ($rfile);
    return $rfile;
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
        if ($HAVE->{"File::Rsync"}) {
            $rsync = File::Rsync->new($rsync_options);
            $self->_rsync($rsync);
        } else {
            die "File::Rsync required for rsync operations. Cannot continue";
        }
    }
    return $rsync;
}

=head2 (void) $obj->register_rsync_error($err)

=head2 (void) $obj->un_register_rsync_error()

Register_rsync_error is called whenever the File::Rsync object fails
on an exec (say, connection doesn't succeed). It issues a warning and
sleeps for an increasing amount of time. Un_register_rsync_error
resets the error count. See also accessor C<max_rsync_errors>.

=cut

{
    my $no_success_count = 0;
    my $no_success_time = 0;
    sub register_rsync_error {
        my($self, $err) = @_;
        chomp $err;
        $no_success_time = time;
        $no_success_count++;
        my $max_rsync_errors = $self->max_rsync_errors;
        $max_rsync_errors = MAX_INT unless defined $max_rsync_errors;
        if ($max_rsync_errors>=0 && $no_success_count >= $max_rsync_errors) {
            require Carp;
            Carp::confess
                  (
                   sprintf
                   (
                    "Alert: Error while rsyncing (%s): '%s', error count: %d, exiting now,",
                    $self->interval,
                    $err,
                    $no_success_count,
                   ));
        }
        my $sleep = 12 * $no_success_count;
        $sleep = 300 if $sleep > 300;
        require Carp;
        Carp::cluck
              (sprintf
               (
                "Warning: %s, Error while rsyncing (%s): '%s', sleeping %d",
                scalar(localtime($no_success_time)),
                $self->interval,
                $err,
                $sleep,
               ));
        sleep $sleep
    }
    sub un_register_rsync_error {
        my($self) = @_;
        $no_success_time = 0;
        $no_success_count = 0;
    }
}

=head2 $clone = $obj->_sparse_clone

Clones just as much from itself that it does not hurt. Experimental
method.

Note: what fits better: sparse or shallow? Other suggestions?

=cut

sub _sparse_clone {
    my($self) = @_;
    my $new = bless {}, ref $self;
    for my $m (qw(
                  _interval
                  _localroot
                  _remoteroot
                  _rfile
                  _use_tempfile
                  aggregator
                  dirtymark
                  filenameroot
                  is_slave
                  max_files_per_connection
                  protocol
                  rsync_options
                  serializer_suffix
                  sleep_per_connection
                  verbose
                 )) {
        my $o = $self->$m;
        $o = Storable::dclone $o if ref $o;
        $new->$m($o);
    }
    $new;
}

=head2 $boolean = OBJ->ttl_reached ()

=cut

sub ttl_reached {
    my($self) = @_;
    my $have_mirrored = $self->have_mirrored || 0;
    my $now = Time::HiRes::time;
    my $ttl = $self->ttl;
    $ttl = 24.2 unless defined $ttl;
    if ($now > $have_mirrored + $ttl) {
        return 1;
    }
    return 0;
}

=head2 (void) $obj->unlock()

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

=head2 unseed

Sets this recentfile in the state of not 'seeded'.

=cut
sub unseed {
    my($self) = @_;
    $self->seeded(0);
}

=head2 $ret = $obj->update ($path, $type)

Enter one file into the local I<recentfile>. $path is the (usually
absolute) path. If the path is outside the I<our> tree, then it is
ignored.

$type is one of C<new> or C<delete>.

The new file event is uhshifted to the array of recent_events and the
array is shortened to the length of the timespan allowed. This is
usually the timespan specified by the interval of this recentfile but
as long as this recentfile has not been merged to another one, the
timespan may grow without bounds.

=cut
sub _epoch_monotonically_increasing {
    my($self,$epoch,$recent) = @_;
    return $epoch unless @$recent; # the first one goes unoffended
    if (_bigfloatgt("".$epoch,$recent->[0]{epoch})) {
        return $epoch;
    } else {
        return _increase_a_bit($recent->[0]{epoch});
    }
}
sub update {
    my($self,$path,$type) = @_;
    die "update called without path argument" unless defined $path;
    die "update called without type argument" unless defined $type;
    die "update called with illegal type argument: $type" unless $type =~ /(new|delete)/;
    my $canonmeth = $self->canonize;
    unless ($canonmeth) {
        $canonmeth = "naive_path_normalize";
    }
    $path = $self->$canonmeth($path);
    my $lrd = $self->localroot;
    if ($path =~ s|^\Q$lrd\E||) {
        $path =~ s|^/||;
        my $interval = $self->interval;
        my $secs = $self->interval_secs();
        $self->lock;
        # you must calculate the time after having locked, of course
        my $epoch = Time::HiRes::time;
        my $recent = $self->recent_events;
        $epoch = $self->_epoch_monotonically_increasing($epoch,$recent);
        $recent ||= [];
        my $oldest_allowed = 0;
        if (my $merged = $self->merged) {
            $oldest_allowed = min($epoch - $secs, $merged->{epoch});
        } else {
            # as long as we are not merged at all, no limits!
        }
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
        $self->_assert_symlink;
        $self->unlock;
    }
}

=head2 seed

Sets this recentfile in the state of 'seeded' which means it has to
re-evaluate its uptodateness.

=cut
sub seed {
    my($self) = @_;
    $self->seeded(1);
}

=head2 seeded

Tells if the recentfile is in the state 'seeded'.

=cut
sub seeded {
    my($self, $set) = @_;
    if (defined $set) {
        $self->_seeded ($set);
    }
    my $x = $self->_seeded;
    unless (defined $x) {
        $x = 0;
        $self->_seeded ($x);
    }
    return $x;
}

=head2 uptodate

True if this object has mirrored the complete interval covered by the
current recentfile.

*** WIP ***

=cut
sub uptodate {
    my($self) = @_;
    my $uptodate;
    my $why;
    if ($self->_uptodateness_ever_reached and not $self->seeded) {
        $why = "saturated";
        $uptodate = 1;
    }
    unless (defined $uptodate) {
        if ($self->ttl_reached){
            $why = "ttl_reached returned true, so we are not uptodate";
            $uptodate = 0 ;
        }
    }
    unless (defined $uptodate) {
        # look if recentfile has unchanged timestamp
        my $minmax = $self->minmax;
        if (exists $minmax->{mtime}) {
            my $rfile = $self->_my_current_rfile;
            my @stat = stat $rfile;
            my $mtime = $stat[9];
            if ($mtime > $minmax->{mtime}) {
                $why = "mtime[$mtime] of rfile[$rfile] > minmax/mtime[$minmax->{mtime}], so we are not uptodate";
                $uptodate = 0;
            } else {
                my $covered = $self->done->covered(@$minmax{qw(max min)});
                $why = "minmax covered[$covered], so we return that";
                $uptodate = $covered;
            }
        }
    }
    unless (defined $uptodate) {
        $why = "fallthrough, so not uptodate";
        $uptodate = 0;
    }
    if ($uptodate) {
        $self->_uptodateness_ever_reached(1);
        $self->unseed;
    }
    my $remember =
        {
         uptodate => $uptodate,
         why => $why,
        };
    $self->_remember_last_uptodate_call($remember);
    return $uptodate;
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
    my $suffix = $self->serializer_suffix;
    my $data = {
                meta => $self->meta_data,
                recent => $recent,
               };
    my $serialized;
    if ($suffix eq ".yaml") {
        $serialized = YAML::Syck::Dump($data);
    } elsif ($HAVE->{"Data::Serializer"}) {
        my $serializer = Data::Serializer->new
            ( serializer => $serializers{$suffix} );
        $serialized = $serializer->raw_serialize($data);
    } else {
        die "Data::Serializer not installed, cannot proceed with suffix '$suffix'";
    }
    open my $fh, ">", "$rfile.new" or die "Could not open >'$rfile.new': $!";
    print $fh $serialized;
    close $fh or die "Could not close '$rfile.new': $!";
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
reflected in the shortest file, it can complement the list of recent
file events with the next one. And if this is not long enough we want
another one, again a bit longer. And we want one that completes the
history back to the oldest file. The index files do contain the
complete list of current files. The larger an index file is the less
often it is updated. For practical reasons adjacent files will often
overlap a bit but this is neither necessary nor enforced. That's the
basic idea. The following example represents a tree that has a few
updates every day:

 RECENT.recent -> RECENT-1h.yaml
 RECENT-6h.yaml
 RECENT-1d.yaml
 RECENT-1M.yaml
 RECENT-1W.yaml
 RECENT-1Q.yaml
 RECENT-1Y.yaml
 RECENT-Z.yaml

The first file is the principal file, in so far it is the one that is
written first after a filesystem change. Usually a symlink links to it
with a filename that has the same filenameroot and the suffix
C<.recent>. On systems that do not support symlinks there is a plain
copy maintained instead.

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

 rsnapshot           www.rsnapshot.org                       focus on backup
 csync               www.csync.org                           more like unison
 multi-rsync         sourceforge 167893                      lan push to many

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

=head1 SEE ALSO

Barbie is providing a database of release dates. See
http://use.perl.org/~barbie/journal/37907

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

Copyright 2008 Andreas König.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of File::Rsync::Mirror::Recentfile

# Local Variables:
# mode: cperl
# cperl-indent-level: 4
# End:
