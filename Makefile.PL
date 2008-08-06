use strict;
use warnings;
use ExtUtils::MakeMaker;

my @sign = (MM->can("signature_target") ? (SIGN => 1) : ());
my $version_diff = 0; # we'll have to die if this becomes true
my $version_from = 'lib/File/Rsync/Mirror/Recentfile.pm';
if ($ARGV[0] && $ARGV[0] eq "--setversion") {
    eval {  require $version_from; };
    open my $fh, "make the-release-name|" or die;
    local $/;
    my ($have_version) = <$fh> =~ /^version\s+([\d\._]+)/m;
    my ($namespace) = $version_from;
    $namespace =~ s|^lib/(.+)\.pm$|$1|;
    $namespace =~ s|/|::|g;
    eval qq{ if (\$$namespace\::VERSION != \$have_version) {
        warn "Not equal: CPAN::Testers::ParseReport::VERSION[\$$namespace\::VERSION] Makefile version[\$have_version]";
        \$version_diff = 1;
    }};
}

WriteMakefile
    (
     NAME                => 'File::Rsync::Mirror::Recentfile',
     AUTHOR              => 'Andreas König <andreas.koenig.7os6VVqR@franz.ak.mind.de>',
     VERSION_FROM        => $version_from,
     PL_FILES            => {},
     PREREQ_PM =>
     {
      # 'Data::Serializer' => 0.46, # restricted to YAML without it
      'File::Rsync'      => 0,
      'Test::More'       => 0,
      'File::Basename'   => 0,
      'File::Path'       => 0,
      'File::Temp'       => 0,
      'Scalar::Util'     => 0,
      'Time::HiRes'      => 0,
      'YAML::Syck'       => 0,
     },
     EXE_FILES    => [qw(bin/rrr-aggregate)],
     dist => {
              DIST_DEFAULT => join(" ", # note: order matters!
                                   "Makefile",
                                   "META.yml",
                                   "setversion",
                                   "all",
                                   "tardist",
                                  ),
              COMPRESS => 'gzip -9',
             },
     @sign,
     clean               => { FILES => 'File-Rsync-Mirror-Recentfile-*' },
    );

if ($version_diff){
  die "
==> I had to update some \$VERSIONs <==
==> Your Makefile has been rebuilt. <==
==> Please rerun the make command.  <==
";
}

package MY;
sub postamble {
  my @m;
  push @m, q{
setversion:
	$(PERL) Makefile.PL --setversion

the-release-name :
	$(NOECHO) $(ECHO) 'version ' $(VERSION)
	$(NOECHO) $(ECHO) 'release-name ' $(DISTVNAME).tar$(SUFFIX)

release ::
	/usr/bin/perl -I../CPAN/GIT/trunk/release-lib -S release $(DISTVNAME).tar$(SUFFIX)
	rm -rf $(DISTVNAME)

sign:
	cpansign -s
};
  join "", @m;
}

sub dist_test {
  return q{
# if we depend on $(DISTVNAME).tar$(SUFFIX), then the rest of the
# Makefile breaks our intent to NOT remake dist
disttest :
	rm -rf $(DISTVNAME)
	tar xvzf $(DISTVNAME).tar$(SUFFIX)
	cd $(DISTVNAME) && $(ABSPERLRUN) Makefile.PL
	cd $(DISTVNAME) && $(MAKE) $(PASTHRU)
	cd $(DISTVNAME) && $(MAKE) test $(PASTHRU)

distdir ::
	touch $(DISTVNAME)/SIGNATURE && $(CP) $(DISTVNAME)/SIGNATURE ./SIGNATURE
	$(CP) $(DISTVNAME)/META.yml  ./META.yml
	$(CP) $(DISTVNAME)/MANIFEST  ./MANIFEST

}
}

sub distdir {
  my $self = shift;
  my $out = $self->SUPER::distdir;
  $out =~ s/distdir :/distdir ::/g;
  return $out;
}

# dist_dir was the name in very old MakeMaker as of 5.005_04
sub dist_dir {
  my $self = shift;
  my $out = $self->SUPER::dist_dir;
  $out =~ s/distdir :/distdir ::/g;
  return $out;
}
