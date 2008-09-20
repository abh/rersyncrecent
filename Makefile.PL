use strict;
use warnings;
use ExtUtils::MakeMaker;
$ExtUtils::MakeMaker::VERSION =~ s/_//g;
my @sign = (MM->can("signature_target") ? (SIGN => 1) : ());
my $version_diff = 0; # we'll have to die if this becomes true
my $version_from = 'lib/File/Rsync/Mirror/Recent.pm';
my $version;
{
    open my $fh, "<", $version_from or die "Could not open $version_from: $!";
    my @version = grep /VERSION\s*=\s*qv/, <$fh>;
    our $VERSION;
    eval $version[0];
    $version = $VERSION;
}
if ($ARGV[0] && $ARGV[0] eq "--setversion") {
    die "Your perl is a bit dated[$]].\nDo not make a release with it\n" if $] < 5.008;
    warn "Your MakeMaker is a bit dated[$ExtUtils::MakeMaker::VERSION].\nYou should get a new one\n"
        if $ExtUtils::MakeMaker::VERSION < 6.4502;
    eval {  require $version_from; }; die $@ if $@;
    my $makefile = "Makefile";
    unless (-f $makefile) {
        $makefile = "Makefile.old";
    }
    unless (-f $makefile) {
        die "no Makefile and no Makefile.old found";
    }
    open my $fh, "make the-release-name|" or die "Could not fork: $!";
    local $/;
    my ($have_version) = <$fh> =~ /^version\s+([\d\._]+)/m;
    close $fh or die "Could not close: $!";
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
     NAME                => 'File::Rsync::Mirror::Recent',
     VERSION             => $version,
     AUTHOR              => 'Andreas König <andreas.koenig.7os6VVqR@franz.ak.mind.de>',
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
     EXE_FILES    => [qw(bin/rrr-aggregate bin/rrr-news)],
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
     ($ExtUtils::MakeMaker::VERSION >= 6.4502 ?
      (META_ADD => {
                    # write: git push git+ssh://repo.or.cz/srv/git/rersyncrecent.git master
                    repository => "git://repo.or.cz/rersyncrecent.git",
                    provides => {
                                 "File::Rsync::Mirror::Recent" =>
                                 {
                                  version => $version,
                                  file => $version_from,
                                 }
                                },
                   }) : ()),
     @sign,
     clean               => { FILES => 'File-Rsync-Mirror-Recent-*' },
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
META.yml: metafile
	$(CP) $(DISTVNAME)/META.yml  ./META.yml

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
