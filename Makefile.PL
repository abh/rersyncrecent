use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'File::Rsync::Mirror::Recentfile',
    AUTHOR              => 'Andreas König <andreas.koenig.7os6VVqR@franz.ak.mind.de>',
    VERSION_FROM        => 'lib/File/Rsync/Mirror/Recentfile.pm',
    ABSTRACT_FROM       => 'lib/File/Rsync/Mirror/Recentfile.pm',
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'File-Rsync-Mirror-Recentfile-*' },
);
