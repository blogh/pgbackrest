####################################################################################################################################
# ARCHIVE PUSH FILE MODULE
####################################################################################################################################
package pgBackRest::Archive::ArchivePushFile;

use strict;
use warnings FATAL => qw(all);
use Carp qw(confess);
use English '-no_match_vars';

use Exporter qw(import);
    our @EXPORT = qw();
# use Fcntl qw(SEEK_CUR O_RDONLY);
# use File::Basename qw(dirname basename);

use pgBackRest::Common::Exception;
# use pgBackRest::Common::Lock;
use pgBackRest::Common::Log;
# use pgBackRest::Archive::ArchiveCommon;
# use pgBackRest::Archive::ArchiveInfo;
# use pgBackRest::Common::Wait;
# use pgBackRest::Config::Config;
# use pgBackRest::DbVersion;
# use pgBackRest::File;
# use pgBackRest::FileCommon;
# use pgBackRest::Protocol::Common;
# use pgBackRest::Protocol::Protocol;

####################################################################################################################################
# archivePushFile
####################################################################################################################################
sub archivePushFile
{
    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $oFile,
        $strWalPath,
        $strWalFile,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '::archivePushFile', \@_,
            {name => 'oFile'},
            {name => 'strWalPath'},
            {name => 'strWalFile'},
        );

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation
        # ,
        # {name => 'iResult', value => true, trace => true}
    );
}

push @EXPORT, qw(archivePushFile);

1;
