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
use File::Basename qw(basename);

use pgBackRest::Archive::ArchiveCommon;
use pgBackRest::Common::Exception;
use pgBackRest::Common::Log;
use pgBackRest::Config::Config;
use pgBackRest::File;
use pgBackRest::Protocol::Common;

####################################################################################################################################
# archivePushCheck
####################################################################################################################################
sub archivePushCheck
{
    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $oFile,
        $strArchiveFile,
        $strDbVersion,
        $ullDbSysId,
        $strWalFile,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '::archivePushCheck', \@_,
            {name => 'oFile'},
            {name => 'strArchiveFile'},
            {name => 'strDbVersion'},
            {name => 'ullDbSysId'},
            {name => 'strWalFile', required => false},
        );

    # Set operation and debug strings
    my $strArchiveId;
    my $strChecksum;

    # WAL file is segment?
    my $bWalSegment = walIsSegment($strArchiveFile);

    if ($oFile->isRemote(PATH_BACKUP_ARCHIVE))
    {
        # Execute the command
        ($strArchiveId, $strChecksum) = $oFile->{oProtocol}->cmdExecute(
            OP_ARCHIVE_PUSH_CHECK, [$strArchiveFile, $strDbVersion, $ullDbSysId], true);
    }
    else
    {
        # If a segment check db version and system-id
        if ($bWalSegment)
        {
            # If the info file exists check db version and system-id else error
            $strArchiveId = (new pgBackRest::Archive::ArchiveInfo($oFile->pathGet(PATH_BACKUP_ARCHIVE)))->check(
                $strDbVersion, $ullDbSysId);

            # Check if the WAL segment already exists in the archive
            my $strFoundFile = walSegmentFind($oFile->pathGet(PATH_BACKUP_ARCHIVE, $strArchiveId), $strArchiveFile);

            if (defined($strFoundFile))
            {
                $strChecksum = substr($strFoundFile, length($strArchiveFile) + 1, 40);
            }
        }
        # Else just get the archive id
        else
        {
            $strArchiveId = (new pgBackRest::Archive::ArchiveInfo($oFile->pathGet(PATH_BACKUP_ARCHIVE)))->archiveId();
        }
    }

    if (defined($strChecksum) && !commandTest(CMD_REMOTE))
    {
        my $strChecksumNew = $oFile->hash(PATH_DB_ABSOLUTE, $strWalFile);

        if ($strChecksumNew ne $strChecksum)
        {
            confess &log(ERROR, "WAL segment " . basename($strWalFile) . " already exists in the archive", ERROR_ARCHIVE_DUPLICATE);
        }

        &log(WARN, "WAL segment " . basename($strWalFile) . " already exists in the archive with the same checksum\n" .
                   "HINT: this is valid in some recovery scenarios but may also indicate a problem.");
    }

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'strArchiveId', value => $strArchiveId},
        {name => 'strChecksum', value => $strChecksum}
    );
}

push @EXPORT, qw(archivePushCheck);

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
        $bCompress,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '::archivePushFile', \@_,
            {name => 'oFile'},
            {name => 'strWalPath'},
            {name => 'strWalFile'},
            {name => 'bCompress'},
        );

    # !!! Put some logic in here

    # Return from function and log return values if any
    return logDebugReturn($strOperation);
}

push @EXPORT, qw(archivePushFile);

1;
