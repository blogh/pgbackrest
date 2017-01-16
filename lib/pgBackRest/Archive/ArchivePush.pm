####################################################################################################################################
# ARCHIVE PUSH MODULE
####################################################################################################################################
package pgBackRest::Archive::ArchivePush;
use parent 'pgBackRest::Archive::Archive';

use strict;
use warnings FATAL => qw(all);
use Carp qw(confess);
use English '-no_match_vars';

use Exporter qw(import);
    our @EXPORT = qw();
use File::Basename qw(dirname basename);

use pgBackRest::Common::Exception;
use pgBackRest::Common::Lock;
use pgBackRest::Common::Log;
use pgBackRest::Archive::ArchiveCommon;
use pgBackRest::Archive::ArchiveInfo;
use pgBackRest::Common::Wait;
use pgBackRest::Config::Config;
use pgBackRest::DbVersion;
use pgBackRest::File;
use pgBackRest::FileCommon;
use pgBackRest::Protocol::Common;
use pgBackRest::Protocol::Protocol;

####################################################################################################################################
# process
####################################################################################################################################
sub process
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my ($strOperation) = logDebugParam(__PACKAGE__ . '->process');

    # Make sure the archive push command happens on the db side
    if (!isDbLocal())
    {
        confess &log(ERROR, CMD_ARCHIVE_PUSH . ' operation must run on the db host');
    }

    # Error if no WAL segment is defined
    my $strWalSegment = $ARGV[1];

    if (!defined($strWalSegment))
    {
        confess &log(ERROR, 'wal segment to push required', ERROR_PARAM_REQUIRED);
    }

    my $strWalFile = basename($strWalSegment);

    # Check for a stop lock
    lockStopTest();

    # Only do async archiving when the file being archived is a WAL segment, otherwise do it synchronously.  In the async code it
    # would be very hard to decide when it is appropriate to archive timeline, backup, and partial files.
    my $bArchiveAsync = optionGet(OPTION_ARCHIVE_ASYNC) && $strWalFile =~ '^[0-F]{24}$';

    # Start the async process and wait for WAL to complete
    if ($bArchiveAsync)
    {
        &log(INFO, "push WAL segment ${strWalFile} asynchronously");

        # Load module dynamically
        require pgBackRest::Archive::ArchivePushAsync;
        new pgBackRest::Archive::ArchivePushAsync(dirname($strWalSegment), $strWalFile)->process();
    }
    # Else push synchronously
    else
    {
        $self->push($strWalSegment);
    }

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'iResult', value => 0, trace => true}
    );
}

####################################################################################################################################
# push
####################################################################################################################################
sub push
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $strSourceFile,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->push', \@_,
            {name => 'strSourceFile'},
        );

    # Create the file object
    my $oFile = new pgBackRest::File
    (
        optionGet(OPTION_STANZA),
        optionGet(OPTION_REPO_PATH),
        protocolGet(BACKUP)
    );

    # Construct absolute path to the WAL file when it is relative
    $strSourceFile = walPath($strSourceFile, optionGet(OPTION_DB_PATH, false), commandGet());

    # Get the destination file
    my $strDestinationFile = basename($strSourceFile);

    # Get the compress flag
    my $bCompress = optionGet(OPTION_COMPRESS);

    # Determine if this is an archive file (don't do compression or checksum on .backup, .history, etc.)
    my $bArchiveFile = basename($strSourceFile) =~ /^[0-F]{24}(\.partial){0,1}$/ ? true : false;

    # Determine if this is a partial archive file
    my $bPartial = $bArchiveFile && basename($strSourceFile) =~ /\.partial/ ? true : false;

    # Check that there are no issues with pushing this WAL segment
    my $strArchiveId;
    my $strChecksum = undef;

    if ($bArchiveFile)
    {
        my ($strDbVersion, $ullDbSysId) = $self->walInfo($strSourceFile);
        ($strArchiveId, $strChecksum) = $self->pushCheck(
            $oFile, substr(basename($strSourceFile), 0, 24), $bPartial, $strSourceFile, $strDbVersion, $ullDbSysId);
    }
    else
    {
        $strArchiveId = $self->getCheck($oFile);
    }

    # Only copy the WAL segment if checksum is not defined.  If checksum is defined it means that the WAL segment already exists
    # in the repository with the same checksum (else there would have been an error on checksum mismatch).
    if (!defined($strChecksum))
    {
        # Append compression extension
        if ($bArchiveFile && $bCompress)
        {
            $strDestinationFile .= '.' . $oFile->{strCompressExtension};
        }

        # Copy the WAL segment
        $oFile->copy(
            PATH_DB_ABSOLUTE, $strSourceFile,                       # Source type/file
            PATH_BACKUP_ARCHIVE,                                    # Destination type
            "${strArchiveId}/${strDestinationFile}",                # Destination file
            false,                                                  # Source is not compressed
            $bArchiveFile && $bCompress,                            # Destination compress is configurable
            undef, undef, undef,                                    # Unused params
            true,                                                   # Create path if it does not exist
            undef, undef,                                           # Default User and group
            $bArchiveFile,                                          # Append checksum if archive file
            optionGet(OPTION_REPO_SYNC));                           # Sync repo directories?
    }

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation
    );
}

1;
