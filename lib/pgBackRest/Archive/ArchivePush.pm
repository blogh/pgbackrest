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
use pgBackRest::Config::Config;
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
    my $strWalPathFile = $ARGV[1];

    if (!defined($strWalPathFile))
    {
        confess &log(ERROR, 'WAL file to push required', ERROR_PARAM_REQUIRED);
    }

    # Check for a stop lock
    lockStopTest();

    # Extract WAL path and file
    my $strWalPath = dirname(walPath($strWalPathFile, optionGet(OPTION_DB_PATH, false), commandGet()));
    my $strWalFile = basename($strWalPathFile);

    # Start the async process and wait for WAL to complete
    if (optionGet(OPTION_ARCHIVE_ASYNC))
    {
        # Create the file object
        my $strSpoolPath = (new pgBackRest::File(
            optionGet(OPTION_STANZA), optionGet(OPTION_SPOOL_PATH), protocolGet(NONE)))->pathGet(PATH_BACKUP_ARCHIVE_OUT);

        # Loop to check for status files and launch async process
        my $bPushed = false;
        my $oWait = waitInit(optionGet(OPTION_ARCHIVE_TIMEOUT));

        do
        {
            # Check WAL status
            $bPushed = $self->walStatus($strSpoolPath, $strWalFile);

            # If not found then launch async process
            if (!$bPushed)
            {
                # Load module dynamically
                require pgBackRest::Archive::ArchivePushAsync;
                (new pgBackRest::Archive::ArchivePushAsync($strWalPath, $strSpoolPath))->process();
            }
        }
        while (!$bPushed && waitMore($oWait))
    }
    # Else push synchronously
    else
    {
        # Load module dynamically
        require pgBackRest::Archive::ArchivePushFile;
        pgBackRest::Archive::ArchivePushFile->import();

        # Create the file object
        my $oFile = new pgBackRest::File
        (
            optionGet(OPTION_STANZA),
            optionGet(OPTION_REPO_PATH),
            protocolGet(BACKUP)
        );

        # Push the WAL file
        archivePushFile($oFile, $strWalPath, $strWalFile, optionGet(OPTION_COMPRESS), optionGet(OPTION_REPO_SYNC));
    }

    &log(INFO, "pushed WAL segment ${strWalFile}" . (optionGet(OPTION_ARCHIVE_ASYNC) ? ' asynchronously' : ''));

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'iResult', value => 0, trace => true}
    );
}

####################################################################################################################################
# walStatus
####################################################################################################################################
sub walStatus
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $strSpoolPath,
        $strWalFile,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->walStatusCheck', \@_,
            {name => 'strSpoolPath'},
            {name => 'strWalFile'},
        );

    # Default result is false
    my $bResult = false;

    # Find matching status files
    my @stryStatusFile = fileList($strSpoolPath, '^' . $strWalFile . '\.(ok|error)$', undef, true);

    if (@stryStatusFile > 0)
    {
        # If more than one status file was found then assert - this could be a bug in the async process
        if (@stryStatusFile > 1)
        {
            confess &log(ASSERT,
                "multiple status files found in ${strSpoolPath} for ${strWalFile}: " . join(', ', @stryStatusFile));
        }

        # Read the status file
        my @stryWalStatus = split("\n", fileStringRead("${strSpoolPath}/$stryStatusFile[0]"));

        # Status file must have at least two lines if it has content
        my $iCode;
        my $strMessage;

        # Parse status content
        if (@stryWalStatus != 0)
        {
            if (@stryWalStatus < 2)
            {
                confess &log(ASSERT, "$stryStatusFile[0] content must have at least two lines:\n" . join("\n", @stryWalStatus));
            }

            $iCode = shift(@stryWalStatus);
            $strMessage = join("\n", @stryWalStatus);
        }

        # Process ok files
        if ($stryStatusFile[0] =~ /\.ok$/)
        {
            # If there is content in the status file it is a warning
            if (@stryWalStatus != 0)
            {
                # If error code is not success, then this was a renamed .error file
                if ($iCode != 0)
                {
                    $strMessage =
                        "WAL segment ${strWalFile} was not pushed due to error and was manually skipped:\n" . $strMessage;
                }

                &log(WARN, $strMessage);
            }
        }
        # Process error files
        else
        {
            # Error files must have content
            if (@stryWalStatus == 0)
            {
                confess &log(ASSERT, "$stryStatusFile[0] has no content");
            }

            # Confess the error
            confess &log(ERROR, $strMessage, $iCode);
        }

        $bResult = true;
    }

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'bResult', value => $bResult}
    );
}

1;
