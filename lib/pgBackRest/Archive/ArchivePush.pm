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
        &log(INFO, "push WAL segment ${strWalFile} asynchronously");

        # Load module dynamically
        require pgBackRest::Archive::ArchivePushAsync;
        new pgBackRest::Archive::ArchivePushAsync(
            $strWalPath, optionGet(OPTION_LOCK_PATH) . '/' . optionGet(OPTION_STANZA) . '_' . commandGet() . '.socket')->process();
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

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'iResult', value => 0, trace => true}
    );
}

1;
