####################################################################################################################################
# ARCHIVE PUSH ASYNC MODULE
####################################################################################################################################
package pgBackRest::Archive::ArchivePushAsync;
use parent 'pgBackRest::Archive::ArchivePush';

use strict;
use warnings FATAL => qw(all);
use Carp qw(confess);
use English '-no_match_vars';

use Exporter qw(import);
    our @EXPORT = qw();
use Fcntl qw(SEEK_CUR O_RDONLY O_WRONLY O_CREAT);
use File::Basename qw(dirname basename);
use IO::Socket::UNIX;
use POSIX qw(setsid);
use Scalar::Util qw(blessed);

use pgBackRest::Common::Exception;
use pgBackRest::Common::Lock;
use pgBackRest::Common::Log;
use pgBackRest::Archive::ArchiveCommon;
use pgBackRest::Archive::ArchiveInfo;
use pgBackRest::Common::String;
use pgBackRest::Common::Wait;
use pgBackRest::Config::Config;
use pgBackRest::Db;
use pgBackRest::DbVersion;
use pgBackRest::File;
use pgBackRest::FileCommon;
use pgBackRest::Protocol::ArchivePushMaster;
use pgBackRest::Protocol::ArchivePushMinion;
use pgBackRest::Protocol::Common;
use pgBackRest::Protocol::LocalProcess;
use pgBackRest::Protocol::Protocol;
use pgBackRest::Version;

####################################################################################################################################
# constructor
####################################################################################################################################
sub new
{
    my $class = shift;          # Class name

    # Init object
    my $self = $class->SUPER::new();
    bless $self, $class;

    # Assign function parameters, defaults, and log debug info
    (
        my $strOperation,
        $self->{strWalPath},
        $self->{strSocketFile},
        $self->{strBackRestBin},
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->new', \@_,
            {name => 'strWalPath'},
            {name => 'strSocketFile'},
            {name => 'strBackRestBin', default => BACKREST_BIN},
        );

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'self', value => $self}
    );
}

####################################################################################################################################
# process
####################################################################################################################################
sub process
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my ($strOperation) = logDebugParam(__PACKAGE__ . '->process');

    # If logging locally then create the stop archiving file name
    # my $strStopFile;
    #
    # if ($bArchiveAsync)
    # {
    #     $strStopFile = optionGet(OPTION_SPOOL_PATH) . '/stop/' . optionGet(OPTION_STANZA) . "-archive.stop";
    # }

    # If the stop file exists then discard the archive log
    # if ($bArchiveAsync)
    # {
    #     if (-e $strStopFile)
    #     {
    #         &log(ERROR, "discarding " . basename($ARGV[1]) .
    #                     " due to the archive store max size exceeded" .
    #                     " - remove the archive stop file (${strStopFile}) to resume archiving" .
    #                     " and be sure to take a new backup as soon as possible");
    #         return 0;
    #     }
    # }

    my $bClient = true;

    if (lockAcquire(commandGet(), false))
    {
        # Remove the old socket file
        fileRemove($self->{strSocketFile}, true);
        $bClient = fork() == 0 ? false : true;
    }
    else
    {
        logDebugMisc($strOperation, 'async archive-push process is already running');
    }

    if ($bClient)
    {
        $self->processClient();
    }
    else
    {
        chdir '/'
            or confess "chdir() failed: $!";

        # close stdin/stdout/stderr
        open STDIN, '<', '/dev/null'
            or confess "Couldn't close stdin: $!";
        open STDOUT, '>', '/dev/null'
            or confess "Couldn't close stdout: $!";
        open STDERR, '>', '/dev/null'
            or confess "Couldn't close stderr: $!";

        # create new session group
        setsid() or confess("setsid() failed: $!");

        # Open the log file
        logFileSet(optionGet(OPTION_LOG_PATH) . '/' . optionGet(OPTION_STANZA) . '-archive-async');

        $self->processServer();
    }

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'iResult', value => 0, trace => true}
    );
}

####################################################################################################################################
# initServer
####################################################################################################################################
sub initServer
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my ($strOperation) = logDebugParam(__PACKAGE__ . '->initServer');

    # Initialize the backup process
    $self->{oArchiveProcess} = new pgBackRest::Protocol::LocalProcess(BACKUP, 0, $self->{strBackRestBin});
    $self->{oArchiveProcess}->hostAdd(1, optionGet(OPTION_PROCESS_MAX));

    # Initialize the socket
    $self->{oServerSocket} = logErrorResult(
        IO::Socket::UNIX->new(Type => SOCK_STREAM, Local => $self->{strSocketFile}, Listen => 1), ERROR_ARCHIVE_TIMEOUT,
        'unable to initialize ' . CMD_ARCHIVE_PUSH . " async process on socket: $self->{strSocketFile}");

    # Return from function and log return values if any
    return logDebugReturn($strOperation);
}

####################################################################################################################################
# processServer
####################################################################################################################################
sub processServer
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my ($strOperation) = logDebugParam(__PACKAGE__ . '->processServer');

    $self->initServer();

    # my $oSocket = $oServer->accept();

    # $self->processServer(new pgBackRest::Protocol::ArchivePushMinion($oSocket));
    # $oSocket->close();
    # $oServer->close();
    # fileRemove($self->{strSocketFile}, true);

    # Return from function and log return values if any
    return logDebugReturn($strOperation);
}

####################################################################################################################################
# processClient
####################################################################################################################################
sub processClient
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my ($strOperation) = logDebugParam(__PACKAGE__ . '->processClient');

    # Wait for the socket file to appear
    my $iWaitSeconds = 10;
    my $oWait = waitInit($iWaitSeconds);
    my $bExists = false;

    do
    {
        $bExists = fileExists($self->{strSocketFile});
    }
    while (!$bExists && waitMore($oWait));

    if (!$bExists)
    {
        confess &log(ERROR, "unable to find socket after ${iWaitSeconds} second(s)", ERROR_ARCHIVE_TIMEOUT);
    }

    $self->{oClientSocket} = logErrorResult(
        IO::Socket::UNIX->new(Type => SOCK_STREAM, Peer => $self->{strSocketFile}), ERROR_ARCHIVE_TIMEOUT,
        'unable to connect to ' . CMD_ARCHIVE_PUSH . " async process socket: $self->{strSocketFile}");

    # $self->processClient(new pgBackRest::Protocol::ArchivePushMaster($oSocket));

    # my $strWalSegment = $oMaster->cmdExecute(OP_ARCHIVE_PUSH_ASYNC, ['000000010000000100000001'], true);
    # &log(WARN, "I AM CONNECTED: " . $strWalSegment);

    # Return from function and log return values if any
    return logDebugReturn($strOperation);
}

####################################################################################################################################
# processQueue
####################################################################################################################################
sub processQueue
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my ($strOperation) = logDebugParam(__PACKAGE__ . '->processQueue');

    # If no jobs are bing processed then get more jobs
    my $stryWalFile = [];

    if (!$self->{oArchiveProcess}->processing())
    {
        $stryWalFile = $self->readyList();

        foreach my $strWalFile (@{$stryWalFile})
        {
            $self->{oArchiveProcess}->queueJob(
                1, 'default', $strWalFile, OP_ARCHIVE_PUSH_FILE,
                [$self->{strWalPath}, $strWalFile, optionGet(OPTION_COMPRESS), optionGet(OPTION_REPO_SYNC)]);
        }
    }

    # Process jobs if there are any
    if ($self->{oArchiveProcess}->jobTotal() > 0)
    {
        if (my $hyJob = $self->{oArchiveProcess}->process())
        {
            foreach my $hJob (@{$hyJob})
            {
                $self->{hWalState}{@{$hJob->{rParam}}[1]} = true;
            }
        }
    }

    # If there are no jobs left then process one more time to properly reset the queue
    if ($self->{oArchiveProcess}->jobTotal() == 0)
    {
        $self->{oArchiveProcess}->process();
    }

    return logDebugReturn
    (
        $strOperation,
        {name => 'iNewTotal', value => scalar(@{$stryWalFile})},
        {name => 'iQueueTotal', value => $self->{oArchiveProcess}->jobTotal()}
    );
}

####################################################################################################################################
# readyList
####################################################################################################################################
sub readyList
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my ($strOperation) = logDebugParam(__PACKAGE__ . '->processQueue');

    # Read the ready files
    my $strWalStatusPath = "$self->{strWalPath}/archive_status";
    my @stryReadyFile = fileList($strWalStatusPath, '^.*\.ready$');

    # Generate a list of new files
    my @stryNewReadyFile;
    my $hReadyFile = {};

    foreach my $strReadyFile (@stryReadyFile)
    {
        # Remove .ready extension
        $strReadyFile = substr($strReadyFile, 0, length($strReadyFile) - length('.ready'));

        # Add the file if it is not already in the hash
        if (!defined($self->{hWalState}{$strReadyFile}))
        {
            # Set the file as not pushed
            $self->{hWalState}{$strReadyFile} = false;

            # Push onto list of new files
            push(@stryNewReadyFile, $strReadyFile);
        }

        # Add to the ready hash for speed finding removed files
        $hReadyFile->{$strReadyFile} = true;
    }

    # Remove files that are no longer in ready state
    foreach my $strReadyFile (sort(keys(%{$self->{hWalState}})))
    {
        if (!defined($hReadyFile->{$strReadyFile}))
        {
            delete($self->{hWalState}{$strReadyFile});
        }
    }

    return logDebugReturn
    (
        $strOperation,
        {name => 'stryWalFile', value => \@stryNewReadyFile, ref => true}
    );
}

1;
