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
        $self->{strSpoolPath},
        $self->{strBackRestBin},
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->new', \@_,
            {name => 'strWalPath'},
            {name => 'strSpoolPath'},
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
    my $bRunning = false;

    if (lockAcquire(commandGet(), false))
    {
        # Remove the old socket file
        fileRemove($self->{strSocketFile}, true);
        $bClient = fork() == 0 ? false : true;
    }
    else
    {
        logDebugMisc($strOperation, 'async archive-push process is already running');
        $bRunning = true;
    }

    if (!$bClient)
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
        {name => 'bRunning', value => $bRunning, trace => true}
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

    # Create the spool path
    filePathCreate($self->{strSpoolPath}, undef, true, true);

    # Initialize the backup process
    $self->{oArchiveProcess} = new pgBackRest::Protocol::LocalProcess(BACKUP, 0, $self->{strBackRestBin}, false);
    $self->{oArchiveProcess}->hostAdd(1, optionGet(OPTION_PROCESS_MAX));

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
    my $stryWalFile = $self->readyList();

    foreach my $strWalFile (@{$stryWalFile})
    {
        $self->{oArchiveProcess}->queueJob(
            1, 'default', $strWalFile, OP_ARCHIVE_PUSH_FILE,
            [$self->{strWalPath}, $strWalFile, optionGet(OPTION_COMPRESS), optionGet(OPTION_REPO_SYNC)]);
    }

    # Process jobs if there are any
    my $iOkTotal = 0;
    my $iErrorTotal = 0;

    if ($self->{oArchiveProcess}->jobTotal() > 0)
    {
        while (my $hyJob = $self->{oArchiveProcess}->process())
        {
            foreach my $hJob (@{$hyJob})
            {
                my $strWalFile = @{$hJob->{rParam}}[1];

                # Remove from queue
                delete($self->{hWalQueue}{$strWalFile});

                # If error then write out an error file
                if (defined($hJob->{oException}))
                {
                    fileStringWrite(
                        "$self->{strSpoolPath}/${strWalFile}.error",
                        $hJob->{oException}->code() . "\n" . $hJob->{oException}->message());

                        $iErrorTotal++;
                }
                # Else write success
                else
                {
                    # Remove the error file, if any
                    fileRemove("$self->{strSpoolPath}/${strWalFile}.error", true);

                    # Write the ok file to indicate success
                    fileStringWrite("$self->{strSpoolPath}/${strWalFile}.ok");

                    $iOkTotal++;
                }
            }
        }
    }

    return logDebugReturn
    (
        $strOperation,
        {name => 'iNewTotal', value => scalar(@{$stryWalFile})},
        {name => 'iOkTotal', value => $iOkTotal},
        {name => 'iErrorTotal', value => $iErrorTotal}
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

    # Read the .ok files
    my $hOkFile = {};

    foreach my $strOkFile (fileList($self->{strSpoolPath}, '\.ok$'))
    {
        $strOkFile = substr($strOkFile, 0, length($strOkFile) - length('.ok'));
        $hOkFile->{$strOkFile} = true;
    }

    # Read the .ready files
    my $strWalStatusPath = "$self->{strWalPath}/archive_status";
    my @stryReadyFile = fileList($strWalStatusPath, '\.ready$');

    # Generate a list of new files
    my @stryNewReadyFile;
    my $hReadyFile = {};

    foreach my $strReadyFile (@stryReadyFile)
    {
        # Remove .ready extension
        $strReadyFile = substr($strReadyFile, 0, length($strReadyFile) - length('.ready'));

        # Add the file if it is not already queued or previously processed
        if (!defined($self->{hWalQueue}{$strReadyFile}) && !defined($hOkFile->{$strReadyFile}))
        {
            # Set the file as not pushed
            $self->{hWalQueue}{$strReadyFile} = true;

            # Push onto list of new files
            push(@stryNewReadyFile, $strReadyFile);
        }

        # Add to the ready hash for speed finding removed files
        $hReadyFile->{$strReadyFile} = true;
    }

    # Remove .ok files that are no longer in .ready state
    foreach my $strOkFile (sort(keys(%{$hOkFile})))
    {
        if (!defined($hReadyFile->{$strOkFile}))
        {
            fileRemove("$self->{strSpoolPath}/${strOkFile}.ok");
        }
    }

    return logDebugReturn
    (
        $strOperation,
        {name => 'stryWalFile', value => \@stryNewReadyFile, ref => true}
    );
}

1;
