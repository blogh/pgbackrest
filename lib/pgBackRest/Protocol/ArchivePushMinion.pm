####################################################################################################################################
# PROTOCOL ARCHIVE PUSH MINION MODULE
####################################################################################################################################
package pgBackRest::Protocol::ArchivePushMinion;
use parent 'pgBackRest::Protocol::SocketMinion';

use strict;
use warnings FATAL => qw(all);
use Carp qw(confess);

use pgBackRest::Common::Log;
use pgBackRest::Config::Config;
use pgBackRest::Protocol::Common;

####################################################################################################################################
# CONSTRUCTOR
####################################################################################################################################
sub new
{
    my $class = shift;                  # Class name

    # Assign function parameters, defaults, and log debug info
    my
    (
        $strOperation,
        $oSocket,
    ) =
        logDebugParam
        (
            __PACKAGE__ . '->new', \@_,
            {name => 'oSocket'},
        );

    # Init object and store variables
    my $self = $class->SUPER::new(
        CMD_ARCHIVE_PUSH, CMD_ARCHIVE_PUSH, $oSocket, optionGet(OPTION_BUFFER_SIZE), optionGet(OPTION_COMPRESS_LEVEL),
        optionGet(OPTION_COMPRESS_LEVEL_NETWORK), optionGet(OPTION_PROTOCOL_TIMEOUT));
    bless $self, $class;

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'self', value => $self}
    );
}

####################################################################################################################################
# init
####################################################################################################################################
sub init
{
    my $self = shift;

    # Assign function parameters, defaults, and log debug info
    my ($strOperation) = logDebugParam(__PACKAGE__ . '->init');

    # Create anonymous subs for each command
    my $hCommandMap =
    {
        &OP_ARCHIVE_PUSH_ASYNC => sub {return @{shift()}},
    };

    # Return from function and log return values if any
    return logDebugReturn
    (
        $strOperation,
        {name => 'hCommandMap', value => $hCommandMap}
    );
}

1;
