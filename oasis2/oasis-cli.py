from nubia import Nubia
from nubia import Options

from cli import cmds
from oasis.utils.logger import logger

if __name__ == '__main__':
    import sys

    logger.init_logger('cli', 'cli')

    shell = Nubia(
        name='oasis-cli',
        command_pkgs=cmds,
        options=Options(
            persistent_history=True, auto_execute_single_suggestions=False
        ),
    )
    sys.exit(shell.run())
