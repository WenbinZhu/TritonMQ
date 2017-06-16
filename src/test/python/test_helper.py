from subprocess import Popen, PIPE, CalledProcessError
from threading import Thread
import sys, os
from time import sleep

ROOT = os.path.abspath(__file__ + '/../../../..')
_built = False


def build_first():
    global _built
    if _built is False:
        assert os.system('cd {} && mvn compile'.format(ROOT)) == 0
        _built = True


def monitor_process(process, error):
    """
    :type process: Popen
    :type error: CalledProcessError
    """
    stdout, stderr = process.communicate()
    if process.returncode not in [0, -9]:
        print stdout
        print >> sys.stderr, stderr
        error.returncode = process.returncode
        error.output = stdout
        raise error
    else:
        process.stdout = stdout
        process.stderr = stderr


def build_cmd(name, args):
    return 'cd {} && exec mvn exec:java -Dexec.mainClass={}'.format(ROOT, name) + \
            ' -Dexec.args="{}"'.format(' '.join(args))


def run_class(name, **config):
    """
    :type name: str
    :type config: dict
    """
    args = ['--{}={}'.format(key, value) for key, value in config.iteritems()]
    cmd = build_cmd(name, args)
    p = Popen(cmd, shell=True)
    # p = Popen(cmd, shell=True, stdout=1, stderr=2)
    # Thread(target=monitor_process, args=(p, CalledProcessError(0, name, ''))).start()
    sleep(0.5)
    return p


if __name__ == '__main__':
    build_first()
    os.system(build_cmd(sys.argv[1], sys.argv[2:]))