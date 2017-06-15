from subprocess import Popen, PIPE, CalledProcessError
from threading import Thread
import sys, os

ROOT = os.path.abspath(__file__ + '/../../../..')

def monitor_process(process, error):
    """
    :type process: Popen
    :type error: CalledProcessError
    :param process:
    :return:
    """
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        print stdout
        print >> sys.stderr, stderr
        error.returncode = process.returncode
        error.output = stdout
        raise error


def run_class(name, **config):
    """
    :type name: str
    :type config: dict
    :return None
    """
    cmd = 'cd {} && mvn exec:java -Dexec.mainClass={}'.format(ROOT, name)
    args = ''
    for key, value in config.iteritems():
        args += ' --{}={}'.format(key, value)
    cmd += ' -Dexec.args="{}"'.format(args)
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    Thread(target=monitor_process, args=(p, CalledProcessError(0, name, ''))).start()
    return p


if __name__ == '__main__':
    cmd = 'cd {} && mvn exec:java -Dexec.mainClass={}'.format(ROOT, sys.argv[1])
    cmd += ' -Dexec.args="{}"'.format(' '.join(sys.argv[2:]))
    os.system(cmd)