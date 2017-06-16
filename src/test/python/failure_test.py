from test_helper import run_class, build_first
from time import sleep
import random

def main():
    build_first()
    brokers = [
        run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9000, groupId=0),
        run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9001, groupId=0),
        run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9002, groupId=0),
        run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9003, groupId=1),
        run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9004, groupId=1),
        run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9005, groupId=1),
        run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9006, groupId=2),
        run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9007, groupId=2),
        run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9008, groupId=2),
    ]
    sleep(20)
    while True:
        print [i is not None for i in brokers]
        i = raw_input('Please input a number to manipulate (empty to random one) ')
        if i.isdigit():
            i = int(i)
        else:
            i = random.randrange(0, 9)
        group = i // 3
        if brokers[i] is None:
            print "================ Start {} in group {}".format(i, group)
            brokers[i] = run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9000 + i, groupId=group)
        elif sum([brokers[3*group+j] is not None for j in [0, 1, 2]]) == 1:
            # only one broker is up
            print 'Refuse to kill the last broker in one group'
            continue
        else:
            print "================ Stop {} in group {}".format(i, group)
            brokers[i].kill()
            brokers[i] = None
        sleep(10)

if __name__ == '__main__':
    main()