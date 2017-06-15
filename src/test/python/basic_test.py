from test_helper import run_class
from time import sleep

def main():
    run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9000, groupId=0)
    run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9001, groupId=1)
    run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9002, groupId=2)
    sleep(1)
    run_class('edu.ucsd.tritonmq.app.ConsumerApp')
    sleep(1)
    p = run_class('edu.ucsd.tritonmq.app.ProducerApp')
    p.wait()

if __name__ == '__main__':
    main()