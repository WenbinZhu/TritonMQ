from test_helper import run_class
from time import sleep

def main():
    """
    Test a basic setting where there're 3 groups, 1 server per group, 1
    producer and 1 consumer. The producer keeps producing 1000 messages across
    10 topics, 100 messages per topic. The consumer keeps consuming messages
    of these 10 topics.
    """
    run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9000, groupId=0)
    run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9001, groupId=1)
    run_class('edu.ucsd.tritonmq.app.BrokerApp', port=9002, groupId=2)
    sleep(5)
    c = run_class('edu.ucsd.tritonmq.app.ConsumerApp')
    sleep(1)
    p = run_class('edu.ucsd.tritonmq.app.ProducerApp', recordCount=10, topicCount=5, interval=0)
    p.wait()
    c.terminate()
    sleep(1)
    lines = c.stdout.split('\n')
    assert len([l for l in lines if l[:8] == '[topic0]']) == 10
    assert len([l for l in lines if l[:8] == '[topic1]']) == 10
    assert len([l for l in lines if l[:8] == '[topic2]']) == 10
    assert len([l for l in lines if l[:8] == '[topic3]']) == 10
    assert len([l for l in lines if l[:8] == '[topic4]']) == 10


if __name__ == '__main__':
    main()