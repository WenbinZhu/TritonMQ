namespace java edu.ucsd.tritonmq.broker

service BrokerService {

    void send(1:binary record)

}