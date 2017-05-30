namespace java edu.ucsd.tritonmq.broker

service BrokerService {

    void send(1:string topic, 2:binary record)

}