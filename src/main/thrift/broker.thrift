namespace java edu.ucsd.tritonmq.broker

service BrokerService {

    void publish(1:string topic, 2:binary message)

}