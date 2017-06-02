namespace java edu.ucsd.tritonmq.broker

service BrokerService {

    string send(1:binary record)

}