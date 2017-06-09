namespace java edu.ucsd.tritonmq.broker

service BrokerService {

    string send(1:binary record)

    string migrate(1:binary record)

    string replicate(1:binary record)

}