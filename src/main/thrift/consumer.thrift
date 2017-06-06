namespace java edu.ucsd.tritonmq.broker

service ConsumerService {

    string deliver(1:binary record)

}