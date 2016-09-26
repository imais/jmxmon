import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.management.JMException;
import sun.tools.jconsole.LocalVirtualMachine;
import org.apache.log4j.Logger;


public class KafkaMonitor {
    static Logger log = Logger.getLogger(KafkaMonitor.class.getName());

    static private final int LISTEN_PORT = 8888;
    static private final int ACCEPT_TIMEOUT_MS = 3000; // [ms]
    static private final String KAKFA_BROKER_CLASSNAME = "kafka.Kafka";
    static private final String[] MONITORING_METRICS = {
        // bean#attributes(csv)
        "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec#OneMinuteRate",
        "kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics#OneMinuteRate",
        "kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics#OneMinuteRate"
    };
    // Producer scaling decision: K out of N
    static private final int K = 8;
    static private final int N = 10;
    static private final int SCALING_THRESHOLD_PERCENTAGE = 3;
    static private final int COOLDOWN_PERIOD_MS = 60000; // [ms]
    // Termination criteria: program terminates if both of the following conditions meet
    // 1. messagesInPerSec_ is within a% of totalMessagesInPerSec_
    // 2. "delta between BytesOutPerSec and lastBytesOutPerSec is less than b%" 
    //    is observed M times in a row
    static private final int TERMINATION_MESSAGESIN_THRESHOLD_PERCENTAGE = 1;   // a
    static private final int TERMINATION_BYTESOUT_THRESHOLD_PERCENTAGE = 5;     // b
    static private final int M = 10;

    private int port_;
    private ServerSocket serverSock_;

    private JmxClient client_;
    private int pid_;
    private String[] beans_;
    private String[] csvAttributes_;
    private long startTime_;

    private String[] producerIpAddrs_; // ip1:port1,ip2:port2,...
    private Socket[] producerSocks_;
    private int currentProducer_;
    private LinkedList<Boolean> lastNChecks_;
    private long lastScalingTime_;
    private int messagesInPerSec_;     
    private boolean messagesInPerSecWithinThreshold_;
    private int totalMessagesInPerSec_;
    private double maxBytesOutPerSec_;
    private double lastBytesOutPerSec_;
    private int numBytesOutDeltaWithinThreshold_;

    public KafkaMonitor(String[] args) {
        port_ = LISTEN_PORT;
        try {
            serverSock_ = new ServerSocket(port_);
            serverSock_.setSoTimeout(ACCEPT_TIMEOUT_MS);
        } catch (Exception ex) {
            log.error(ex);
        }

        pid_ = getPid(KAKFA_BROKER_CLASSNAME);
        beans_ = new String[MONITORING_METRICS.length];
        csvAttributes_ = new String[MONITORING_METRICS.length];
        for (int i = 0; i < MONITORING_METRICS.length; i++) {
            beans_[i] = MONITORING_METRICS[i].substring(0, MONITORING_METRICS[i].indexOf('#'));
            csvAttributes_[i] = MONITORING_METRICS[i].
                substring(MONITORING_METRICS[i].indexOf('#') + 1);
        }

        client_ = new JmxClient(pid_);
        startTime_ = System.currentTimeMillis();

        // Producer scaling related
        producerIpAddrs_ = args[0].split(",");
        currentProducer_ = 0;
        lastNChecks_ = new LinkedList<Boolean>();
        lastScalingTime_ = 0;
        messagesInPerSec_ = Integer.parseInt(args[1]);
        messagesInPerSecWithinThreshold_ = false;
        totalMessagesInPerSec_ = 0;
        maxBytesOutPerSec_ = 0.0;
        lastBytesOutPerSec_ = 0.0;
        numBytesOutDeltaWithinThreshold_ = 0;

        producerSocks_ = new Socket[producerIpAddrs_.length];
        for (int i = 0; i < producerIpAddrs_.length; i++) {
            int semiColonIndex = producerIpAddrs_[i].indexOf(':');
            String ipAddr = producerIpAddrs_[i].substring(0, semiColonIndex);
            int port = Integer.parseInt(producerIpAddrs_[i].substring(semiColonIndex + 1));
            try {
                producerSocks_[i] = new Socket(ipAddr, port);
            } catch (IOException ex) {
                log.error(ex);
            }
            
        }

        log.info("KafkaMonitor started at time " + startTime_ + " listening port " + port_);
    }

    private int getPid(String className) {
        int pid = -1;

        Map<Integer, LocalVirtualMachine> vms = LocalVirtualMachine.getAllVirtualMachines();
        for (Map.Entry<Integer, LocalVirtualMachine> entry : vms.entrySet()) {
            LocalVirtualMachine vm = entry.getValue();
            if (vm.displayName().startsWith(className)) {
                pid = vm.vmid();
                log.info("Found vm \"" + vm.displayName() + "\" with pid " + pid);
                break;
            }
        }

        return pid;
    }

    private boolean checkIfTerminate(Map<String, Object> vals) {
        boolean terminate = false;

        double bytesOutPerSec = 0.0;
        for (Map.Entry<String, Object> entry : vals.entrySet()) {
            String beanAttr = entry.getKey();
            if (beanAttr.contains("BytesOutPerSec")) {
                bytesOutPerSec = (Double)entry.getValue();
                break;
            }
        }
        
        if (messagesInPerSecWithinThreshold_) {
            if (100 * Math.abs((lastBytesOutPerSec_ - bytesOutPerSec) / lastBytesOutPerSec_) <
                TERMINATION_BYTESOUT_THRESHOLD_PERCENTAGE) {
                numBytesOutDeltaWithinThreshold_++;
                log.debug("bytesOutPerSec:" +  bytesOutPerSec +
                          " is within " + TERMINATION_BYTESOUT_THRESHOLD_PERCENTAGE +
                          "% of lastBytesOutPerSec: " + lastBytesOutPerSec_ + 
                          ", M: " + numBytesOutDeltaWithinThreshold_);
            }
            else {
                numBytesOutDeltaWithinThreshold_ = 0;
                log.debug("bytesOutPerSec:" +  bytesOutPerSec +
                          " is out of " + TERMINATION_BYTESOUT_THRESHOLD_PERCENTAGE +
                          "% of lastBytesOutPerSec: " + lastBytesOutPerSec_);
            }
            if (M <= numBytesOutDeltaWithinThreshold_) 
                terminate = true;
        }
        else {
            double messagesInPerSec = 0.0;
            for (Map.Entry<String, Object> entry : vals.entrySet()) {
                String beanAttr = entry.getKey();
                if (beanAttr.contains("MessagesInPerSec")) {
                    messagesInPerSec = (Double)entry.getValue();
                    break;
                }
            }

            if ((totalMessagesInPerSec_ * (100 - TERMINATION_MESSAGESIN_THRESHOLD_PERCENTAGE) / 100) < 
                messagesInPerSec) {
                log.info("messagesInPerSec:" +  messagesInPerSec + 
                         " reached within " + TERMINATION_MESSAGESIN_THRESHOLD_PERCENTAGE +
                         "% of totalMessagesInPerSec: " + totalMessagesInPerSec_);
                messagesInPerSecWithinThreshold_ = true;
            }
        }

        lastBytesOutPerSec_ = bytesOutPerSec;

        return terminate;
    }

    private boolean checkIfScaleProducers(Map<String, Object> vals) {
        if (lastScalingTime_ == 0) {
            log.debug("First time, always create a new producer");
            return true;
        }

        // K out of N check
        double bytesInPerSec = 0.0, bytesOutPerSec = 0.0;
        for (Map.Entry<String, Object> entry : vals.entrySet()) {
            String beanAttr = entry.getKey();
            if (beanAttr.contains("BytesInPerSec"))
                bytesInPerSec = (Double)entry.getValue();
            else if (beanAttr.contains("BytesOutPerSec")) {
                bytesOutPerSec = (Double)entry.getValue();
                if (maxBytesOutPerSec_ < bytesOutPerSec) 
                    maxBytesOutPerSec_ = bytesOutPerSec;
            }
        }

        // check if the consumer is processing at least (100-alpha)% of the producer throughput
        boolean isConsumerKeepingUp = ((bytesInPerSec * (100 - SCALING_THRESHOLD_PERCENTAGE) / 100) < bytesOutPerSec);
        lastNChecks_.addLast(new Boolean(isConsumerKeepingUp));

        long now = System.currentTimeMillis();
        long cooldownPeriod = lastScalingTime_ + COOLDOWN_PERIOD_MS;
        if (now < cooldownPeriod) {
            log.debug("In cooldown period (now: " + now + ", cooldown: " + cooldownPeriod + ")");
            return false;       
        }

        if (N <= lastNChecks_.size()) {
            // we have done enough checks
            int k = 0;
            int index = lastNChecks_.size() - 1;
            for (int i = 0; i < N; i++) {
                boolean check = lastNChecks_.get(index);
                if (check) k++;
                index--;
            }
            log.debug("K out N check: K " + k + ", N " + N);
            lastNChecks_.removeFirst(); // remove the oldest one for the next check
            return (K <= k);
        }

        log.debug("Not enough checks yet (check: " + lastNChecks_.size() + " times)");
        return false;               
    }

    private void requestNewProducer() {
        try {
            // request creating a new producer by sending producer throughput
            Socket sock = producerSocks_[currentProducer_];
            DataOutputStream out = new DataOutputStream(sock.getOutputStream());
            out.writeBytes(Integer.toString(messagesInPerSec_) + "\n"); 
            log.debug("Sent request to producer " + currentProducer_ +
                      " with throughput " + messagesInPerSec_);
        } catch (IOException ex) {
            log.error(ex);
        }
        
        lastScalingTime_ = System.currentTimeMillis();
        currentProducer_ = (currentProducer_ + 1) % producerIpAddrs_.length;
        totalMessagesInPerSec_ += messagesInPerSec_;
        log.debug("Updated lastScalingTime: " + lastScalingTime_ + 
                  ", currentProducer: " + currentProducer_ + 
                  ", totalProducerThroughput: " + totalMessagesInPerSec_);
    }

    public void doMonitor() {
        try {
            client_.open();

            while (true) {
                String str = (System.currentTimeMillis() - startTime_) + ", ";

                Map<String, Object> allVals = new TreeMap<String, Object>();
                for (int i = 0; i < beans_.length; i++) {
                    Map<String, Object> vals = client_.getAttributeValues(beans_[i], csvAttributes_[i]);
                    for (Map.Entry<String, Object> val : vals.entrySet()) {
                        str += val.getValue() + ", ";
                    }
                    allVals.putAll(vals);
                }

                str = str.substring(0, str.lastIndexOf(','));
                log.info(str);

                if (checkIfTerminate(allVals))
                    break;
                else if (checkIfScaleProducers(allVals)) {
                    requestNewProducer();
                }

                try {
                    Socket sock = serverSock_.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                    String data = in.readLine();
                    if (data.equals("bye") || data.equals("quit")) {
                        sock.close();
                        serverSock_.close();
                        for (int i = 0; i < producerSocks_.length; i++)
                            producerSocks_[i].close();
                        break;
                    }
                } catch (SocketTimeoutException ex) {
                    ;
                }
            }

            client_.close();
            log.info("maxConsumerThroughput: " + maxBytesOutPerSec_);
            
        } catch (IOException ex) {
            log.error(ex);
        } catch (JMException ex) {
            log.error(ex);
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java KafkaMonitor [Kafka Producer IP addrs(csv)] [producer throughput");
            System.exit(1);
        }

        // TODO: add handler for Ctrl-C signal

        KafkaMonitor kakfaMon  = new KafkaMonitor(args);
        kakfaMon.doMonitor();
    }
}
