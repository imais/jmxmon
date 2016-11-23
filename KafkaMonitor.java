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
    static private final int COOLDOWN_THRESHOLD_PERCENTAGE = 5;
    // BytesIn < BytesOut
    // See why this happens: http://mail-archives.apache.org/mod_mbox/kafka-users/201404.mbox/%3cCAFbh0Q1ikxwxce8i5H_yEd9DfsxUJSoJoQ7AYpGvwQexii-nYA@mail.gmail.com%3e
    static private final int BYTES_INOUT_DIFF_PERCENTAGE = 15;

    // Termination criteria: program terminates if both conditions 1 and 2 meet
    //  OR
    // condition 3 meet
    // 1. messagesInPerSec_ is within a% of totalMessagesInPerSec_
    // 2. "delta between BytesOutPerSec and lastBytesOutPerSec is less than b%" 
    //    is observed M times in a row
    // 3. bytesOutPerSec_ drops more than c% from its peak value (maxBytesOutPerSec-)
    static private final int TERMINATION_MESSAGESIN_THRESHOLD_PERCENTAGE = 3;   // a
    static private final int TERMINATION_BYTESOUT_THRESHOLD_PERCENTAGE = 5;     // b
    static private final int TERMINATION_BYTESOUT_DROP_THRESHOLD_PERCENTAGE = 10;// c
    static private final int M = 10;


    private int port_;
    private ServerSocket serverSock_;

    private JmxClient client_;
    private int pid_;
    private String[] beans_;
    private String[] csvAttributes_;
    private long startTime_;

    private boolean producerScalingEnabled_;
    private String[] producerIpAddrs_; // ip1:port1,ip2:port2,...
    private Socket[] producerSocks_;
    private int currentProducer_;
    private LinkedList<Boolean> lastNChecks_;
    private long lastScalingTime_;
    private int messagesInPerSec_;     
    private double maxMessagesInPerSec_;
    private double maxBytesInPerSec_;
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

        if (2 <= args.length) {
            // Producer scaling enabled
            producerScalingEnabled_ = true;

            messagesInPerSec_ = Integer.parseInt(args[1]);
            messagesInPerSecWithinThreshold_ = false;
            totalMessagesInPerSec_ = 0;
            maxBytesOutPerSec_ = 0.0;
            maxMessagesInPerSec_ = 0.0;
            lastBytesOutPerSec_ = 0.0;
            numBytesOutDeltaWithinThreshold_ = 0;

            producerIpAddrs_ = args[0].split(",");
            currentProducer_ = 0;
            lastNChecks_ = new LinkedList<Boolean>();
            lastScalingTime_ = 0;

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
        }
        else 
            producerScalingEnabled_ = false;

        log.info("KafkaMonitor started at time " + startTime_ + 
                 ", listening port: " + port_ +
                 ", producer scaling: " + producerScalingEnabled_);
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

        double bytesOutPerSec = 0.0, bytesInPerSec = 0.0, messagesInPerSec = 0.0;
        for (Map.Entry<String, Object> entry : vals.entrySet()) {
            String beanAttr = entry.getKey();
            if (beanAttr.contains("BytesOutPerSec"))
                bytesOutPerSec = (Double)entry.getValue();
            else if (beanAttr.contains("BytesInPerSec")) {
                bytesInPerSec = (Double)entry.getValue();
                if (maxBytesInPerSec_ < bytesInPerSec)
                    maxBytesInPerSec_ = bytesInPerSec;
            }
            else if (beanAttr.contains("MessagesInPerSec")) {
                messagesInPerSec = (Double)entry.getValue();
                if (maxMessagesInPerSec_ < messagesInPerSec)
                    maxMessagesInPerSec_ = messagesInPerSec;
            }
        }

        if (bytesOutPerSec < maxBytesOutPerSec_) {
            double dropPercent = 
                100 * (double)(maxBytesOutPerSec_ - bytesOutPerSec) / maxBytesOutPerSec_;
            if (TERMINATION_BYTESOUT_DROP_THRESHOLD_PERCENTAGE <= dropPercent) {
                log.debug("bytesOutPerSec:" + bytesOutPerSec + 
                          " dropped more than " + TERMINATION_BYTESOUT_DROP_THRESHOLD_PERCENTAGE +
                          "% of maxBytesOutPerSec_: " + maxBytesOutPerSec_);
                terminate = true;
            }
        }

        if (!terminate) {
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
                // check if messagesInPerSec is within a% of totalMessagesInPerSec_
                if ((totalMessagesInPerSec_ * 
                     (100 - TERMINATION_MESSAGESIN_THRESHOLD_PERCENTAGE) / 100) < messagesInPerSec) {
                    log.debug("messagesInPerSec:" +  messagesInPerSec + 
                              " reached within " + TERMINATION_MESSAGESIN_THRESHOLD_PERCENTAGE +
                              "% of totalMessagesInPerSec: " + totalMessagesInPerSec_);
                    messagesInPerSecWithinThreshold_ = true;
                }
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
        double bytesInPerSec = 0.0, messagesInPerSec = 0.0, bytesOutPerSec = 0.0;
        for (Map.Entry<String, Object> entry : vals.entrySet()) {
            String beanAttr = entry.getKey();
            if (beanAttr.contains("BytesInPerSec"))
                bytesInPerSec = (Double)entry.getValue();
            else if (beanAttr.contains("MessagesInPerSec"))
                messagesInPerSec = (Double)entry.getValue();
            else if (beanAttr.contains("BytesOutPerSec")) {
                bytesOutPerSec = (Double)entry.getValue();
                if (maxBytesOutPerSec_ < bytesOutPerSec &&
                    bytesOutPerSec <= 
                    maxBytesInPerSec_ * (100 + BYTES_INOUT_DIFF_PERCENTAGE) / 100) {
                    /* Output should be within a reasonable range from input throughput */
                    maxBytesOutPerSec_ = bytesOutPerSec;
                    log.debug("Updated maxBytesOutPerSec: " + maxBytesOutPerSec_);
                }
            }
        }

        // check if the consumer is processing at least (100-alpha)% of the producer throughput
        boolean isConsumerKeepingUp = ((bytesInPerSec * (100 - SCALING_THRESHOLD_PERCENTAGE) / 100) < bytesOutPerSec);
        lastNChecks_.addLast(new Boolean(isConsumerKeepingUp));


        // cooldown check - time based
        long now = System.currentTimeMillis();
        long cooldownPeriod = lastScalingTime_ + COOLDOWN_PERIOD_MS;
        if (now < cooldownPeriod) {
            double countdown = (double)(cooldownPeriod - now) / 1000;
            log.debug("Cooling down (time remaining: " + countdown + " sec)");
            return false;       
        }
        // cooldown check - messagesInPerSec based
        else if (messagesInPerSec < totalMessagesInPerSec_) {
            double messagesInPerSecPercent = 
                100 * (double)(totalMessagesInPerSec_ - messagesInPerSec) / totalMessagesInPerSec_;
            if (COOLDOWN_THRESHOLD_PERCENTAGE < messagesInPerSecPercent) {
                log.debug("Cooling down (msgInPerSecPercent now: " + messagesInPerSecPercent + 
                          ", threshold: " + COOLDOWN_THRESHOLD_PERCENTAGE + ")");
                messagesInPerSecWithinThreshold_ = false;                
                return false;       
            }
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
                      " with messagesInPerSec " + messagesInPerSec_);
        } catch (IOException ex) {
            log.error(ex);
        }
        
        lastScalingTime_ = System.currentTimeMillis();
        currentProducer_ = (currentProducer_ + 1) % producerIpAddrs_.length;
        totalMessagesInPerSec_ += messagesInPerSec_;
        log.debug("Updated lastScalingTime: " + lastScalingTime_ + 
                  ", currentProducer: " + currentProducer_ + 
                  ", totalMessagesInPerSec: " + totalMessagesInPerSec_);
    }

    public void doMonitor() {
        try {
            client_.open();

            while (true) {
                // String str = (double)(System.currentTimeMillis() - startTime_)/1000 + ", ";
                String str = System.currentTimeMillis() + ", ";

                Map<String, Object> allVals = new TreeMap<String, Object>();
                for (int i = 0; i < beans_.length; i++) {
                    Map<String, Object> vals = client_.getAttributeValues(beans_[i], csvAttributes_[i]);
                    for (Map.Entry<String, Object> val : vals.entrySet()) {
                        str += String.format("%.3f", val.getValue()) + ", ";
                    }
                    allVals.putAll(vals);
                }

                str = str.substring(0, str.lastIndexOf(','));
                log.info(str);

                if (producerScalingEnabled_) {
                    if (checkIfTerminate(allVals))
                        break;
                    else if (checkIfScaleProducers(allVals)) {
                        requestNewProducer();
                    }
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
            log.info("maxBytesOutPerSec: " + maxBytesOutPerSec_);
            
        } catch (IOException ex) {
            log.error(ex);
        } catch (JMException ex) {
            log.error(ex);
        }
    }

    public static void main(String[] args) {
        if (2 < args.length) {
            System.err.println("Usage: java KafkaMonitor [Kafka Producer IP addrs(csv)] [producer messagesInPerSec]");
            System.exit(1);
        }

        // TODO: add handler for Ctrl-C signal

        KafkaMonitor kakfaMon  = new KafkaMonitor(args);
        kakfaMon.doMonitor();
    }
}
