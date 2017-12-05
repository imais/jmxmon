import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Map;

import javax.management.JMException;
import sun.tools.jconsole.LocalVirtualMachine;
import org.apache.log4j.Logger;


public class JmxMonitor {
    static Logger log = Logger.getLogger(JmxMonitor.class.getName());

    static private int LISTEN_PORT = 9999;
    static private int ACCEPT_TIMEOUT_MS = 3000; // [ms]

    private int port_;
    private ServerSocket serverSock_;

    private int pid_;
    private JmxClient client_;
    private ArrayList<String[]> beanAttrList_;
    private long startTime_;

    public JmxMonitor(String className, String beansFile) {

        port_ = LISTEN_PORT;
        try {
            serverSock_ = new ServerSocket(port_);
            serverSock_.setSoTimeout(ACCEPT_TIMEOUT_MS);
        } catch (Exception ex) {
            log.error(ex);
        }

        pid_ = getPid(className);
        if (pid_ < 0) {
            log.error("pid not found for " + className);
            System.exit(1);
        }

        beanAttrList_ = new ArrayList<String[]>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(beansFile));
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] beanAttr = line.split("#");
                beanAttrList_.add(beanAttr);
            }
            reader.close();
        } catch  (Exception ex) {
            ex.printStackTrace();
        }
        // for (String[] beanAttr : beanAttrList_)
        //     System.out.println(beanAttr[0] + ", " + beanAttr[1]);

        client_ = new JmxClient(pid_);
        startTime_ = System.currentTimeMillis();
        log.info("Started at time " + startTime_ + " with listening port " + port_);
    }

    private int getPid(String className) {
        int pid = -1;

        Map<Integer, LocalVirtualMachine> vms = LocalVirtualMachine.getAllVirtualMachines();
        for (Map.Entry<Integer, LocalVirtualMachine> entry : vms.entrySet()) {
            LocalVirtualMachine vm = entry.getValue();
            // if (vm.displayName().startsWith(className)) {
            if (vm.displayName().contains(className) && 
                !vm.displayName().contains(JmxMonitor.class.getName())) {
                pid = vm.vmid();
                log.info("Found vm \"" + vm.displayName() + "\" with pid " + pid);
                break;
            }
        }

        return pid;
    }

    public void doMonitor() {
        try {
            client_.open();

            while (true) {
                String str = System.currentTimeMillis() + ", ";

                for (String[] beanAttr : beanAttrList_) {
                    Map<String, Object> vals = client_
                        .getAttributeValues(beanAttr[0], beanAttr[1]);
                    for (Map.Entry<String, Object> val : vals.entrySet()) {
                        if (val.getValue() instanceof Double)
                            str += String.format("%.3f", val.getValue()) + ", ";
                        else
                            str += val.getValue() + ", ";
                    }
                }

                str = str.substring(0, str.lastIndexOf(','));
                log.info(str);

                try {
                    Socket sock = serverSock_.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                    String data = in.readLine();
                    if (data.equals("bye") || data.equals("quit")) {
                        sock.close();
                        serverSock_.close();
                        break;
                    }
                    else if (data.equals("get_data")) {
                        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
                        out.println(str);
                        sock.close();
                    }
                } catch (SocketTimeoutException ex) {
                    ;
                }
            }

            client_.close();
        } catch (IOException ex) {
            log.error(ex);
        } 
        catch (JMException ex) {
            log.error(ex);
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java JmxMonitor [monitoring class name] [beans file]");
            System.exit(1);
        }

        JmxMonitor monitor  = new JmxMonitor(args[0], args[1]);
        monitor.doMonitor();
    }
}
