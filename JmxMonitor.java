import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Map;

import javax.management.JMException;

import sun.tools.jconsole.LocalVirtualMachine;


public class JmxMonitor {
    static private int LISTEN_PORT = 8888;
    static private int ACCEPT_TIMEOUT_MS = 3000; // [ms]

    private int port_;
    private ServerSocket serverSock_;

    private JmxClient client_;
    private int pid_;
    private String[] beans_;
    private String[] csvAttributes_;

    public JmxMonitor(String[] args) {
        port_ = LISTEN_PORT;
        try {
            serverSock_ = new ServerSocket(port_);
            serverSock_.setSoTimeout(ACCEPT_TIMEOUT_MS);
        } catch (Exception ex) {
            System.err.println(ex);
        }

        pid_ = getPid(args[0] /* class name */);
        beans_ = new String[args.length - 1];
        csvAttributes_ = new String[args.length - 1];
        for (int i = 1; i < args.length; i++) {
            beans_[i - 1] = args[i].substring(0, args[i].indexOf('#'));
            csvAttributes_[i - 1] = args[i].substring(args[i].indexOf('#') + 1);
        }

        client_ = new JmxClient(pid_);
    }

    private int getPid(String className) {
        int pid = -1;

        Map<Integer, LocalVirtualMachine> vms = LocalVirtualMachine.getAllVirtualMachines();
        for (Map.Entry<Integer, LocalVirtualMachine> entry : vms.entrySet()) {
            LocalVirtualMachine vm = entry.getValue();
            if (vm.displayName().startsWith(className)) {
                pid = vm.vmid();
                System.out.println("Found vm \"" + vm.displayName() + "\" with pid " + pid);
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

                for (int i = 0; i < beans_.length; i++) {
                    Map<String, Object> vals = client_.getAttributeValues(beans_[i], csvAttributes_[i]);
                    for (Map.Entry<String, Object> val : vals.entrySet()) {
                        str += val.getValue() + ", ";
                    }
                }

                str = str.substring(0, str.lastIndexOf(','));
                System.out.println(str);

                try {
                    Socket sock = serverSock_.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                    String data = in.readLine();
                    if (data.equals("bye") || data.equals("quit")) {
                        sock.close();
                        serverSock_.close();
                        break;
                    }
                } catch (SocketTimeoutException ex) {
                    ;
                }
            }

            client_.close();
        } catch (IOException ex) {
            System.err.println(ex);
        } catch (JMException ex) {
            System.err.println(ex);
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java JmxMonitor [monitoring class name] [bean'#'attributes(csv)]+");
            System.exit(1);
        }

        JmxMonitor monitor  = new JmxMonitor(args);
        monitor.doMonitor();
    }
}
