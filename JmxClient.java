import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import sun.tools.jconsole.LocalVirtualMachine;
import org.apache.log4j.Logger;

public class JmxClient {
    static Logger log = Logger.getLogger(JmxClient.class.getName());

    private int pid_;
    private String bean_;
    private List<String> attributes_;
    private String url_;
    private JMXConnector connector_;

    public JmxClient(int pid, String bean, String csvAttributes) {
        pid_ = pid;
        bean_ = bean;
        String[] attrs = csvAttributes.split(",");
        attributes_ = new ArrayList<String>(Arrays.asList(attrs));
        url_ = null;
        connector_ = null;
    }

    public JmxClient(int pid) {
        pid_ = pid;
        bean_ = null;
        attributes_ = null;
        url_ = null;
        connector_ = null;
    }
    
    public void open() throws IOException {
        LocalVirtualMachine vm = LocalVirtualMachine.getLocalVirtualMachine(pid_);
        url_ = vm.connectorAddress();
        // System.out.println("Connecting to MBean server: " + url_);
        connector_ = JMXConnectorFactory.connect(new JMXServiceURL(url_), null /* env */);
        log.info("Opened connection to pid " + pid_);
    }

    public boolean isOpened() {
        return (connector_ != null);
    }
    
    public void close() throws IOException {
        if (connector_ != null) {
            connector_.close();
            log.info("Bye");
        }
    }

    public Map<String, Object> getAttributeValues(String bean, String csvAttributes) 
        throws JMException, IOException {
        bean_ = bean;
        String[] attrs = csvAttributes.split(",");
        attributes_ = new ArrayList<String>(Arrays.asList(attrs));
        return getAttributeValues();
    }

    public Map<String, Object> getAttributeValues() throws JMException, IOException {
        if (attributes_ == null || attributes_.isEmpty())
            throw new IllegalArgumentException( "Please specify at least one attribute" );
        if (bean_ == null)
            throw new IllegalArgumentException( "Please specify a valid bean name" );

        ObjectName beanName = new ObjectName(bean_);
        MBeanServerConnection conn = connector_.getMBeanServerConnection();
        MBeanAttributeInfo[] attrInfos = conn.getMBeanInfo(beanName).getAttributes();
        Map<String, MBeanAttributeInfo> attrNames = new TreeMap<String, MBeanAttributeInfo>();

        if (attributes_.contains("*")) {
            for (MBeanAttributeInfo attrInfo : attrInfos)
                attrNames.put(attrInfo.getName(), attrInfo);
        }
        else {
            for (String attr : attributes_) {
                for (MBeanAttributeInfo attrInfo : attrInfos) {
                    if (attrInfo.getName().equals(attr)) {
                        attrNames.put(attr, attrInfo);
                        break;
                    }
                }
            }
        }

        Map<String, Object> attrValues = new TreeMap<String, Object>();
        for (Map.Entry<String, MBeanAttributeInfo> entry : attrNames.entrySet()) {
            String attrName = entry.getKey();
            MBeanAttributeInfo attrInfo = entry.getValue();
            if (attrInfo.isReadable()) {
                Object result = conn.getAttribute(beanName, attrName);
                attrValues.put(bean_ + "#" + attrName, result);
            }
        }

        return attrValues;
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java JmxClient [pid] [bean#attributes(csv)]+");
            System.exit(1);
        }

        JmxClient jmx = new JmxClient(Integer.parseInt(args[0]));

        try {
            jmx.open();
            for (int i = 1; i < args.length; i++) {
                String bean = args[i].substring(0, args[i].indexOf('#'));
                String csvAttributes = args[i].substring(args[i].indexOf('#') + 1);
                Map<String, Object> vals = jmx.getAttributeValues(bean, csvAttributes);
            }
            jmx.close();
        } catch (IOException ex) {
            log.error(ex);
        } catch (JMException ex) {
            log.error(ex);
        }
    }
}
