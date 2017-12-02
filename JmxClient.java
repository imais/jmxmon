import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import sun.tools.jconsole.LocalVirtualMachine;
import org.apache.log4j.Logger;

public class JmxClient {
    static Logger log = Logger.getLogger(JmxClient.class.getName());

    private int pid_;
    private JMXConnector connector_;

    public JmxClient(int pid) {
        pid_ = pid;
        connector_ = null;
    }
    
    public void open() throws IOException {
        LocalVirtualMachine vm = LocalVirtualMachine.getLocalVirtualMachine(pid_);
        String url = vm.connectorAddress();
        // log.debug("Connecting to MBean server: " + url);
        connector_ = JMXConnectorFactory.connect(new JMXServiceURL(url), null /* env */);
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
        String[] attrs = csvAttributes.split(",");
        ArrayList<String> attributes = new ArrayList<String>(Arrays.asList(attrs));

        if (attributes == null || attributes.isEmpty())
            throw new IllegalArgumentException( "Please specify at least one attribute" );
        if (bean == null)
            throw new IllegalArgumentException( "Please specify a valid bean name" );

        ObjectName beanName = new ObjectName(bean);
        MBeanServerConnection conn = connector_.getMBeanServerConnection();
        MBeanAttributeInfo[] attrInfos = conn.getMBeanInfo(beanName).getAttributes();
        Map<String, MBeanAttributeInfo> attrNames = new TreeMap<String, MBeanAttributeInfo>();

        if (attributes.contains("*")) {
            for (MBeanAttributeInfo attrInfo : attrInfos)
                attrNames.put(attrInfo.getName(), attrInfo);
        }
        else {
            for (String attr : attributes) {
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
                if (result instanceof CompositeDataSupport ) {
                    Set<String> keys = ((CompositeDataSupport)result)
                        .getCompositeType().keySet();
                    for (String key : keys) {
                        Object val = ((CompositeData)result).get(key);
                        attrValues.put(bean + "#" + attrName + "-" + key, val); 
                    }
                }
                else {
                    attrValues.put(bean + "#" + attrName, result);
                }
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
                for (Map.Entry<String, Object> entry : vals.entrySet()) {
                    log.info(entry.getKey() + ": " + entry.getValue());
                }
            }
            jmx.close();
        } catch (IOException ex) {
            log.error(ex);
        } catch (JMException ex) {
            log.error(ex);
        }
    }
}
