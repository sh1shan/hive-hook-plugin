package org.data.meta.hive.service.notification;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;

/**
 * @author chenchaolin
 * @create 2023-07-07
 */
public class JaasConfiguration extends Configuration {
    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        if ("KafkaClient".equals(name)) {
            AppConfigurationEntry krb5LoginModule = new AppConfigurationEntry(
                    "com.sun.security.auth.module.Krb5LoginModule",
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                    new HashMap<String, Object>() {{
                        put("useTicketCache", "false");
                        put("storeKey", "true");
                        put("serviceName", "kafka");
                        put("principal", "kafka/miniso-newpt3@MINISO-BDP-TEST.CN");
                        put("keyTab", "/etc/security/keytabs/kafka.service.keytab");
                        put("useKeyTab", "true");
                    }}
            );
            return new AppConfigurationEntry[]{krb5LoginModule};
        }
        return null;
    }
}
