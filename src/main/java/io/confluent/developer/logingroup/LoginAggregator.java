package io.confluent.developer.logingroup;

import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.developer.avro.LoginRollup;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.kstream.Aggregator;

public class LoginAggregator implements Aggregator<String, ElectronicOrder, LoginRollup> {

    @Override
    public LoginRollup apply(final String appId,
                             final ElectronicOrder loginEvent,
                             final LoginRollup loginRollup) {
        final String userId = loginEvent.getUserId();
        final Map<String, Map<String, Long>> allLogins = loginRollup.getLoginByAppAndUser();
        final Map<String, Long> userLogins = allLogins.computeIfAbsent(appId, key -> new HashMap<>());
        userLogins.compute(userId, (k, v) -> v == null ? 1L : v + 1L);
        return loginRollup;
    }
}