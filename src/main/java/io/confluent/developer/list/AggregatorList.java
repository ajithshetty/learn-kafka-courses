package io.confluent.developer.list;

import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.developer.avro.LoginRollupList;
import io.confluent.developer.avro.RollupList;
import org.apache.kafka.streams.kstream.Aggregator;

import java.util.List;

public class AggregatorList implements Aggregator<String, ElectronicOrder, RollupList> {

    @Override
    public RollupList apply(final String appId,
                            final ElectronicOrder loginEvent,
                            final RollupList rollupList) {
        final String userId = loginEvent.getUserId();
        final List<String> allLogins = rollupList.getUsersList();
        if(!allLogins.contains(userId)) {
            allLogins.add(userId);
        }
        return rollupList;
    }
}