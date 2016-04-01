package io.confluent.strata.utils;

import com.google.common.collect.Maps;
import kafka.tools.DefaultMessageFormatter;

import java.util.Properties;


/**
 * Created by jadler on 4/1/16.
 */
public class BetterDefaultMessageFormatter extends DefaultMessageFormatter {

    @Override
    public void init(Properties props) {
        super.init(props);
        keyDeserializer().get().configure(Maps.fromProperties(props), true);
        valueDeserializer().get().configure(Maps.fromProperties(props), false);
    }
}
