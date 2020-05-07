package com.dummy;

import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

public class ConsumerIdConverter extends ClassicConverter {

    @Override
    public String convert(ILoggingEvent event) {
        if(AutoSubscribeTest.clientId != null) {
            return AutoSubscribeTest.clientId;
        }
        return null;
    }
}
