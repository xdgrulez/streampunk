package org.xdgrulez.streampunk.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xdgrulez.streampunk.consumer.Consumer;
import org.xdgrulez.streampunk.consumer.ConsumerStringProtobuf;
import org.xdgrulez.streampunk.helper.Helpers;

import java.util.Map;

public class HelpersTest {
    @BeforeEach
    public void setup() {
    }

    @AfterEach
    public void tearDown() {
    }

//    @Test
    public void testPID() {
        long pidLong = ProcessHandle.current().pid();
        System.out.println("Process ID: " + pidLong);
    }
}
