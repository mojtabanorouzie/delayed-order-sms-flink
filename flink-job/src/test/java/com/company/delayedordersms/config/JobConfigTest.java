package com.company.delayedordersms.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JobConfigTest {

    @Test
    void shouldUseDefaultValues() {
        DelayedOrderSmsJobConfig config = DelayedOrderSmsJobConfig.fromArgs(new String[]{});
        assertThat(config.parallelism()).isEqualTo(1);
        assertThat(config.stateTtlDays()).isEqualTo(7);
        assertThat(config.consumerGroupId()).isEqualTo("delayed-order-sms-flink");
        assertThat(config.ordersTopic()).isEqualTo("Orders");
        assertThat(config.smsCommandsTopic()).isEqualTo("sms-commands");
    }

    @Test
    void shouldParseParallelismArg() {
        DelayedOrderSmsJobConfig config = DelayedOrderSmsJobConfig.fromArgs(
                new String[]{"--parallelism", "4"});
        assertThat(config.parallelism()).isEqualTo(4);
    }

    @Test
    void shouldParseStateTtlDaysArg() {
        DelayedOrderSmsJobConfig config = DelayedOrderSmsJobConfig.fromArgs(
                new String[]{"--state.ttl.days", "14"});
        assertThat(config.stateTtlDays()).isEqualTo(14);
    }

    @Test
    void shouldParseAllArgs() {
        DelayedOrderSmsJobConfig config = DelayedOrderSmsJobConfig.fromArgs(new String[]{
                "--parallelism", "4",
                "--state.ttl.days", "14",
                "--consumer.group.id", "my-group",
                "--orders.topic", "MyOrders"
        });
        assertThat(config.parallelism()).isEqualTo(4);
        assertThat(config.stateTtlDays()).isEqualTo(14);
        assertThat(config.consumerGroupId()).isEqualTo("my-group");
        assertThat(config.ordersTopic()).isEqualTo("MyOrders");
    }
}
