package com.company.delayedordersms.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JobConfigTest {

    @Test
    void shouldUseDefaultValues() {
        JobConfig config = JobConfig.fromArgs(new String[]{});
        assertThat(config.parallelism()).isEqualTo(1);
        assertThat(config.stateTtlDays()).isEqualTo(7);
    }

    @Test
    void shouldParseParallelismArg() {
        JobConfig config = JobConfig.fromArgs(new String[]{"--parallelism", "4"});
        assertThat(config.parallelism()).isEqualTo(4);
    }

    @Test
    void shouldParseStateTtlDaysArg() {
        JobConfig config = JobConfig.fromArgs(new String[]{"--state.ttl.days", "14"});
        assertThat(config.stateTtlDays()).isEqualTo(14);
    }

    @Test
    void shouldParseAllArgs() {
        JobConfig config = JobConfig.fromArgs(new String[]{
                "--parallelism", "4",
                "--state.ttl.days", "14"
        });
        assertThat(config.parallelism()).isEqualTo(4);
        assertThat(config.stateTtlDays()).isEqualTo(14);
    }
}