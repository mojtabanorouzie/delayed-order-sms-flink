package com.company.delayedordersms.model;

import java.io.Serializable;
import java.time.Instant;

/**
 * Represents an event that could not be processed and is routed to the dead letter queue.
 */
public class DeadLetterEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String originalPayload;
    private String errorReason;
    private String sourceTopic;
    private Instant occurredAt;

    public DeadLetterEvent() {
    }

    public DeadLetterEvent(String originalPayload, String errorReason, String sourceTopic, Instant occurredAt) {
        this.originalPayload = originalPayload;
        this.errorReason = errorReason;
        this.sourceTopic = sourceTopic;
        this.occurredAt = occurredAt;
    }

    public String getOriginalPayload() {
        return originalPayload;
    }

    public void setOriginalPayload(String originalPayload) {
        this.originalPayload = originalPayload;
    }

    public String getErrorReason() {
        return errorReason;
    }

    public void setErrorReason(String errorReason) {
        this.errorReason = errorReason;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }

    @Override
    public String toString() {
        return "DeadLetterEvent{" +
                "originalPayload='" + originalPayload + '\'' +
                ", errorReason='" + errorReason + '\'' +
                ", sourceTopic='" + sourceTopic + '\'' +
                ", occurredAt=" + occurredAt +
                '}';
    }
}