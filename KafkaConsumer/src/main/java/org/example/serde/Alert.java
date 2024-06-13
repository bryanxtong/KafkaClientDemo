package org.example.serde;


import java.io.Serializable;

public class Alert implements Serializable {
    private int alertId;
    private String stageId;
    private String alertLevel;
    private String alertMessage;
    public Alert(){
    }
    public Alert(int alertId, String stageId,
                 String alertLevel, String alertMessage) {
        this.alertId = alertId;
        this.stageId = stageId;
        this.alertLevel = alertLevel;
        this.alertMessage = alertMessage;
    }

    public int getAlertId() {
        return alertId;
    }

    public String getStageId() {
        return stageId;
    }

    public void setStageId(String stageId) {
        this.stageId = stageId;
    }

    public String getAlertLevel() {
        return alertLevel;
    }

    public String getAlertMessage() {
        return alertMessage;
    }

    public void setAlertId(int alertId) {
        this.alertId = alertId;
    }

    public void setAlertLevel(String alertLevel) {
        this.alertLevel = alertLevel;
    }

    public void setAlertMessage(String alertMessage) {
        this.alertMessage = alertMessage;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "alertId=" + alertId +
                ", stageId='" + stageId + '\'' +
                ", alertLevel='" + alertLevel + '\'' +
                ", alertMessage='" + alertMessage + '\'' +
                '}';
    }
}

