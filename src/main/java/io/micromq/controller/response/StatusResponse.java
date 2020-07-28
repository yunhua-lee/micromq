package io.micromq.controller.response;

public class StatusResponse {
    private String serverName;
    private String role;
    private Boolean supportPull;
    private Boolean supportPublish;
    private Boolean isActive;

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public Boolean getSupportPull() {
        return supportPull;
    }

    public void setSupportPull(Boolean supportPull) {
        this.supportPull = supportPull;
    }

    public Boolean getSupportPublish() {
        return supportPublish;
    }

    public void setSupportPublish(Boolean supportPublish) {
        this.supportPublish = supportPublish;
    }

    public Boolean getActive() {
        return isActive;
    }

    public void setActive(Boolean active) {
        isActive = active;
    }
}
