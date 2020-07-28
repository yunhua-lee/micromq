package io.micromq.queue;

public class MQQueue {
    private final String name;
    private boolean active;
    private int pullMode;
    private int saveMode;

    public MQQueue(String name){
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public int getPullMode() {
        return pullMode;
    }

    public void setPullMode(int pullMode) {
        this.pullMode = pullMode;
    }

    public int getSaveMode() {
        return saveMode;
    }

    public void setSaveMode(int saveMode) {
        this.saveMode = saveMode;
    }

}
