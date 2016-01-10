package com.alibaba.jstorm.task;

public class GlobalTaskId {
    private int task;

    private String stream;

    public GlobalTaskId(int task, String stream) {
        this.task = task;
        this.stream = stream;
    }

    public int getTask() {
        return task;
    }

    public String getStream() {
        return stream;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GlobalTaskId that = (GlobalTaskId) o;

        if (task != that.task) return false;
        if (stream != null ? !stream.equals(that.stream) : that.stream != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = task;
        result = 31 * result + (stream != null ? stream.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "GlobalTaskId{" +
                "task=" + task +
                ", stream='" + stream + '\'' +
                '}';
    }
}

