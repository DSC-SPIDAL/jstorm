package com.alibaba.jstorm.task;

import java.util.ArrayList;
import java.util.List;

public class WorkerTask implements Comparable {
    int priority = 0;
    private int port;
    private List<Integer> tasks = new ArrayList<Integer>();

    public WorkerTask(int port, List<Integer> tasks) {
        this.port = port;
        this.tasks = tasks;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }

    public int getPort() {
        return port;
    }

    public List<Integer> getTasks() {
        return tasks;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof WorkerTask) {
            if (this.priority == ((WorkerTask) o).priority) {
                return ((WorkerTask) o).port - this.port;
            } else {
                return ((WorkerTask) o).priority - this.priority;
            }
        }
        return 0;
    }
}
