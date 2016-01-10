package com.alibaba.jstorm.task;

import java.util.ArrayList;
import java.util.List;

public class SupervisorWorker implements Comparable {
    private String supervisorId;

    private List<WorkerTask> workerTasksList = new ArrayList<WorkerTask>();

    private int priority = 0;

    public SupervisorWorker(String supervisorId, List<WorkerTask> workerTasksList) {
        this.supervisorId = supervisorId;
        this.workerTasksList = workerTasksList;
    }

    public String getSupervisorId() {
        return supervisorId;
    }

    public List<WorkerTask> getWorkerTasksList() {
        return workerTasksList;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof SupervisorWorker) {
            if (((SupervisorWorker) o).priority == this.priority) {
                return ((SupervisorWorker) o).supervisorId.compareTo(this.supervisorId);
            } else {
                return ((SupervisorWorker) o).priority - this.priority;
            }
        }
        return 0;
    }
}
