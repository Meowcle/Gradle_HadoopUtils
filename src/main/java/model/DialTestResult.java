package model;

/**
 * Created by Meowcle~ on 2017/7/27.
 */
public class DialTestResult {
    private int status;
    private String tableName;
    private long cost;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getCost() {
        return cost;
    }

    public void setCost(long cost) {
        this.cost = cost;
    }
}
