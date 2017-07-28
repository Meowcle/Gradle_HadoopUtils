package HadoopUtils.utils;

import HadoopUtils.infrastructure.DialTestStatus;
import model.DialTestResult;
import model.TableName;
import net.sf.json.JSONObject;
import org.apache.hadoop.hbase.client.Result;
import net.sf.json.JSONArray;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.*;

/**
 * Created by Meowcle~ on 2017/7/24.
 */
public class DialTestUtils {
    private static SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss:SSS");

    public static JSONObject TableList() throws Exception {
        String[] tableNames;
        tableNames = HbaseUtils.listTableNames();
        ArrayList<TableName> tableList = new ArrayList<TableName>();
        TableName tableName;
        for (String table : tableNames) {
            tableName = new TableName();
            tableName.setTableName(table);
            tableList.add(tableName);
        }
        JSONArray ja = JSONArray.fromObject(tableList);
        JSONObject jo = new JSONObject();
        jo.put("table", ja);
        System.out.println(jo.toString());
        return jo;
    }

    public static JSONObject DialTestAll() throws Exception {
        String[] tableNames;
        tableNames = HbaseUtils.listTableNames();
        ArrayList<DialTestResult> resultList = new ArrayList<DialTestResult>();
        for (String table : tableNames) {
            resultList.add(DialAttempt(table));
        }
        JSONArray ja = JSONArray.fromObject(resultList);
        JSONObject jo = new JSONObject();
        jo.put("data", ja);
        System.out.println(jo.toString());
        return jo;
    }

    public static DialTestResult DialAttempt (String tableName) throws Exception {
        DialTestResult dtr = new DialTestResult();
        final ExecutorService exec = Executors.newFixedThreadPool(1);
        Callable<DialTestResult> task = new Callable<DialTestResult>() {
            @Override
            public DialTestResult call() throws Exception {
                return DialTest(tableName);
            }
        };
        try {
            Future<DialTestResult> future = exec.submit(task);
            dtr = future.get(1000 * 5, TimeUnit.MILLISECONDS); //set timeout to 5000ms
            dtr.setStatus(DialTestStatus.SUCCESS.getCode());
        } catch (TimeoutException e) {
            System.out.println("Dialtest failed for Timeout Exception.\n");
            dtr.setStatus(DialTestStatus.FAIL.getCode());
            dtr.setCost(-1);
        } catch (Exception e) {
            System.out.println("Dialtest failed.\n");
            dtr.setStatus(DialTestStatus.FAIL.getCode());
            dtr.setCost(-1);
        }
        finally {
            dtr.setTableName(tableName);
            System.out.println("===========================================\n");
            exec.shutdown();
            return dtr;

        }
    }

    public static long DialAttempt (String tableName, int type) throws Exception {
        if (type != 1 && type != 0)
            return -2;
        DialTestResult dtr = new DialTestResult();
        final ExecutorService exec = Executors.newFixedThreadPool(1);
        Callable<DialTestResult> task = new Callable<DialTestResult>() {
            @Override
            public DialTestResult call() throws Exception {
                return DialTest(tableName);
            }
        };
        try {
            Future<DialTestResult> future = exec.submit(task);
            dtr = future.get(1000 * 5, TimeUnit.MILLISECONDS); //set timeout to 5000ms
            dtr.setStatus(DialTestStatus.SUCCESS.getCode());
        } catch (TimeoutException e) {
            System.out.println("Dialtest failed for Timeout Exception.\n");
            dtr.setStatus(DialTestStatus.TIMEOUT.getCode());
            dtr.setCost(-1);
        } catch (Exception e) {
            System.out.println("Dialtest failed.\n");
            dtr.setStatus(DialTestStatus.FAIL.getCode());
            dtr.setCost(-1);
        }
        finally {
            dtr.setTableName(tableName);
            System.out.println("===========================================\n");
            exec.shutdown();
            if (type == 0) {
                System.out.println(dtr.getStatus());
                return dtr.getStatus();
            }
            else {
                System.out.println(dtr.getCost());
                return dtr.getCost();
            }
        }
    }

    public static DialTestResult DialTest(String tableName) throws Exception {
        DialTestResult dtr = new DialTestResult();
        ArrayList<String> rowkeyList;
        System.out.println("Dialtesting table " + tableName + " ……\n");
        rowkeyList = HbaseUtils.getRowkeyList(tableName);
        Date begin = new Date();
        Result rs = HbaseUtils.getResult(tableName, rowkeyList.get(0));
        Date end = new Date();
        long cost = end.getTime() - begin.getTime();
        dtr.setCost(cost);
        System.out.println("Dialtest finished. \n Time starts: "
                + formatter.format(begin) + "\n Time ends: " + formatter.format(end) + "\n Time spends: " + cost + "ms\n");
        return dtr;
    }


    public static void main(String[] args) throws Exception {
        TableList();
    }
}
