package HadoopUtils.infrastructure;

/**
 * Created by Meowcle~ on 2017/7/27.
 */
public enum DialTestStatus {
    SUCCESS(1, "success"),
    FAIL(0, "fail"),
    TIMEOUT(-1, "timeout");


    // 定义私有变量
    private Integer nCode;

    private String name;

    // 构造函数，枚举类型只能为私有
    DialTestStatus(Integer _nCode, String _name) {
        this.nCode = _nCode;
        this.name = _name;
    }

    public Integer getCode() {
        return this.nCode;
    }

    public String getName() {
        return this.name;
    }

    public static DialTestStatus valueOf(Integer code) {
        for (DialTestStatus status : DialTestStatus.values()){
            if (status.getCode().equals(code)){
                return status;
            }
        }
        return DialTestStatus.TIMEOUT;
    }

    public static String getNameByCode(Integer code) {
        for (DialTestStatus status : DialTestStatus.values()){
            if (status.getCode().equals(code)){
                return status.getName();
            }
        }
        return DialTestStatus.TIMEOUT.getName();
    }
}

