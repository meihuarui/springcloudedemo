package com.atguigu.reflecttest.enumtest;

public enum  GradeEnum {
    A("100-90"){
        @Override
        public String localValue() {
            return "优";
        }
    },
    B("90-80"){
        @Override
        public String localValue() {
            return "好";
        }
    },
    C("80-70"){
        @Override
        public String localValue() {
            return "良";
        }
    },
    D("70-60"){
        @Override
        public String localValue() {
            return "及格";
        }
    },
    E("60-0"){
        @Override
        public String localValue() {
            return "不及格";
        }
    };

    private String value;//封装每一个对象对应到分数

    private GradeEnum(String value){
        this.value=value;
    }

    public String getValue() {
        return value;
    }

    public abstract String localValue();

    public static void main(String[] args) {
        String value = GradeEnum.B.getValue();
        System.out.println(value);
        String s = GradeEnum.B.localValue();
        System.out.println(s);
        String name = GradeEnum.B.name();
        System.out.println(name);
    }
}
