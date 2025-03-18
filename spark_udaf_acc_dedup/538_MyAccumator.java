//https://raw.githubusercontent.com/Yanbuc/mysparkproject/69a44a9591b5099b78bb375ac189a77cbad94631/src/main/java/accu/MyAccumator.java
package accu;

import constant.Constants;
import org.apache.spark.util.AccumulatorV2;
import utils.StringUtils;


public class MyAccumator  extends AccumulatorV2<String,String> {

    private String val= Constants.TIME_PERIOD_1s_3s+"=0"+"|"
                        +Constants.TIME_PERIOD_4s_6s+"=0"+"|"
                        +Constants.TIME_PERIOD_7s_9s+"=0"+"|"
                        +Constants.TIME_PERIOD_10s_30s+"=0"+"|"
                        +Constants.TIME_PERIOD_30s_60s+"=0"+"|"
                        +Constants.TIME_PERIOD_1m_3m+"=0"+"|"
                        +Constants.TIME_PERIOD_3m_10m+"=0"+"|"
                        +Constants.TIME_PERIOD_10m_30m+"=0"+"|"
                        +Constants.TIME_PERIOD_30m+"=0"+"|"
                        +Constants.STEP_PERIOD_1_3+"=0"+"|"
                        +Constants.STEP_PERIOD_4_6+"=0"+"|"
                        +Constants.STEP_PERIOD_7_9+"=0"+"|"
                        +Constants.STEP_PERIOD_10_30+"=0"+"|"
                        +Constants.STEP_PERIOD_30_60+"=0"+"|"
                        +Constants.STEP_PERIOD_60+"=0"+"|"
                        +Constants.SESSION_COUNT+"=0";

    @Override
    public AccumulatorV2<String, String> copy() {
        MyAccumator a= new MyAccumator();
        a.val=val;
        return a;
    }

    @Override
    public boolean isZero() {
        String val=Constants.TIME_PERIOD_1s_3s+"=0"+"|"
                +Constants.TIME_PERIOD_4s_6s+"=0"+"|"
                +Constants.TIME_PERIOD_7s_9s+"=0"+"|"
                +Constants.TIME_PERIOD_10s_30s+"=0"+"|"
                +Constants.TIME_PERIOD_30s_60s+"=0"+"|"
                +Constants.TIME_PERIOD_1m_3m+"=0"+"|"
                +Constants.TIME_PERIOD_3m_10m+"=0"+"|"
                +Constants.TIME_PERIOD_10m_30m+"=0"+"|"
                +Constants.TIME_PERIOD_30m+"=0"+"|"
                +Constants.STEP_PERIOD_1_3+"=0"+"|"
                +Constants.STEP_PERIOD_4_6+"=0"+"|"
                +Constants.STEP_PERIOD_7_9+"=0"+"|"
                +Constants.STEP_PERIOD_10_30+"=0"+"|"
                +Constants.STEP_PERIOD_30_60+"=0"+"|"
                +Constants.STEP_PERIOD_60+"=0"+"|"
                +Constants.SESSION_COUNT+"=0";
        return this.val != "";
    }



    @Override
    public void reset() {
        val= Constants.TIME_PERIOD_1s_3s+"=0"+"|"
                +Constants.TIME_PERIOD_4s_6s+"=0"+"|"
                +Constants.TIME_PERIOD_7s_9s+"=0"+"|"
                +Constants.TIME_PERIOD_10s_30s+"=0"+"|"
                +Constants.TIME_PERIOD_30s_60s+"=0"+"|"
                +Constants.TIME_PERIOD_1m_3m+"=0"+"|"
                +Constants.TIME_PERIOD_3m_10m+"=0"+"|"
                +Constants.TIME_PERIOD_10m_30m+"=0"+"|"
                +Constants.TIME_PERIOD_30m+"=0"+"|"
                +Constants.STEP_PERIOD_1_3+"=0"+"|"
                +Constants.STEP_PERIOD_4_6+"=0"+"|"
                +Constants.STEP_PERIOD_7_9+"=0"+"|"
                +Constants.STEP_PERIOD_10_30+"=0"+"|"
                +Constants.STEP_PERIOD_30_60+"=0"+"|"
                +Constants.STEP_PERIOD_60+"=0"+"|"
                +Constants.SESSION_COUNT+"=0";
    }

    @Override
    public void add(String v) {
        /*
        int  count= Integer.valueOf(StringUtils.getFieldFromConcatString(val,"\\|",v));
        count+=1;
        this.val= StringUtils.setFieldInConcatString(this.val,"\\|",v,String.valueOf(count));
*/
        String fieldsValue= StringUtils.getFieldFromConcatString(this.val,"\\|",v);
        if(fieldsValue == null){
            return ;
        }
        String upDateVal =String.valueOf(Integer.valueOf(fieldsValue)+1);
        String rs =StringUtils.setFieldInConcatString(this.val,"\\|",v,upDateVal);
        this.val=rs;

    }

    @Override
    public void merge(AccumulatorV2<String, String> other) {
        String[] arr = this.val.split("\\|");
        String[] oarr=other.value().split("\\|");
        for(int i=0;i< arr.length;i++){
            String arrKey=arr[i].split("=")[0];
            int arrV=Integer.valueOf(arr[i].split("=")[1]);
            int oarrV =Integer.valueOf(oarr[i].split("=")[1]);
            arr[i]=arrKey+"="+String.valueOf(arrV+oarrV);
        }
        StringBuffer b = new StringBuffer();
        for(int i=0;i<arr.length;i++){
            b.append(arr[i]+"|");
        }
        String re =b.toString();
        String ret=re.substring(0,re.length()-1);
        this.val=ret;
    }

    @Override
    public String value() {
        return this.val;
    }


}
