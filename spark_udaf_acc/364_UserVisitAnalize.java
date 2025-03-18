//https://raw.githubusercontent.com/Yanbuc/mysparkproject/69a44a9591b5099b78bb375ac189a77cbad94631/src/main/java/session/UserVisitAnalize.java
package session;

import accu.MyAccumator;
import com.alibaba.fastjson.JSONObject;
import constant.Constants;
import dao.*;
import domain.*;
import factory.Daofactory;
import mockdata.MockData;
import mockdata.MocksData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;
import scala.collection.immutable.Stream;
import utils.*;

import java.util.*;
import java.util.zip.CheckedInputStream;

public class UserVisitAnalize {

    // 生成测试数据
    public static void mockData(JavaSparkContext jsc,SQLContext sqlContext){
        MockData.mock(jsc,sqlContext);
    }

    // 获得任务的过滤参数 转化为key val的形式
    public static JSONObject getTaskParmas(int taskId){
        ITask iTask= Daofactory.getTaskImpl();
        Task taskById = iTask.getTaskById(1);
        JSONObject params= ParamsUtils.strToJsonObject(taskById.getTaskParam());
        return params;
    }


    // 按照传入的条件对用户行为数据进行过滤
    public static JavaRDD<Row> getRangeSession(SparkSession sc, JSONObject params){
        String condition=SqlUtils.produceSql(params);
        // 这里注意的就是 spark的sql 后面的条件 字符串比较的视乎
        // 记得后面加上引号。
        String sql="select * from user_visit_action where "+condition;
        Dataset<Row> data = sc.sql(sql);
        return data.javaRDD();
    }

    // 将数据转化成为 sessionId 为key 的键值对
    public static JavaPairRDD<String, Row> getDataKeyBySessionId(JavaRDD<Row> data){
        JavaPairRDD<String, Row> retn = data.mapToPair((row) -> {
            String sessionId = row.getString(2);
            String str = row.toString();
            return new Tuple2<String, Row>(sessionId, row);
        });
        return retn;
    }

    // 将每个session的数据聚合起来
    public static  JavaPairRDD<Long,String> dealDataToLine(JavaPairRDD<String, Iterable<Row>> data){
        JavaPairRDD<Long,String> lineData=data.mapToPair((da)->{
            String session_id=da._1;
            Iterable<Row> rws = da._2;
            Iterator<Row> iterator = rws.iterator();
            Long userId=null;
            StringBuffer keyWords=new StringBuffer();
            HashSet<String> kwds=new HashSet<String>();
            HashSet<Long> clickCategoryIds=new HashSet<Long>();
            HashSet<String> payCategoryIds=new HashSet<String>();
            HashSet<String> orderCategoryIds=new HashSet<String>();
            String startActionTime="";
            String endActionTime="";
            int stepLength=0;

            while (iterator.hasNext()){
                stepLength+=1;
                Row tmp=iterator.next();
                if(startActionTime == "" || DateUtils.before(tmp.getString(4),startActionTime)){
                    startActionTime=tmp.getString(4);
                }
                if(endActionTime=="" || DateUtils.after(tmp.getString(4),endActionTime)){
                    endActionTime=tmp.getString(4);
                }
                if(userId==null){
                    userId=tmp.getLong(1);
                }
                String kw=tmp.getString(5);
                if(kw!=null && kw!=""){
                    kwds.add(kw);
                }
                // 因为这里是 空值 在取出的时候 会由误差
                Object clickCategoryId=tmp.get(6);
                if(clickCategoryId !=null){
                    clickCategoryIds.add((Long) clickCategoryId);
                }
                Object orderCatgeoryId=tmp.get(8);
                if(orderCatgeoryId!=null){
                    orderCategoryIds.add((String) orderCatgeoryId);
                }
                Object payCategoryId=tmp.get(10);
                if(payCategoryId!=null){
                    payCategoryIds.add((String) payCategoryId);
                }
            }
            for(String kw:kwds){
                keyWords.append(kw+",");
            }
            String keyWord=keyWords.length()==0 ? "":keyWords.substring(0,keyWords.length()-1);
            StringBuffer cli=new StringBuffer();
            for(Long ids:clickCategoryIds){
                cli.append(ids+",");
            }
            String tcli=cli.length()==0?"":cli.substring(0,cli.length()-1);
            StringBuffer oids=new StringBuffer();
            for(String oid:orderCategoryIds){
                oids.append(oid+",");
            }
            String realOids= oids.length()==0 ?"":oids.substring(0,oids.length()-1);
            StringBuffer pids=new StringBuffer();
            for(String pid:payCategoryIds){
                pids.append(pid+",");
            }
            String realPids=pids.length()==0 ? "": pids.substring(0,pids.length()-1);
            StringBuffer li=new StringBuffer();
            int grep=DateUtils.minus(endActionTime,startActionTime);
            li.append(Constants.SESSION_ID+"="+session_id+"|");
            li.append(Constants.SEARCH_KEY_WORD+"="+keyWord+"|");
            li.append(Constants.SESSION_TIME+"="+grep+"|");
            li.append(Constants.STEP_LENGTH+"="+stepLength+"|");
            li.append(Constants.CLICK_CATEGORY_IDS+"="+tcli+"|");
            li.append(Constants.START_TIME+"="+startActionTime+"|");
            li.append(Constants.ORDER_CATEGORY_IDS+"="+realOids+"|");
            li.append(Constants.PAY_CATEGORY_IDS+"="+realPids+"|");
            String line=li.toString();
            if(line.endsWith("|")){
                line=line.substring(0,line.length()-1);
            }
            return new Tuple2<Long,String>(userId,line);
        });
        return lineData;
    }

    // 获得用户信息
    public static JavaPairRDD<Long, String> getUserInfo(SparkSession sc){
        String sql="select * from user_info";
        Dataset<Row> dataset = sc.sql(sql);
        dataset.show();
        JavaRDD<Row> rowJavaRDD = dataset.javaRDD();
        JavaPairRDD<Long, String> dataKeyByUserId = rowJavaRDD.mapToPair((row) -> {
            Long userId = row.getLong(0);
            String userName=row.getString(1);
            String name=row.getString(2);
            int age=row.getInt(3);
            String professional=row.getString(4);
            String city=row.getString(5);
            String sex=row.getString(6);
            String line=Constants.FIELD_USERNAME+"="+userName+"|"
                        +Constants.FIELD_NAME+"="+name+"|"
                        +Constants.FILED_AGE+"="+age+"|"
                        +Constants.FIELD_PROFRESSION+"="+professional+"|"
                        +Constants.FIELD_CITY+"="+city+"|"
                        +Constants.FIELD_SEX+"="+sex;
            return new Tuple2<Long, String>(userId, line);
        });
        return dataKeyByUserId;
    }

    public static  JavaPairRDD<Long, Tuple2<String, String>>   filterUserSessionDataByParams(JavaPairRDD<Long, Tuple2<String, String>> data, JSONObject params, AccumulatorV2<String,String> accumator){
         Integer startAge=params.getInteger(Constants.START_AGE);
         Integer endAge=params.getInteger(Constants.END_AGE);
         String city=params.getString(Constants.CITY);
         String sex=params.getString(Constants.SEX);
         String professional=params.getString(Constants.FIELD_PROFRESSION);
         String searchKeyWord=params.getString(Constants.SEARCH_KEY_WORD);
         final String pms=Constants.START_AGE+"="+(startAge==null? "":startAge)+"|"
                    +Constants.END_AGE+"="+(endAge==null?"":endAge)+"|"
                    +Constants.CITY+"="+(city==null?"":city)+"|"
                    +Constants.SEX+"="+(sex==null?"":sex)+"|"
                    +Constants.PROFESSION+"="+(professional==null?"":professional)+"|"
                    +Constants.SEARCH_KEY_WORD+"="+(searchKeyWord==null?"":searchKeyWord);
         String pro=StringUtils.getFieldFromConcatString(pms,"\\|",Constants.PROFESSION);


        JavaPairRDD<Long, Tuple2<String, String>> filterData=data.filter((row)->{
             String userInfo=row._2._1;
             String sessionInfo=row._2._2;
             String psearchKeyWords=StringUtils.getFieldFromConcatString(pms,"\\|",Constants.SEARCH_KEY_WORD);
             if(!StringUtils.whetherIsInStr(psearchKeyWords,StringUtils.getFieldFromConcatString(sessionInfo,"\\|",Constants.SEARCH_KEY_WORD))){
                 return false;
             }
             Integer pstartAge=Integer.valueOf(StringUtils.getFieldFromConcatString(pms,"\\|",Constants.START_AGE));
             Integer pendAge=Integer.valueOf(StringUtils.getFieldFromConcatString(pms,"\\|",Constants.END_AGE));
             Integer uage=Integer.valueOf(StringUtils.getFieldFromConcatString(userInfo,"\\|",Constants.FILED_AGE));
             if(!StringUtils.betweenIn(uage,pstartAge,pendAge)){
                 return false;
             }
             String pcity=StringUtils.getFieldFromConcatString(pms,"\\|",Constants.CITY);
             String ucity=StringUtils.getFieldFromConcatString(userInfo,"\\|",Constants.CITY);
             if(pcity!=null && !pcity.equals(ucity)  ){
                return false;
             }
             String pprofession=StringUtils.getFieldFromConcatString(pms,"\\|",Constants.PROFESSION);
             String uprofession=StringUtils.getFieldFromConcatString(userInfo,"\\|",Constants.PROFESSION);
             if(pprofession!=null && !pprofession.equals(uprofession)){
                 return false;
             }
             String psex=StringUtils.getFieldFromConcatString(pms,"\\|",Constants.SEX);
             String usex=StringUtils.getFieldFromConcatString(pms,"\\|",Constants.SEX);
             if(psex!=null && !usex.equals(psex)){
                 return false;
             }
             accumator.add(Constants.SESSION_COUNT);
             int sessionTime=Integer.valueOf(StringUtils.getFieldFromConcatString(sessionInfo,"\\|", Constants.SESSION_TIME));
             int stepLength=Integer.valueOf(StringUtils.getFieldFromConcatString(sessionInfo,"\\|",Constants.STEP_LENGTH));

             addSession(sessionTime,accumator);
             addStepLength(stepLength,accumator);
             return true;
         });
        filterData=filterData.cache();
        filterData.count();
        return filterData;
    }

    // 将步长放入累加器之中。
    private static  void addSession(int sessionTime,AccumulatorV2<String,String> accumator){
        if(sessionTime>=0 && sessionTime <=3){
            accumator.add(Constants.TIME_PERIOD_1s_3s);
        }else if(sessionTime>3 &&sessionTime <=6){
            accumator.add(Constants.TIME_PERIOD_4s_6s);
        }else if(sessionTime>6 &&sessionTime <=9){
            accumator.add(Constants.TIME_PERIOD_7s_9s);
        }else if(sessionTime>10 &&sessionTime <=30){
            accumator.add(Constants.TIME_PERIOD_10s_30s);
        }else  if(sessionTime>30&& sessionTime<=60){
            accumator.add(Constants.TIME_PERIOD_30s_60s);
        }else if(sessionTime>60&& sessionTime<=180){
            accumator.add(Constants.TIME_PERIOD_1m_3m);
        }else if(sessionTime>180&& sessionTime<=600){
            accumator.add(Constants.TIME_PERIOD_3m_10m);
        }else if(sessionTime>600 && sessionTime<=1800){
            accumator.add(Constants.TIME_PERIOD_10m_30m);
        }else  if(sessionTime>1800){
            accumator.add(Constants.TIME_PERIOD_30m);
        }
    }

    private static void addStepLength(int stepLength,AccumulatorV2<String,String> accumator){
        if(stepLength>0 && stepLength<=3){
            accumator.add(Constants.STEP_PERIOD_1_3);
        }else if(stepLength>3&& stepLength<=6){
            accumator.add(Constants.STEP_PERIOD_4_6);
        }else if(stepLength>6 && stepLength<=9){
            accumator.add(Constants.STEP_PERIOD_7_9);
        }else if(stepLength>9 && stepLength<=30){
            accumator.add(Constants.STEP_PERIOD_10_30);
        }else  if(stepLength>30 && stepLength<=60){
            accumator.add(Constants.STEP_PERIOD_30_60);
        }else if(stepLength>60){
            accumator.add(Constants.STEP_PERIOD_60);
        }
    }

    // 计算访问比例 并且存入数据库
    public static void calculateSessionAndStepAndInsert(int taskId,AccumulatorV2<String,String> accumulator){
        String line=accumulator.value();
        SessionAggrStat data=new SessionAggrStat();
        String[] fields=line.split("\\|");
        long[] val=new long[fields.length];
        for(int i=0;i<fields.length;i++){
            val[i]=Long.valueOf(fields[i].split("=")[1]);
        }
        int last=fields.length-1;
        double[] retn=new double[fields.length-1];
        for(int i=0;i<retn.length;i++){
            retn[i]=((double) val[i])/(double) val[last];
            retn[i]=NumberUtils.formatDouble(retn[i],2);

        }
        SessionAggrStat d=new SessionAggrStat();
        d.setSessionCount(val[last]);
        d.setTaskId(taskId);
        int i=0;
        d.setSession1_3(retn[i++]);
        d.setSession4_6(retn[i++]);
        d.setSession7_9(retn[i++]);
        d.setSession10_30(retn[i++]);
        d.setSession30_60(retn[i++]);
        d.setSession1m_3m(retn[i++]);
        d.setSession3m_10m(retn[i++]);
        d.setSession10m_30m(retn[i++]);
        d.setSession30m(retn[i++]);
        d.setStep1_3(retn[i++]);
        d.setStep4_6(retn[i++]);
        d.setStep7_9(retn[i++]);
        d.setStep10_30(retn[i++]);
        d.setStep30_60(retn[i]);
        d.setStep60(retn[i]);
        ISessionAggrStat iSessionAggrStat=Daofactory.getSessionAggrStat();
        iSessionAggrStat.insert(d);
    }

    // 按时间比例实现session数据的抽取
    public static JavaRDD< String> randExtractSession(JavaSparkContext jsc,JavaPairRDD<Long, Tuple2<String, String>>  data,long
                                          sessionCount,int extractNum){
        // 获得抽取的比例
         double radio;
        if(sessionCount>extractNum){
            radio=NumberUtils.formatDouble((double) extractNum /sessionCount,2);
        }else{
            radio=NumberUtils.formatDouble((double) sessionCount/extractNum,2);
        }
        // 将抽取比例进行广播
        Broadcast<Double> broadcast = jsc.broadcast(radio);

        JavaPairRDD<String, String> keyByDHourOfDay = data.mapToPair((row) -> {
            String sessionInfo = row._2._2;
            String startTime = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.START_TIME);
            Date st = DateUtils.parseTime(startTime);
            Calendar c = Calendar.getInstance();
            c.setTime(st);
            String key = c.get(Calendar.YEAR) + "_" + (c.get(Calendar.MONTH) + 1) + "_" + c.get(Calendar.DAY_OF_MONTH) + "_" + c.get(Calendar.HOUR_OF_DAY) ;
            return new Tuple2<String, String>(key, sessionInfo);
        });
        JavaPairRDD<String, Iterable<String>> dayHourData = keyByDHourOfDay.groupByKey();
        JavaRDD<String> retnData = dayHourData.flatMap((line) -> {
            List<String> retn = new ArrayList<String>();
            Iterator<String> tmpData = line._2.iterator();
            List<String> tmp = new ArrayList<String>();
            while (tmpData.hasNext()) {
                tmp.add(tmpData.next());
            }
            double radi = broadcast.value();
            int count = (int) Math.round(radi * tmp.size());
            Random rand = new Random();
            for (int i = 0; i < count; i++) {
                int j = rand.nextInt(tmp.size());
                while (retn.contains(tmp.get(j))) {
                    j = rand.nextInt(tmp.size());
                }
                retn.add(tmp.get(j));
            }
            return retn.iterator();
        });
        return retnData;
    }
    // 将数据抽取的数据插入到mysql之中。 还有一种实现的思路 就是使用foreach 的方式来实现。
    public static void insertRandomExtractData(JavaRDD<String> data,int taskId){
        List<String> collect = data.collect();
        List<SessionRandomExtract> insertData=new ArrayList<SessionRandomExtract>();
        for(String line:collect){
            String startTime=StringUtils.getFieldFromConcatString(line,"\\|",Constants.START_TIME);
            String sessionId=StringUtils.getFieldFromConcatString(line,"\\|",Constants.SESSION_ID);
            String searchKeyWords=StringUtils.getFieldFromConcatString(line,"\\|",Constants.SEARCH_KEY_WORD);
            String clickCategoryIds=StringUtils.getFieldFromConcatString(line,"\\|",Constants.CLICK_CATEGORY_IDS);
            SessionRandomExtract s=new SessionRandomExtract(taskId,sessionId,startTime,searchKeyWords,clickCategoryIds);
            insertData.add(s);
        }
        ISessionRandomExtract iSessionRandomExtract=Daofactory.getSessionRandomExtract();
        iSessionRandomExtract.insert(insertData);
    }

    //获得购买 点击 下单 前10 的品类
    public static List<Tuple2<Category, Long>> getTop10CategoryByPayAdnOrderAndClick(JavaPairRDD<Long, Tuple2<String, String>>  data,int topn){
        JavaRDD<String> sessionData= data.map((line)->{
             String sessionInfo=line._2._2;
             return sessionInfo;
         });
        JavaRDD<String> filterSessionData=sessionData.filter((line)->{
           String clickCategoryIds=StringUtils.getFieldFromConcatString(line,"\\|",Constants.CLICK_CATEGORY_IDS);
           String orderCatgeoryIds=StringUtils.getFieldFromConcatString(line,"\\|",Constants.ORDER_CATEGORY_IDS);
           String payCategoryIds=StringUtils.getFieldFromConcatString(line,"\\|",Constants.PAY_CATEGORY_IDS);
           if(clickCategoryIds==null&& orderCatgeoryIds==null && payCategoryIds==null){
               return false;
           }
           return true;
        });
        JavaPairRDD<Long, Integer> sessionDataKeyByCategoryId = filterSessionData.flatMapToPair((line) -> {
            List<Tuple2<Long, Integer>> params = new ArrayList<Tuple2<Long, Integer>>();
            HashSet<Long> ids = new HashSet<Long>();
            String clickCategoryIds = StringUtils.getFieldFromConcatString(line, "\\|", Constants.CLICK_CATEGORY_IDS);
            String orderCatgeoryIds = StringUtils.getFieldFromConcatString(line, "\\|", Constants.ORDER_CATEGORY_IDS);
            String payCategoryIds = StringUtils.getFieldFromConcatString(line, "\\|", Constants.PAY_CATEGORY_IDS);
            String[] tmp;
            if (clickCategoryIds != null) {
                tmp = clickCategoryIds.split(",");
                for (String t : tmp) {
                    ids.add(Long.valueOf(t));
                }
            }
            if (orderCatgeoryIds != null) {
                tmp = orderCatgeoryIds.split(",");
                for (String t : tmp) {
                    ids.add(Long.valueOf(t));
                }
            }
            if (payCategoryIds != null) {
                tmp = payCategoryIds.split(",");
                for (String t : tmp) {
                    ids.add(Long.valueOf(t));
                }
            }
            for (Long id : ids) {
                Tuple2<Long, Integer> tt = new Tuple2<Long, Integer>(id, 1);
                params.add(tt);
            }
            return params.iterator();
        });
        JavaPairRDD<Long, Integer> finalSessionDateWithCategoryId = sessionDataKeyByCategoryId.reduceByKey((Integer a, Integer b) -> {
            return a + b;
        });

        JavaPairRDD<Long, Integer> everyPayIdCount=calculatePayId(sessionData);
        JavaPairRDD<Long,Integer> everyOrderIdCount=calculateOrderId(sessionData);
        JavaPairRDD<Long,Integer> everyClickIdCount=calculateClickId(sessionData);
        JavaPairRDD<Long, String> fullCatgeoryIdData=getFullJoinCategoryIdData(finalSessionDateWithCategoryId,everyPayIdCount,everyOrderIdCount,everyClickIdCount);
        JavaPairRDD<Category, Long> transFromData = fullCatgeoryIdData.mapToPair((line) -> {
            long categotyId = line._1;
            String tmpP = StringUtils.getFieldFromConcatString(line._2, "\\|", Constants.PAY_CATGEORY_COUNT);
            long payCategoryId = tmpP == null ? 0L : Long.valueOf(tmpP);
            String tmpO = StringUtils.getFieldFromConcatString(line._2, "\\|", Constants.ORDER_CATEGORY_COUNT);
            long orderCategoryId = tmpO == null ? 0L : Long.valueOf(tmpO);
            String tmpC = StringUtils.getFieldFromConcatString(line._2, "\\|", Constants.CLICK_CATEGORY_COUNT);
            long clickCategoryCount = tmpC == null ? 0L : Long.valueOf(tmpC);
            return new Tuple2<Category, Long>(new Category(categotyId, payCategoryId, orderCategoryId, clickCategoryCount), categotyId);
        });

        JavaPairRDD<Category, Long> sortedData = transFromData.sortByKey(false);
        List<Tuple2<Category, Long>> topNData = sortedData.take(topn);
        return  topNData;
    }

    private static JavaPairRDD<Long, Integer> calculatePayId( JavaRDD<String> sessionData){
       JavaRDD<String> filterSessionData=sessionData.filter((line)->{
            String payIds = StringUtils.getFieldFromConcatString(line, "\\|", Constants.PAY_CATEGORY_IDS);
            if(payIds==null){
                return false;
            }
            return true;
        });
        JavaPairRDD<Long, Integer> payIdsData = filterSessionData.flatMapToPair((line) -> {
            List<Tuple2<Long, Integer>> params = new ArrayList<Tuple2<Long, Integer>>();
            String payIds = StringUtils.getFieldFromConcatString(line, "\\|", Constants.PAY_CATEGORY_IDS);
            String[] fields = payIds.split(",");
            for (String id : fields) {
                Tuple2<Long, Integer> tmp = new Tuple2<Long, Integer>(Long.valueOf(id), 1);
                params.add(tmp);
            }
            return params.iterator();
        });
        JavaPairRDD<Long, Integer> payIdsCountData = payIdsData.reduceByKey((Integer one, Integer two) -> {
            return one + two;
        });
        return payIdsCountData;
    }

    private static  JavaPairRDD<Long,Integer> calculateOrderId(JavaRDD<String> sessionData){
        JavaRDD<String> filterSessionData = sessionData.filter((line) -> {
            String orderIds = StringUtils.getFieldFromConcatString(line, "\\|", Constants.ORDER_CATEGORY_IDS);
            if (orderIds == null) {
                return false;
            }
            return true;
        });
        JavaPairRDD<Long, Integer> orderIdsData = filterSessionData.flatMapToPair((line) -> {
            List<Tuple2<Long, Integer>> params = new ArrayList<Tuple2<Long, Integer>>();
            String orderIds = StringUtils.getFieldFromConcatString(line, "\\|", Constants.ORDER_CATEGORY_IDS);
            String[] fields = orderIds.split(",");
            for (String id : fields) {
                Tuple2<Long, Integer> tmp = new Tuple2<Long, Integer>(Long.valueOf(id), 1);
                params.add(tmp);
            }
            return params.iterator();
        });
        JavaPairRDD<Long, Integer> everyOrderIdCount = orderIdsData.reduceByKey((Integer one, Integer two) -> {
            return one + two;
        });
        return everyOrderIdCount;
    }

    private static JavaPairRDD<Long,Integer> calculateClickId(JavaRDD<String> sessionData){
        JavaRDD<String> filterSessionData = sessionData.filter((line) -> {
            String clickIds = StringUtils.getFieldFromConcatString(line, "\\|", Constants.CLICK_CATEGORY_IDS);
            if (clickIds == null) {
                return false;
            }
            return true;
        });
        JavaPairRDD<Long, Integer> clickIdCount = filterSessionData.flatMapToPair((line) -> {
            List<Tuple2<Long, Integer>> params = new ArrayList<Tuple2<Long, Integer>>();
            String clickCategoryIds = StringUtils.getFieldFromConcatString(line, "\\|", Constants.CLICK_CATEGORY_IDS);
            String[] fields = clickCategoryIds.split(",");
            for (String id : fields) {
                Tuple2<Long, Integer> tmp = new Tuple2<Long, Integer>(Long.valueOf(id), 1);
                params.add(tmp);
            }
            return params.iterator();
        });
        JavaPairRDD<Long, Integer> everyClickIdCount = clickIdCount.reduceByKey((Integer a, Integer b) -> {
            return a + b;
        });
        return everyClickIdCount;
    }

    // 获得完成的点击数据
   private static JavaPairRDD<Long, String> getFullJoinCategoryIdData(JavaPairRDD<Long, Integer> finalSessionDateWithCategoryId,JavaPairRDD<Long, Integer> everyPayIdCount,
                                                 JavaPairRDD<Long,Integer> everyOrderIdCount,JavaPairRDD<Long,Integer> everyClickIdCount ){
       JavaPairRDD<Long, Tuple2<Integer, Optional<Integer>>> tmpJoinDataOne = finalSessionDateWithCategoryId.leftOuterJoin(everyPayIdCount);
       JavaPairRDD<Long, String> sessionJoinPayData = tmpJoinDataOne.mapToPair((line) -> {
           Optional<Integer> integerOptional = line._2._2;
           String retn;
           if (integerOptional.isPresent()) {
               retn = Constants.PAY_CATGEORY_COUNT + "=" + integerOptional.get() + "|";
           } else {
               retn = Constants.PAY_CATGEORY_COUNT + "=" +"|";
           }
           return new Tuple2<Long, String>(line._1, retn);
       });
       JavaPairRDD<Long, Tuple2<String, Optional<Integer>>> tmpJoinTwo = sessionJoinPayData.leftOuterJoin(everyOrderIdCount);
       JavaPairRDD<Long, String> sessionJoinOrder = tmpJoinTwo.mapToPair((line) -> {
           Optional<Integer> option = line._2._2;
           String retn;
           if (option.isPresent()) {
               retn = line._2._1 + "" + Constants.ORDER_CATEGORY_COUNT + "=" + option.get() + "|";
           } else {
               retn = line._2._1+ "" + Constants.ORDER_CATEGORY_COUNT + "="  + "|";
           }
           return new Tuple2<Long, String>(line._1, retn);
       });
       JavaPairRDD<Long, Tuple2<String, Optional<Integer>>> tmpJoinThree = sessionJoinOrder.leftOuterJoin(everyClickIdCount);
       JavaPairRDD<Long, String> fullJoinData = tmpJoinThree.mapToPair((line) -> {
           Optional<Integer> option = line._2._2;
           String retn;
           if (option.isPresent()) {
               retn = line._2._1 + "" + Constants.CLICK_CATEGORY_COUNT + "=" + option.get();
           } else {
               retn = line._2._1 + "" + Constants.CLICK_CATEGORY_COUNT + "=";
           }
           return new Tuple2<Long, String>(line._1, retn);
       });
       return fullJoinData;
   }

    // 插入获得的top10数据
    public static void insertTopNCategoryPayAndOrderAndClickData(int taskId,List<Tuple2<Category, Long>> topNCategoryData){
        List<Top10Category> data=new ArrayList<Top10Category>();
        for(Tuple2<Category,Long> s:topNCategoryData){
            Top10Category tmp=new Top10Category(taskId,s._1.getCategoryId(),s._1.getPayCatgoryCount(),s._1.getOrderCategoryCount(),s._1.getClickCategoryCount());
            data.add(tmp);
        }
        ITop10Category iTop10Category=Daofactory.getItop10Category();
        iTop10Category.insert(data);
    }

    // 获取top10 的 category 点击次数的session
    public static JavaPairRDD<Long, String>  getTop10CategorySession(JavaSparkContext jsc,JavaPairRDD<String,Row> dataKeyBySessionId,JavaPairRDD<Long,
            Tuple2<String, String>> filterData,List<Tuple2<Category, Long>> topNCategoryData){
        // 转化数据成为sessionId
        JavaPairRDD<String, String> filterDataKeyBySessionId = filterData.mapToPair((line) -> {
            String sessionId = StringUtils.getFieldFromConcatString(line._2._2, "\\|", Constants.SESSION_ID);
            return new Tuple2<String, String>(sessionId, line._2._2);
        });
        JavaPairRDD<String, Tuple2<String, Row>> joinData = filterDataKeyBySessionId.join(dataKeyBySessionId);
        // 对join的数据进行过滤
        joinData= joinData.filter((line)->{
            Object clickCategoryId=line._2._2.get(6);
            if(clickCategoryId==null){
                return false;
            }
            return true;
        });
        JavaPairRDD<String, Long> joinDataKeyBySessionCategoryId = joinData.mapToPair((line) -> {
            String key = line._1 + "_" + line._2._2.getLong(6);
            return new Tuple2<String, Long>(key, 1L);
        });
        JavaPairRDD<String, Long> finalcategoryId = joinDataKeyBySessionCategoryId.reduceByKey((Long a, Long b) -> {
            return a + b;
        });
        JavaPairRDD<Long, String> categoryData = finalcategoryId.mapToPair((line) -> {
            long category = Long.valueOf(line._1.split("_")[1]);
            String val = Constants.SESSION_ID + "=" + line._1.split("_")[0] + "|" + Constants.CLICK_CATEGORY_COUNT + "=" + line._2;
            return new Tuple2<Long, String>(category, val);
        });
        JavaPairRDD<Long, Iterable<String>> categoryDataGroupByKey = categoryData.groupByKey();

        JavaRDD<Tuple2<Category, Long>> pdata = jsc.parallelize(topNCategoryData);
        JavaPairRDD<Long, Long> pDataKeyByCategoryId = pdata.mapToPair((line) -> {
            long catgeoryId = line._1.getCategoryId();
            return new Tuple2<Long, Long>(catgeoryId, catgeoryId);
        });
        JavaPairRDD<Long, Tuple2<Long, Iterable<String>>> tmpJoinData = pDataKeyByCategoryId.join(categoryDataGroupByKey);
        JavaPairRDD<Long, String> finalData = tmpJoinData.flatMapToPair((line) -> {
            int index=0;
            int min=0;
            long count=0;
            String[] val=new String[10];
            Iterator<String> da = line._2._2.iterator();
            while (da.hasNext()){
                String v=da.next();
                long clickCount=Long.valueOf(StringUtils.getFieldFromConcatString(v,"\\|",Constants.CLICK_CATEGORY_COUNT));
                if(index<10){
                    val[index]=v;
                    if(count==0) {
                        count=clickCount;
                        min=0;
                    }else{
                        if(count>clickCount){
                            min=index;
                            count=clickCount;
                        }
                    }
                    index++;
                }else{
                    if(clickCount>count){
                        val[min]=v;
                        count=clickCount;
                    }
                }
            }
            List<Tuple2<Long,String>> params=new ArrayList<Tuple2<Long, String>>();
            int flag=index>=10? 10:index;
            for(int i=0;i<index;i++){
                Tuple2<Long,String> tmp=new Tuple2<Long, String>(line._1,val[i]);
                params.add(tmp);
            }
            return params.iterator();
        });
        return finalData;


    }

     public static void insertTopNCategorySessionData(final int taskId,JavaPairRDD<Long, String> categorySessionData){
        categorySessionData.foreach((line)->{
            ITop10CategorySession iTop10CategorySession=Daofactory.getITop10CategorySession();
            long categoryId=line._1;
            String sessionId=StringUtils.getFieldFromConcatString(line._2,"\\|",Constants.SESSION_ID);
            long clickCount=Long.valueOf(StringUtils.getFieldFromConcatString(line._2,"\\|",Constants.CLICK_CATEGORY_COUNT));
            Top10CategorySession ab=new Top10CategorySession(taskId,sessionId,categoryId,clickCount);
            iTop10CategorySession.insert(ab);
        });
     }

    public static void main(String[] args){
        SparkConf conf= SparkUtils.produceSparkConf("userSessionAnalize");
        //注册序列化对象
        conf.registerKryoClasses(new Class[]{Category.class});
        SparkSession sc=SparkUtils.produceSparkSession(conf);
        JavaSparkContext jsc=SparkUtils.produceJavaSparkContext(sc);

        SQLContext sqlContext=sc.sqlContext();
        // 生成模拟数据
        mockData(jsc,sqlContext);
        JSONObject taskParmas = getTaskParmas(1);
        int taskId=2;
        int topn=10; // 设置获取的topn
        // 获得某一个时间段内的session数据
        JavaRDD<Row> data=getRangeSession(sc,taskParmas);
        // 获得以sessionId 为key 的键值对
        JavaPairRDD<String,Row> dataKeyBySessionId=getDataKeyBySessionId(data);
        dataKeyBySessionId=dataKeyBySessionId.cache();
        JavaPairRDD<String, Iterable<Row>> dataGroupBySessionID = dataKeyBySessionId.groupByKey();
        // 获得整理之后的session信息
        JavaPairRDD<Long,String> lineData=dealDataToLine(dataGroupBySessionID);

        // 获得以userId为key的信息
        JavaPairRDD<Long, String> userInfo = getUserInfo(sc);
        JavaPairRDD<Long, Tuple2<String, String>> userSessionData = userInfo.join(lineData);
        MyAccumator accumator=new MyAccumator();
        jsc.sc().register(accumator);
        // 对数据进行过滤 获得需要的数据
        JavaPairRDD<Long, Tuple2<String, String>> filterData=filterUserSessionDataByParams(userSessionData,taskParmas,accumator);
        // 将访问的比例 插入到数据库之中。
        calculateSessionAndStepAndInsert(taskId,accumator);
        long sessioCount=Long.valueOf(StringUtils.getFieldFromConcatString(accumator.value(),"\\|",Constants.SESSION_COUNT));
        int randExtractNum=1000;
        JavaRDD<String> extractData=randExtractSession(jsc,filterData,sessioCount,randExtractNum);
        insertRandomExtractData(extractData,taskId);
        List<Tuple2<Category, Long>> topNCategoryData = getTop10CategoryByPayAdnOrderAndClick(filterData, topn);
        insertTopNCategoryPayAndOrderAndClickData(taskId,topNCategoryData);
        JavaPairRDD<Long, String> categorySessionData=getTop10CategorySession(jsc,dataKeyBySessionId,filterData,topNCategoryData);
        insertTopNCategorySessionData(taskId,categorySessionData);

    }
}
