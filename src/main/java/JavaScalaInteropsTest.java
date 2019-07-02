import java.math.BigInteger;
import java.util.Date;
import java.util.Random;


public class JavaScalaInteropsTest {
    public static void main(String[] args) {
        System.out.println("Hello Java (" + new Random().nextDouble() + ") on " + new Date());

        // exchangind data between scala and java
        Person p = new Person("Reggie", 21);
        Company c = new Company();
        c.add(p);
        p.host(c);

        // calling scala libary from Java
        System.out.println(new scala.math.BigInt(BigInteger.TEN));
        org.apache.spark.SparkConf conf = new org.apache.spark.SparkConf();
        conf.setMaster("local");
        conf.setAppName("JavaScalaInteropsTest");
        System.out.println(conf);
    }
}



