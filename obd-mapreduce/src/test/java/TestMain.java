import com.tiza.support.util.DateUtil;
import org.junit.Test;

import java.util.Calendar;

/**
 * Description: TestMain
 * Author: DIYILIU
 * Update: 2018-09-04 09:21
 */
public class TestMain {

    @Test
    public void test() {

        double d = 1.557;

        System.out.println(String.format("%.2f", d));
    }

    @Test
    public void test1(){

        System.out.println(DateUtil.getYMD(Calendar.getInstance()));
    }
}
