package mdt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class MDTStudy {
//    static Logger log = LoggerFactory.getLogger(MDTStudy.class);
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(MDTStudy.class);
        new Thread(new Runnable() {
            @Override
            public void run() {
                MDC.put("traceId", "trace111");
                while (true) {
                    log.info("thread1");
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            log.info("thread11");
                        }
                    }).start();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                MDC.put("traceId", "trace222");
                while (true) {
                    log.info("thread2");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }).start();
    }
}
