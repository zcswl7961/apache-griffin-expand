/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package org.apache.griffin.core;


import org.apache.griffin.core.common.SimpleCORSFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * <p>
 * 使用手册：
 *      打包：
 *          mvn clean
 *          mvn -T2C install -DskipTests
 *      重命名：
 *          mv measure/target/measure-0.5.0-SNAPSHOT.jar $GRIFFIN_HOME/griffin-measure.jar
 *          mv service/target/service-0.5.0-SNAPSHOT.jar $GRIFFIN_HOME/griffin-service.jar
 *      发布到hadoop目录上(交由spark进行执行)：
 *          hadoop fs -mkdir -p /griffin/persist
 *          hadoop fs -mkdir /griffin/checkpoint
 *          hadoop fs -put $GRIFFIN_HOME/griffin-measure.jar /griffin/
 *      执行：
 *          nohup java -jar $GRIFFIN_HOME/griffin-service.jar>$GRIFFIN_HOME/service.out 2>&1 &
 *      查看日志：
 *          tail -f $GRIFFIN_HOME/service.out
 * </p>
 * @author zhoucg
 * @date 2019-09-21
 */
@SpringBootApplication
@EnableScheduling
public class GriffinWebApplication {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(GriffinWebApplication.class);

    public static void main(String[] args) {
        LOGGER.info("application start");
        /**
         * 这个问题是为了处理在window环境下，调用hadoop winutils.exe路径找不到的错误
         * java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
         */
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master");
        SpringApplication.run(GriffinWebApplication.class, args);
    }

    @Bean
    public SimpleCORSFilter simpleFilter() {
        return new SimpleCORSFilter();
    }

}
