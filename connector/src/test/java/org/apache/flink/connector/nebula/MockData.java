/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockData {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(MockData.class);

    public static void mockSchema() {
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        List<HostAddress> addresses = Arrays.asList(new HostAddress("127.0.0.1", 9669));
        NebulaPool pool = new NebulaPool();
        Session session = null;
        try {
            pool.init(addresses, nebulaPoolConfig);
            session = pool.getSession("root", "nebula", true);

            ResultSet respStringSpace = session.execute(createStringSpace());
            ResultSet respIntSpace = session.execute(createIntSpace());

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            if (!respStringSpace.isSucceeded()) {
                LOGGER.error("create string vid type space failed, {}",
                        respStringSpace.getErrorMessage());
                assert (false);
            }
            if (!respIntSpace.isSucceeded()) {
                LOGGER.error("create int vid type space failed, {}",
                        respIntSpace.getErrorMessage());
                assert (false);
            }
        } catch (UnknownHostException | NotValidConnectionException
                | IOErrorException | AuthFailedException | ClientServerIncompatibleException e) {
            LOGGER.error("create space error, ", e);
            assert (false);
        } finally {
            pool.close();
        }
    }


    private static String createStringSpace() {
        String exec = "CREATE SPACE IF NOT EXISTS test_string(partition_num=10,"
                + "vid_type=fixed_string(8));"
                + "USE test_string;"
                + "CREATE TAG IF NOT EXISTS person(col1 fixed_string(8), col2 string, col3 int32,"
                + " col4 double, col5 date, col6 datetime, col7 time, col8 timestamp);"
                + "CREATE EDGE IF NOT EXISTS friend(col1 fixed_string(8), col2 string, col3 "
                + "int32, col4 double, col5 date, col6 datetime, col7 time, col8 timestamp);";
        return exec;
    }

    private static String createIntSpace() {
        String exec = "CREATE SPACE IF NOT EXISTS test_int(partition_num=10,vid_type=int64);"
                + "USE test_int;"
                + "CREATE TAG IF NOT EXISTS person(col1 fixed_string(8), col2 string, col3 int32,"
                + " col4 double, col5 date, col6 datetime, col7 time, col8 timestamp);"
                + "CREATE EDGE IF NOT EXISTS friend(col1 fixed_string(8), col2 string, col3 "
                + "int32, col4 double, col5 date, col6 datetime, col7 time, col8 timestamp);";
        return exec;
    }
}
