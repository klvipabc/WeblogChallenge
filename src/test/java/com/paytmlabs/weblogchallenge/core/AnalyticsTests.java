package com.paytmlabs.weblogchallenge.core;

import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.paytmlabs.weblogchallenge.entities.WeblogSession;

import scala.Tuple2;

public class AnalyticsTests {

    private SparkConf conf = new SparkConf().setAppName("UnitTest").setMaster("local[2]");
    private JavaSparkContext sc;
    private JavaRDD<String> textLines;

    @Before
    public void setup() {
        sc = new JavaSparkContext(conf);
        textLines = sc.parallelize(Lists.newArrayList(WeblogTestEntries.WEBLOG_ENTRIES));
    }

    @After
    public void cleanup() {
        if (sc != null) {
            sc.close();
        }
    }

    @Test
    public void verifySessionization_createCorrectNumberOfSessions() {
        JavaPairRDD<String, WeblogSession> sessions = Analytics.sessionize(textLines);
        Assert.assertTrue(sessions.count() == 5);
    }

    @Test
    public void verifyAverageSessionTimeIsCorrect() {
        JavaPairRDD<String, WeblogSession> sessions = Analytics.sessionize(textLines);
        Assert.assertTrue(Analytics.analyzeAverageSessionTime(sessions) == 2);
    }

    @Test
    public void verifyUniqueUrlsOfSessionsAreCorrect() {
        JavaPairRDD<String, WeblogSession> sessions = Analytics.sessionize(textLines);
        // There should be 2 sessions each with 2 unique URLs
        Assert.assertEquals(2,
                Analytics.analyzeUniqueUrlsOfEachSession(sessions, 5).stream().filter(pair -> pair._2().size() == 2)
                        .count());
        // There should be 3 sessions each with 1 unique URL
        Assert.assertEquals(3,
                Analytics.analyzeUniqueUrlsOfEachSession(sessions, 5).stream().filter(pair -> pair._2().size() == 1)
                        .count());
    }

    @Test
    public void verifyMostEngagedUsersAreCorrect() {
        JavaPairRDD<String, WeblogSession> sessions = Analytics.sessionize(textLines);
        List<Tuple2<String, Long>> mostEngagedUsers = Analytics.analyzeMostEngagedUsers(sessions, 1);
        Assert.assertEquals("0.0.0.2:1000", mostEngagedUsers.get(0)._1());
        Assert.assertTrue(mostEngagedUsers.get(0)._2() == 8);

    }

}
