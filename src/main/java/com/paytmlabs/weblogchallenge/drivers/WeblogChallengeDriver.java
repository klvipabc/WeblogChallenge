package com.paytmlabs.weblogchallenge.drivers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Strings;
import com.paytmlabs.weblogchallenge.core.Analytics;
import com.paytmlabs.weblogchallenge.entities.WeblogSession;

public class WeblogChallengeDriver {

    private static final String APP_NAME = "Weblog_Challenge";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(APP_NAME);
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<String> textLines = sc.textFile(localPathToWeblogFile(args));

            // key:IP, value:Session
            JavaPairRDD<String, WeblogSession> sessions = Analytics.sessionize(textLines);

            Analytics.analyzeAverageSessionTime(sessions);
            Analytics.analyzeUniqueUrlsOfEachSession(sessions, 10);
            Analytics.analyzeMostEngagedUsers(sessions, 10);
        }
    }

    private static String localPathToWeblogFile(String[] args) {
        if (args.length == 0 || Strings.isNullOrEmpty(args[0])) {
            System.err.println("Please provide local path to the weblog file.");
            System.exit(-1);
        }
        return args[0];
    }

}
