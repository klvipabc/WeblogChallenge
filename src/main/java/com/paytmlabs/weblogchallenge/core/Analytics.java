package com.paytmlabs.weblogchallenge.core;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.base.Preconditions;
import com.paytmlabs.weblogchallenge.entities.WeblogEntry;
import com.paytmlabs.weblogchallenge.entities.WeblogSession;

import scala.Tuple2;

public class Analytics {

    /*
     * Build a cached RDD pair for each weblog session: [key:ip -> value:session]. The session time window is defined in
     * WeblogSessionFactory. Any invalid weblog entry will be ignored.
     *
     * @param weblogLines a RDD which contains the lines in a weblog file
     *
     * @return a cached RDD pair containing weblog sessions: [key:ip -> value:session]
     */
    public static JavaPairRDD<String, WeblogSession> sessionize(JavaRDD<String> textLines) {
        JavaRDD<WeblogEntry> weblogEntries = textLines.map(WeblogEntry::tryCreateFromTextLine)
                .filter(Objects::nonNull);
        JavaPairRDD<String, Iterable<WeblogEntry>> ipToGroupedWeblogEntriesPairs = weblogEntries
                .groupBy(WeblogEntry::getIp);
        return ipToGroupedWeblogEntriesPairs.flatMapValues(WeblogSessionFactory::buildFromWeblogEntries).cache();
    }

    /*
     * Analyze the average (mean) time length of all sessions. If a session has only one weblog entry, the time length
     * is considered to be zero.
     *
     * @param sessions a RDD pair containing weblog sessions: [key:ip -> value:session]
     *
     * @return average session time length in second of all sessions
     */
    public static double analyzeAverageSessionTime(JavaPairRDD<String, WeblogSession> sessions) {

        double averageSessionLengthInSecond = sessions
                .values()
                .mapToDouble(WeblogSession::sessionLengthInSecond)
                .mean();

        highlightOutput();
        System.out.println("Average session time length in second:\n" + averageSessionLengthInSecond);
        return averageSessionLengthInSecond;
    }

    /*
     * Analyze the unique URLs of each session.
     *
     * @param sessions a RDD pair containing weblog sessions: [key:ip -> value:session]
     *
     * @param sampleSize the number of sessions to sample from the entire result
     *
     * @return sampled sessions with their unique URLs
     */
    public static List<Tuple2<WeblogSession, Set<String>>> analyzeUniqueUrlsOfEachSession(
            JavaPairRDD<String, WeblogSession> sessions,
            int sampleSize) {

        List<Tuple2<WeblogSession, Set<String>>> uniqueUrlsSample = sessions
                .values()
                .mapToPair(session -> new Tuple2<WeblogSession, Set<String>>(session, session.uniqueUrls()))
                .takeSample(false, sampleSize);

        highlightOutput();
        System.out.println("Unique URLs of " + sampleSize + " sample sessions:\n" + uniqueUrlsSample.stream()
                .map(t -> t._2().stream().collect(Collectors.joining("||"))).collect(Collectors.joining("\n")));
        return uniqueUrlsSample;
    }

    /*
     * Analyze the most engaged users, in terms of total session time each IP spent.
     *
     * @param sessions a RDD pair containing weblog sessions: [key:ip -> value:session]
     *
     * @param numberOfUsers the number of top-ranked users to analyze
     *
     * @return top-ranked IPs with their total session time in second
     */
    public static List<Tuple2<String, Long>> analyzeMostEngagedUsers(
            JavaPairRDD<String, WeblogSession> sessions,
            int numberOfUsers) {
        Preconditions.checkArgument(numberOfUsers > 0);

        List<Tuple2<String, Long>> mostEngagedUsers = sessions
                .aggregateByKey(
                        0L,
                        (sum, session) -> sum + session.sessionLengthInSecond(),
                        (sum1, sum2) -> sum1 + sum2)
                .mapToPair(pair -> pair.swap())
                .sortByKey(false)
                .mapToPair(pair -> pair.swap())
                .take(numberOfUsers);

        highlightOutput();
        System.out.println("Top 10 most engaged users:\n" + mostEngagedUsers.stream()
                .map(t -> t._1() + " " + t._2() + "sec").collect(Collectors.joining("\n")));
        return mostEngagedUsers;
    }

    private static void highlightOutput() {
        System.out.println("**********OUTPUT**********OUTPUT**********OUTPUT**********OUTPUT**********OUTPUT**********");
    }

}
