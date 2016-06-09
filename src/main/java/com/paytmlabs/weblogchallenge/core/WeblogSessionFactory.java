package com.paytmlabs.weblogchallenge.core;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.paytmlabs.weblogchallenge.entities.WeblogEntry;
import com.paytmlabs.weblogchallenge.entities.WeblogSession;

public class WeblogSessionFactory {

    private static final int SESSION_TIME_WINDOW_IN_MINUTE = 15;

    /*
     * Create one or more weblog session from a list of weblog entries. Two adjacent weblog entries are considered in a
     * same session if they are less than 15 minutes apart from each other. Otherwise, the second weblog entry starts a
     * new session.
     *
     * @param weblogEntriesIt a list/set/... of weblog entries that is not required to be sorted
     *
     * @return a list of weblog sessions
     */
    public static List<WeblogSession> buildFromWeblogEntries(Iterable<WeblogEntry> weblogEntriesIt) {
        Preconditions.checkArgument(weblogEntriesIt != null);

        List<WeblogEntry> weblogEntries = Lists.newArrayList(weblogEntriesIt);
        weblogEntries.sort(Comparator.comparing(WeblogEntry::getTime));

        if (weblogEntries.isEmpty()) {
            return Collections.emptyList();
        }

        List<WeblogSession> weblogSessions = Lists.newArrayList(new WeblogSession());
        for (WeblogEntry weblogEntry : weblogEntries) {
            WeblogSession currentSession = Iterables.getLast(weblogSessions);
            if (shouldAddToCurrentSession(currentSession, weblogEntry)) {
                currentSession.appendWeblogEntry(weblogEntry);
            } else {
                // This weblogEntry starts a new session
                weblogSessions.add(new WeblogSession(weblogEntry));
            }
        }
        return weblogSessions;
    }

    /*
     * Two adjacent weblog entries belong to same session if they are less than 15 minutes apart from each other.
     * Otherwise, the second weblog entry should start a new session.
     */
    private static boolean shouldAddToCurrentSession(WeblogSession currentSession, WeblogEntry weblogEntry) {
        if (currentSession.getWeblogEntries().isEmpty()) {
            return true;
        }
        WeblogEntry lastWeblogEntryInCurrentSession = Iterables.getLast(currentSession.getWeblogEntries());
        // This weblogEntry is less than timeWindowInMinutes later than the previous entry
        if (lastWeblogEntryInCurrentSession.getTime().plusMinutes(SESSION_TIME_WINDOW_IN_MINUTE)
                .isAfter(weblogEntry.getTime())) {
            return true;
        }
        return false;
    }

}
