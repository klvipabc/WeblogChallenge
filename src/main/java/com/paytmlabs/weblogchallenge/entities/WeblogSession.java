package com.paytmlabs.weblogchallenge.entities;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import scala.Serializable;

public class WeblogSession implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<WeblogEntry> weblogEntries;

    public WeblogSession() {
        this.weblogEntries = Lists.newArrayList();
    }

    public WeblogSession(WeblogEntry weblogEntry) {
        this.weblogEntries = Lists.newArrayList(weblogEntry);
    }

    public void appendWeblogEntry(WeblogEntry weblogEntry) {
        weblogEntries.add(weblogEntry);
    }

    public List<WeblogEntry> getWeblogEntries() {
        return weblogEntries;
    }

    /*
     * The time length between the first weblog entry and the last weblog entry in the session. If there is only one
     * weblog entry in the session, the length is considered as zero.
     *
     * @return the session time length
     */
    public long sessionLengthInSecond() {
        if (weblogEntries.size() <= 1) {
            return 0L;
        }
        WeblogEntry start = weblogEntries.get(0);
        WeblogEntry end = Iterables.getLast(weblogEntries);
        return Duration.between(start.getTime(), end.getTime()).getSeconds();
    }

    public Set<String> uniqueUrls() {
        return weblogEntries.stream().map(WeblogEntry::getUrl).collect(Collectors.toSet());
    }

}
