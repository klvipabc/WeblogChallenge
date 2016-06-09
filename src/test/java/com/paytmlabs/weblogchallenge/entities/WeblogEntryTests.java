package com.paytmlabs.weblogchallenge.entities;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.Assert;
import org.junit.Test;

public class WeblogEntryTests {

    @Test
    public void givenValidWeblogLine_shouldCreateWeblogEntry() {
        String weblogLine = "2015-07-22T09:00:31.414860Z marketpalce-shop 182.68.252.156:63577 10.0.6.108:80 0.000021 0.003417 0.000021 200 200 0 12 \"GET https://paytm.com:443/papi/v1/promosearch/product/15573288/offers?parent_id=13135967&price=2197&channel=web&version=2 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2";
        WeblogEntry weblogEntry = new WeblogEntry(weblogLine);
        Assert.assertEquals(ZonedDateTime.of(2015, 7, 22, 9, 00, 31, 414860000, ZoneOffset.UTC), weblogEntry.getTime());
        Assert.assertEquals("182.68.252.156:63577", weblogEntry.getIp());
        Assert.assertEquals(
                "https://paytm.com:443/papi/v1/promosearch/product/15573288/offers?parent_id=13135967&price=2197&channel=web&version=2",
                weblogEntry.getUrl());
    }

    @Test(expected = RuntimeException.class)
    public void givenWeblogLineWithMissingFields_shouldThrowException() {
        String weblogLine = "2015-07-22T09:00:31.414860Z marketpalce-shop 182.68.252.156:63577 10.0.6.108:80";
        new WeblogEntry(weblogLine);
    }

    @Test(expected = RuntimeException.class)
    public void givenEmptyLine_shouldThrowException() {
        String weblogLine = "";
        new WeblogEntry(weblogLine);
    }

    @Test(expected = RuntimeException.class)
    public void givenNull_shouldThrowException() {
        String weblogLine = "";
        new WeblogEntry(weblogLine);
    }
}
