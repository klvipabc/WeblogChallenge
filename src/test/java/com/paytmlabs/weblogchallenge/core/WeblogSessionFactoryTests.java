package com.paytmlabs.weblogchallenge.core;

import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.paytmlabs.weblogchallenge.entities.WeblogEntry;
import com.paytmlabs.weblogchallenge.entities.WeblogSession;

public class WeblogSessionFactoryTests {

    @Test
    public void zeroWeblogEntries_shouldReturnZeroSessions() {
        List<WeblogSession> sessions = WeblogSessionFactory.buildFromWeblogEntries(Collections.emptyList());
        Assert.assertTrue(sessions.isEmpty());
    }

    @Test
    public void oneWeblogEntries_shouldReturnOneSession() {
        WeblogEntry weblogEntry = WeblogEntry.tryCreateFromTextLine(
                "2015-07-22T09:00:31.414860Z marketpalce-shop 182.68.252.156:63577 10.0.6.108:80 0.000021 0.003417 0.000021 200 200 0 12 \"GET https://paytm.com:443/papi/v1/promosearch/product/15573288/offers?parent_id=13135967&price=2197&channel=web&version=2 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2");
        List<WeblogSession> sessions = WeblogSessionFactory.buildFromWeblogEntries(Lists.newArrayList(weblogEntry));
        Assert.assertTrue(sessions.size() == 1);
        sessions.get(0).getWeblogEntries().get(0).getUrl().equals(
                "https://paytm.com:443/papi/v1/promosearch/product/15573288/offers?parent_id=13135967&price=2197&channel=web&version=2");
    }

    @Test
    public void twoWeblogEntries_LessThan15MinApart_shouldReturnOneSession() {
        WeblogEntry weblogEntry1 = WeblogEntry.tryCreateFromTextLine(
                "2015-07-22T09:00:31.414860Z marketpalce-shop 182.68.252.156:63577 10.0.6.108:80 0.000021 0.003417 0.000021 200 200 0 12 \"GET https://paytm.com:443/papi/v1/promosearch/product/15573288/offers?parent_id=13135967&price=2197&channel=web&version=2 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2");
        WeblogEntry weblogEntry2 = WeblogEntry.tryCreateFromTextLine(
                "2015-07-22T09:05:31.414860Z marketpalce-shop 182.68.252.156:63577 10.0.6.108:80 0.000021 0.003417 0.000021 200 200 0 12 \"GET https://paytm.com:443/abc HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2");
        List<WeblogSession> sessions = WeblogSessionFactory.buildFromWeblogEntries(Lists.newArrayList(weblogEntry1, weblogEntry2));
        Assert.assertTrue(sessions.size() == 1);
        sessions.get(0).getWeblogEntries().get(0).getUrl().equals(
                "https://paytm.com:443/papi/v1/promosearch/product/15573288/offers?parent_id=13135967&price=2197&channel=web&version=2");
        sessions.get(0).getWeblogEntries().get(1).getUrl().equals(
                "https://paytm.com:443/abc");
    }

    @Test
    public void twoWeblogEntries_moreThan15MinApart_shouldReturnTwoSessions() {
        WeblogEntry weblogEntry1 = WeblogEntry.tryCreateFromTextLine(
                "2015-07-22T09:00:31.414860Z marketpalce-shop 182.68.252.156:63577 10.0.6.108:80 0.000021 0.003417 0.000021 200 200 0 12 \"GET https://paytm.com:443/papi/v1/promosearch/product/15573288/offers?parent_id=13135967&price=2197&channel=web&version=2 HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2");
        WeblogEntry weblogEntry2 = WeblogEntry.tryCreateFromTextLine(
                "2015-07-22T09:20:31.414860Z marketpalce-shop 182.68.252.156:63577 10.0.6.108:80 0.000021 0.003417 0.000021 200 200 0 12 \"GET https://paytm.com:443/abc HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2");
        List<WeblogSession> sessions = WeblogSessionFactory.buildFromWeblogEntries(Lists.newArrayList(weblogEntry1, weblogEntry2));
        Assert.assertTrue(sessions.size() == 2);
        sessions.get(0).getWeblogEntries().get(0).getUrl().equals(
                "https://paytm.com:443/papi/v1/promosearch/product/15573288/offers?parent_id=13135967&price=2197&channel=web&version=2");
        sessions.get(1).getWeblogEntries().get(0).getUrl().equals(
                "https://paytm.com:443/abc");
    }

    @Test
    public void fourWeblogEntries_moreThan15MinApart_unordered_shouldReturnThreeSessions() {
        WeblogEntry weblogEntry1 = WeblogEntry.tryCreateFromTextLine(
                "2015-07-22T09:00:31.414860Z marketpalce-shop 182.68.252.156:63577 10.0.6.108:80 0.000021 0.003417 0.000021 200 200 0 12 \"GET https://paytm.com:443/a HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2");
        WeblogEntry weblogEntry2 = WeblogEntry.tryCreateFromTextLine(
                "2015-07-22T09:20:31.414860Z marketpalce-shop 182.68.252.156:63577 10.0.6.108:80 0.000021 0.003417 0.000021 200 200 0 12 \"GET https://paytm.com:443/b HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2");
        WeblogEntry weblogEntry3 = WeblogEntry.tryCreateFromTextLine(
                "2015-07-22T09:21:31.414860Z marketpalce-shop 182.68.252.156:63577 10.0.6.108:80 0.000021 0.003417 0.000021 200 200 0 12 \"GET https://paytm.com:443/c HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2");
        WeblogEntry weblogEntry4 = WeblogEntry.tryCreateFromTextLine(
                "2015-07-22T09:50:31.414860Z marketpalce-shop 182.68.252.156:63577 10.0.6.108:80 0.000021 0.003417 0.000021 200 200 0 12 \"GET https://paytm.com:443/d HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2");
        List<WeblogSession> sessions = WeblogSessionFactory.buildFromWeblogEntries(Lists.newArrayList(weblogEntry3, weblogEntry2, weblogEntry1, weblogEntry4));
        Assert.assertTrue(sessions.size() == 3);
        sessions.get(0).getWeblogEntries().get(0).getUrl().equals(
                "https://paytm.com:443/a");
        sessions.get(1).getWeblogEntries().get(0).getUrl().equals(
                "https://paytm.com:443/b");
        sessions.get(1).getWeblogEntries().get(1).getUrl().equals(
                "https://paytm.com:443/c");
        sessions.get(2).getWeblogEntries().get(0).getUrl().equals(
                "https://paytm.com:443/d");
    }

}
