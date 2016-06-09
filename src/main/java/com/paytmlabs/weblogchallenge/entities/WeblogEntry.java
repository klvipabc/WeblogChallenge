package com.paytmlabs.weblogchallenge.entities;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.clearspring.analytics.util.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import scala.Serializable;

public class WeblogEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int TIME_INDEX = 0;

    private static final int IP_INDEX = 2;

    private static final int URL_INDEX = 11;

    private final ZonedDateTime time;

    private final String ip;

    private final String url;

    public static WeblogEntry tryCreateFromTextLine(String weblogTextLine) {
        try {
            return new WeblogEntry(weblogTextLine);
        }
        catch (RuntimeException e) {
            // Invalid text line
            return null;
        }
    }

    public WeblogEntry(String weblogTextLine) {
        List<String> fields = parseTextLine(weblogTextLine);
        this.time = ZonedDateTime.parse(fields.get(TIME_INDEX), DateTimeFormatter.ISO_DATE_TIME);
        this.ip = fields.get(IP_INDEX);
        this.url = fields.get(URL_INDEX).split(" ")[1];
    }

    public ZonedDateTime getTime() {
        return time;
    }

    public String getIp() {
        return ip;
    }

    public String getUrl() {
        return url;
    }

    private List<String> parseTextLine(String textLine) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(textLine));

        // Regular expression to split by space, except those between double quotes
        // Solution copy-pasted from:
        // http://stackoverflow.com/questions/7804335/split-string-on-spaces-in-java-except-if-between-quotes-i-e-treat-hello-wor
        // Verified by unit-tests
        Matcher matcher = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(textLine);
        List<String> fields = Lists.newArrayList();
        while (matcher.find()) {
            fields.add(matcher.group(1));
        }

        if (fields.size() != 15) {
            throw new RuntimeException("[" + textLine + "] is not a valid weblog text line.");
        }
        return fields;
    }

}
