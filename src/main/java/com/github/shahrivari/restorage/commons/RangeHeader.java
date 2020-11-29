package com.github.shahrivari.restorage.commons;

import com.github.shahrivari.restorage.exception.InvalidRangeRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RangeHeader {
    private Long start;
    private Long end;
    private Long suffixLength;

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    public Long getSuffixLength() {
        return suffixLength;
    }

    public Long length() {
        if (end == null)
            return Long.MAX_VALUE;
        else
            return end - start;
    }

    public static List<RangeHeader> decodeRange(String rangeHeader) throws InvalidRangeRequest {
        List<RangeHeader> ranges = new ArrayList<>();
        String byteRangeSetRegex = "(((?<byteRangeSpec>(?<firstBytePos>\\d+)-(?<lastBytePos>\\d+)?)|(?<suffixByteRangeSpec>-(?<suffixLength>\\d+)))(,|$))";
        String byteRangesSpecifierRegex = "bytes=(?<byteRangeSet>" + byteRangeSetRegex + "{1,})";
        Pattern byteRangeSetPattern = Pattern.compile(byteRangeSetRegex);
        Pattern byteRangesSpecifierPattern = Pattern.compile(byteRangesSpecifierRegex);
        Matcher byteRangesSpecifierMatcher = byteRangesSpecifierPattern.matcher(rangeHeader);
        if (byteRangesSpecifierMatcher.matches()) {
            String byteRangeSet = byteRangesSpecifierMatcher.group("byteRangeSet");
            Matcher byteRangeSetMatcher = byteRangeSetPattern.matcher(byteRangeSet);
            while (byteRangeSetMatcher.find()) {
                RangeHeader range = new RangeHeader();
                if (byteRangeSetMatcher.group("byteRangeSpec") != null) {
                    String start = byteRangeSetMatcher.group("firstBytePos");
                    String end = byteRangeSetMatcher.group("lastBytePos");
                    range.start = Long.valueOf(start);
                    range.end = end == null ? null : Long.valueOf(end);
                } else if (byteRangeSetMatcher.group("suffixByteRangeSpec") != null) {
                    range.suffixLength = Long.valueOf(byteRangeSetMatcher.group("suffixLength"));
                } else {
                    throw new InvalidRangeRequest(rangeHeader);
                }
                ranges.add(range);
            }
        } else {
            throw new InvalidRangeRequest(rangeHeader);
        }
        return ranges;
    }

}

