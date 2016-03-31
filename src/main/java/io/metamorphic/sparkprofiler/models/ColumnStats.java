package io.metamorphic.sparkprofiler.models;

import java.io.Serializable;

/**
 * Created by markmo on 25/10/2015.
 */
public class ColumnStats implements Serializable {

    private Long nulls = 0L;
    private Long empties = 0L;
    private Long totalCount = 0L;
    private Long uniqueValues = 0L;
    private Long maxLong = Long.MIN_VALUE;
    private Long minLong = Long.MAX_VALUE;
    private Long sumLong = 0L;
    private TopNList topNValues = new TopNList(5);

    public Long getNulls() {
        return nulls;
    }

    public Long getEmpties() {
        return empties;
    }

    public Long getTotalCount() {
        return totalCount;
    }

    public Long getUniqueValues() {
        return uniqueValues;
    }

    public Long getMaxLong() {
        return maxLong;
    }

    public Long getMinLong() {
        return minLong;
    }

    public Long getSumLong() {
        return sumLong;
    }

    public TopNList getTopNValues() {
        return topNValues;
    }

    public Long avgLong() {
        if (totalCount > 0) {
            return sumLong / totalCount;
        }
        return 0L;
    }

    public void add(Object colValue, Long colCount) {
        totalCount += colCount;
        uniqueValues += 1;
        if (colValue == null) {
            nulls += 1;
        } else if (colValue instanceof String) {
            String colStringValue = (String) colValue;
            if (colStringValue.isEmpty()) {
                empties += 1;
            }
        } else if (colValue instanceof Long) {
            Long colLongValue = (Long) colValue;
            if (maxLong < colLongValue) maxLong = colLongValue;
            if (minLong > colLongValue) minLong = colLongValue;
            sumLong += colLongValue;
        }
        topNValues.add(colValue, colCount);
    }

    public void add(ColumnStats columnStats) {
        totalCount += columnStats.getTotalCount();
        uniqueValues += columnStats.getUniqueValues();
        nulls += columnStats.getNulls();
        empties += columnStats.getEmpties();
        sumLong += columnStats.getSumLong();
        maxLong += columnStats.getMaxLong();
        minLong += columnStats.getMinLong();
        columnStats.getTopNValues().getTopNCountsForColumnArray().forEach(r ->
            topNValues.add(r.getLeft(), r.getRight())
        );
    }

    @Override
    public String toString() {
        return String.format("ColumnStats(nulls=%s, empties=%s, totalCount=%s, uniqueValues=%s, maxLong=%s, minLong=%s, sumLong=%s, topNValues=%s, avgLong=%s)",
                nulls, empties, totalCount, uniqueValues, maxLong, minLong, sumLong, topNValues, avgLong());
    }
}
