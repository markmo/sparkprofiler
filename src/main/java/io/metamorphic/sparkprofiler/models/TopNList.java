package io.metamorphic.sparkprofiler.models;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by markmo on 25/10/2015.
 */
public class TopNList implements Serializable {

    private int maxSize;
    private List<Pair<Object, Long>> topNCountsForColumnArray = new ArrayList<>();
    private int lowestColumnCountIndex = -1;
    private Long lowestValue = Long.MAX_VALUE;
    
    public TopNList(int maxSize) {
        this.maxSize = maxSize;
    }

    public List<Pair<Object, Long>> getTopNCountsForColumnArray() {
        return topNCountsForColumnArray;
    }

    public void add(Object newValue, Long newCount) {
        if (topNCountsForColumnArray.size() < maxSize - 1) {
            topNCountsForColumnArray.add(new ImmutablePair<>(newValue, newCount));
        } else if (topNCountsForColumnArray.size() == maxSize) {
            updateLowestValue();
        } else {
            if (newCount > lowestValue) {
                topNCountsForColumnArray.add(lowestColumnCountIndex, new ImmutablePair<>(newValue, newCount));
            }
        }
    }

    public void updateLowestValue() {
        AtomicInteger index = new AtomicInteger(0);
        topNCountsForColumnArray.forEach(r -> {
            if (r.getRight() < lowestValue) {
                lowestValue = r.getRight();
                lowestColumnCountIndex = index.get();
            }
            index.incrementAndGet();
        });
    }

    @Override
    public String toString() {
        return String.format("TopNList(topNCountsForColumnArray=%s", topNCountsForColumnArray);
    }
}
