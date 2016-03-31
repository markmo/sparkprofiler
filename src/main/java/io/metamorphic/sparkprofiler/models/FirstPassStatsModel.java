package io.metamorphic.sparkprofiler.models;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by markmo on 25/10/2015.
 */
public class FirstPassStatsModel implements Serializable {

    private Map<Integer, ColumnStats> columnStatsMap = new HashMap<>();

    public Map<Integer, ColumnStats> getColumnStatsMap() {
        return columnStatsMap;
    }

    public void add(int colIndex, Object colValue, Long colCount) {
        if (!columnStatsMap.containsKey(colIndex)) {
            columnStatsMap.put(colIndex, new ColumnStats());
        }
        columnStatsMap.get(colIndex).add(colValue, colCount);
    }

    public FirstPassStatsModel add(FirstPassStatsModel firstPassStatsModel) {
        firstPassStatsModel.getColumnStatsMap().entrySet().forEach(e -> {
            ColumnStats columnStats = columnStatsMap.getOrDefault(e.getKey(), null);
            if (columnStats != null) {
                columnStats.add(e.getValue());
            } else {
                columnStatsMap.put(e.getKey(), e.getValue());
            }
        });
        return this;
    }

    @Override
    public String toString() {
        return String.format("FirstPassStatsModel(columnStatsMap=%s)", columnStatsMap);
    }
}
