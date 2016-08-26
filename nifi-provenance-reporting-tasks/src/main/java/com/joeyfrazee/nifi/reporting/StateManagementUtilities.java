package com.joeyfrazee.nifi.reporting;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lev on 8/25/16.
 */
public class StateManagementUtilities {

    public static String getStateEntry(String key, StateManager stateManager)  throws IOException {
        final StateMap stateMap = stateManager.getState(Scope.CLUSTER);
        if (stateMap.toMap().containsKey(key)) {
            return stateMap.get(key);
        }
        return null;
    }

    public static void setStateEntry(String key, StateManager stateManager, String value) throws IOException {
        final StateMap stateMap = stateManager.getState(Scope.CLUSTER);
        final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());
        statePropertyMap.put(key, value);
        stateManager.setState(statePropertyMap, Scope.CLUSTER);
    }
}
