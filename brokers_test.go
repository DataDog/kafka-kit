package main

import (
  "testing"
)

func TestBrokerMapFromTopicMap(t *testing.T) {
  pm, _ := partitionMapFromString(testGetMapString("test_topic"))
  bm := testGetBrokerMetaMap()
  forceRebuild := false

  brokers := brokerMapFromTopicMap(pm, *bm, forceRebuild)

  expected := brokerMap{
    0: &broker{id: 0, replace: true},
    1001: &broker{id: 1001, locality: "a", used: 3, replace: false},
    1002: &broker{id: 1002, locality: "b", used: 3, replace: false},
    1003: &broker{id: 1003, locality: "c", used: 2, replace: false},
    1004: &broker{id: 1004, locality: "a", used: 2, replace: false},
  }


  for id, b := range brokers {
    switch {
    case b.id != expected[id].id:
      t.Errorf("Expected id %d, got %d for broker %d",
        expected[id].id, b.id, id)
    case b.locality != expected[id].locality:
      t.Errorf("Expected locality %s, got %s for broker %d",
        expected[id].locality, b.locality, id)
    case b.used != expected[id].used:
      t.Errorf("Expected used %d, got %d for broker %d",
        expected[id].used, b.used, id)
    case b.replace != expected[id].replace:
      t.Errorf("Expected replace %b, got %b for broker %d",
        expected[id].replace, b.replace, id)
    }
  }
}
