package mapper

import (
	"fmt"
	"io/ioutil"
	"testing"
)

func TestNewPartitionMap(t *testing.T) {
	p := NewPartitionMap()

	// Provided no PartitionMapOpts, the partitions list should be empty.
	if len(p.Partitions) != 0 {
		t.Error("Expected empty Partitions field")
	}

	p = NewPartitionMap(Populate("test", 12, 2))

	// Check the desired partitions len.
	pLen := len(p.Partitions)
	if pLen != 12 {
		t.Errorf("Expected Partitions field len 12, got %d", pLen)
	}

	// Check the replicas len.
	rLen := len(p.Partitions[0].Replicas)
	if rLen != 2 {
		t.Errorf("Expected replication factor 2, got %d", rLen)
	}

	// Check the name.
	if p.Partitions[0].Topic != "test" {
		t.Errorf("Expected topic name 'test', got '%s'", p.Partitions[0].Topic)
	}
}

func TestPartitionEquality(t *testing.T) {
	p1 := Partition{Topic: "test_topic", Partition: 1, Replicas: []int{1, 2, 3}}
	p2 := Partition{Topic: "test_topic", Partition: 1, Replicas: []int{1, 2, 3}}
	p3 := Partition{Topic: "other_topic", Partition: 1, Replicas: []int{1, 2, 3}}
	p4 := Partition{Topic: "test_topic", Partition: 2, Replicas: []int{1, 2, 3}}
	p5 := Partition{Topic: "test_topic", Partition: 1, Replicas: []int{4, 5, 6}}

	if !p1.Equal(p2) {
		t.Error("Unexpected inequality between p1 and p2")
	}
	if p1.Equal(p3) {
		t.Error("Unexpected equality between p1 and p3")
	}
	if p1.Equal(p4) {
		t.Error("Unexpected equality between p1 and p4")
	}
	if p1.Equal(p5) {
		t.Error("Unexpected equality between p1 and p5")
	}
}

func testGetMapString(n string) string {
	return fmt.Sprintf(`{"version":1,"partitions":[
    {"topic":"%s","partition":0,"replicas":[1001,1002]},
    {"topic":"%s","partition":1,"replicas":[1002,1001]},
    {"topic":"%s","partition":2,"replicas":[1003,1004,1001]},
    {"topic":"%s","partition":3,"replicas":[1004,1003,1002]}]}`, n, n, n, n)
}

func testGetMapString2(n string) string {
	return fmt.Sprintf(`{"version":1,"partitions":[
    {"topic":"%s","partition":0,"replicas":[1001,1002]},
    {"topic":"%s","partition":1,"replicas":[1002,1001]},
    {"topic":"%s","partition":2,"replicas":[1003,1004]},
    {"topic":"%s","partition":3,"replicas":[1004,1003]},
		{"topic":"%s","partition":4,"replicas":[1004,1003]},
		{"topic":"%s","partition":5,"replicas":[1004,1003]},
		{"topic":"%s","partition":6,"replicas":[1004,1003]}]}`, n, n, n, n, n, n, n)
}

func testGetMapString3(n string) string {
	return fmt.Sprintf(`{"version":1,"partitions":[
    {"topic":"%s","partition":0,"replicas":[1001,1002]},
    {"topic":"%s","partition":1,"replicas":[1002,1001]},
    {"topic":"%s","partition":2,"replicas":[1003,1004,1001]},
    {"topic":"%s","partition":3,"replicas":[1004,1003,1002]},
		{"topic":"%s","partition":3,"replicas":[1004,1005]}]}`, n, n, n, n, n)
}

func testGetMapString4(n string) string {
	return fmt.Sprintf(`{"version":1,"partitions":[
    {"topic":"%s","partition":0,"replicas":[1004,1003]},
    {"topic":"%s","partition":1,"replicas":[1003,1004]},
    {"topic":"%s","partition":2,"replicas":[1001,1002]},
		{"topic":"%s","partition":3,"replicas":[1003,1002]},
		{"topic":"%s","partition":4,"replicas":[1001,1003]},
    {"topic":"%s","partition":5,"replicas":[1002,1001]}]}`, n, n, n, n, n, n)
}

func testGetMapString5(n string) string {
	return fmt.Sprintf(`{"version":1,"partitions":[
    {"topic":"%s1","partition":0,"replicas":[1004,1003]},
    {"topic":"%s1","partition":1,"replicas":[1003,1004]},
    {"topic":"%s1","partition":2,"replicas":[1001,1002]},
		{"topic":"%s2","partition":3,"replicas":[1003,1002]},
		{"topic":"%s2","partition":4,"replicas":[1001,1003]},
    {"topic":"%s2","partition":5,"replicas":[1002,1001]}]}`, n, n, n, n, n, n)
}

func TestSize(t *testing.T) {
	z := NewZooKeeperStub()

	pm, _ := z.GetPartitionMap("test_topic")
	pmm, _ := z.GetAllPartitionMeta()

	s, err := pmm.Size(pm.Partitions[0])
	if err != nil {
		t.Fatal(err)
	}

	if s != 1000.00 {
		t.Errorf("Expected size result 1000.00, got %f", s)
	}

	// Missing partition.
	delete(pmm["test_topic"], 3)
	_, err = pmm.Size(pm.Partitions[3])
	if err == nil {
		t.Error("Expected error")
	}

	// Missing topic.
	delete(pmm, "test_topic")
	_, err = pmm.Size(pm.Partitions[3])
	if err == nil {
		t.Error("Expected error")
	}
}

func TestSortBySize(t *testing.T) {
	z := NewZooKeeperStub()

	partitionMap, _ := z.GetPartitionMap("test_topic")
	partitionMetaMap, _ := z.GetAllPartitionMeta()

	partitionMap.Partitions.SortBySize(partitionMetaMap)

	expected := []int{3, 2, 1, 0}
	for i, p := range partitionMap.Partitions {
		if p.Partition != expected[i] {
			t.Errorf("Expected partition %d, got %d", expected[i], p.Partition)
		}
	}
}

func TestEqual(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	pm2, _ := PartitionMapFromString(testGetMapString("test_topic"))

	if same, _ := pm.Equal(pm2); !same {
		t.Error("Unexpected inequality")
	}

	// Test truncated partition list.
	pm.Partitions = pm.Partitions[:2]
	if same, _ := pm.Equal(pm2); same {
		t.Error("Unexpected equality")
	}

	pm, _ = PartitionMapFromString(testGetMapString("test_topic"))

	// Test version.
	pm.Version = 2
	if same, _ := pm.Equal(pm2); same {
		t.Error("Unexpected equality")
	}
	pm.Version = 1

	pm, _ = PartitionMapFromString(testGetMapString("test_topic"))

	// Test topic order.
	pm.Partitions[1].Topic = "test_topic2"
	if same, _ := pm.Equal(pm2); same {
		t.Error("Unexpected equality")
	}

	// Test partition order.
	pm.Partitions[0], pm.Partitions[1] = pm.Partitions[1], pm.Partitions[0]
	if same, _ := pm.Equal(pm2); same {
		t.Error("Unexpected equality")
	}

	pm, _ = PartitionMapFromString(testGetMapString("test_topic"))

	// Test replica list.
	pm.Partitions[0].Replicas = pm.Partitions[0].Replicas[:1]
	if same, _ := pm.Equal(pm2); same {
		t.Error("Unexpected equality")
	}

	pm, _ = PartitionMapFromString(testGetMapString("test_topic"))

	// Test replicas.
	pm.Partitions[0].Replicas[0] = 1337
	if same, _ := pm.Equal(pm2); same {
		t.Error("Unexpected equality")
	}
}

func TestPartitionMapTopics(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString5("test_topic"))
	ts := pm.Topics()

	expected := []string{"test_topic1", "test_topic2"}

	for i, n := range ts {
		if n != expected[i] {
			t.Errorf("Expected topic '%s', got '%s'", expected[i], n)
		}
	}
}

func TestPartitionMapReplicaSets(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	rs := pm.ReplicaSets("test_topic")

	expected := ReplicaSets{
		0: []int{1001, 1002},
		1: []int{1002, 1001},
		2: []int{1003, 1004, 1001},
		3: []int{1004, 1003, 1002},
	}

	if len(rs) != len(expected) {
		t.Error("Unexpected ReplicaSets len")
	}

	for i, expectedSet := range expected {
		for j := range expectedSet {
			if expectedSet[j] != rs[i][j] {
				fmt.Printf("%v %v\n", expectedSet[j], rs[i][j])
				t.Errorf("ReplicaSet mismatch")
			}
		}
	}
}

func TestPartitionMapCopy(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	pm2 := pm.Copy()

	if same, _ := pm.Equal(pm2); !same {
		t.Error("Unexpected inequality")
	}

	// After modifying the partitions list, we expect inequality.
	pm.Partitions = pm.Partitions[:2]
	if same, _ := pm.Equal(pm2); same {
		t.Error("Unexpected equality")
	}
}

func TestPartitionMapFromString(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	zk := NewZooKeeperStub()
	pm2, _ := zk.GetPartitionMap("test_topic")

	// We expect equality here.
	if same, _ := pm.Equal(pm2); !same {
		t.Error("Unexpected inequality")
	}
}

func TestSetReplication(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))

	pm.SetReplication(3)
	// All partitions should now have 3 replicas.
	for _, r := range pm.Partitions {
		if len(r.Replicas) != 3 {
			t.Errorf("Expected 3 replicas, got %d", len(r.Replicas))
		}
	}

	pm.SetReplication(2)
	// All partitions should now have 3 replicas.
	for _, r := range pm.Partitions {
		if len(r.Replicas) != 2 {
			t.Errorf("Expected 2 replicas, got %d", len(r.Replicas))
		}
	}

	pm.SetReplication(0)
	// Setting to 0 is a no-op.
	for _, r := range pm.Partitions {
		if len(r.Replicas) != 2 {
			t.Errorf("Expected 2 replicas, got %d", len(r.Replicas))
		}
	}

	pm.SetReplication(3)
	// Replica sets should be expanded with stub brokers
	for _, r := range pm.Partitions {
		if len(r.Replicas) != 3 {
			t.Errorf("Expected 3 replicas, got %d", len(r.Replicas))
		}

		for bIndex := 0; bIndex < 3; bIndex++ {
			if bIndex < 2 {
				if r.Replicas[bIndex] == StubBrokerID {
					t.Errorf("Expected existing replicas not to be stub brokers, got %d", r.Replicas[bIndex])
				}
			} else {
				if r.Replicas[bIndex] != StubBrokerID {
					t.Errorf("Expected extended replicas to be stub brokers, got %d", r.Replicas[bIndex])
				}
			}
		}
	}
}

func TestStrip(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))

	spm := pm.Strip()

	for _, p := range spm.Partitions {
		for _, b := range p.Replicas {
			if b != StubBrokerID {
				t.Errorf("Unexpected non-stub broker ID %d", b)
			}
		}
	}
}

func TestUseStats(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))

	s := pm.UseStats().List()

	expected := map[int][2]int{
		1001: {1, 2},
		1002: {1, 2},
		1003: {1, 1},
		1004: {1, 1},
	}

	for _, b := range s {
		if b.Leader != expected[b.ID][0] {
			t.Errorf("Expected leader count %d for %d, got %d",
				expected[b.ID][0], b.ID, b.Leader)
		}

		if b.Follower != expected[b.ID][1] {
			t.Errorf("Expected follower count %d for %d, got %d",
				expected[b.ID][1], b.ID, b.Follower)
		}
	}
}

// Count rebuild.
func TestRebuildByCount(t *testing.T) {
	forceRebuild := true
	withMetrics := false

	zk := NewZooKeeperStub()
	bm, _ := zk.GetAllBrokerMeta(withMetrics)
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	pmm := NewPartitionMetaMap()
	brokers := BrokerMapFromPartitionMap(pm, bm, forceRebuild)

	rebuildParams := RebuildParams{
		PMM:          pmm,
		BM:           brokers,
		Strategy:     "count",
		Optimization: "distribution",
	}

	out, errs := pm.Rebuild(rebuildParams)
	if errs != nil {
		t.Errorf("Unexpected error(s): %s", errs)
	}

	// This rebuild should be a no-op since all brokers already in the map were
	// provided, none marked as replace.
	if same, _ := pm.Equal(out); !same {
		t.Error("Expected no-op, partition map changed")
	}

	// Mark 1004 for replacement.
	rebuildParams.BM[1004].Replace = true

	// Rebuild.
	out, errs = pm.Rebuild(rebuildParams)
	if errs != nil {
		t.Errorf("Unexpected error(s): %s", errs)
	}

	// Expected map after a replacement rebuild.
	expected, _ := PartitionMapFromString(testGetMapString("test_topic"))
	expected.Partitions[2].Replicas = []int{1003, 1002, 1001}
	expected.Partitions[3].Replicas = []int{1001, 1003, 1002}

	if same, err := out.Equal(expected); !same {
		t.Errorf("Unexpected inequality after broker replacement: %s", err)
	}

	// Test a rebuild with a change in replication factor.
	pm.SetReplication(2)
	expected.SetReplication(2)

	out, _ = pm.Rebuild(rebuildParams)

	if same, err := out.Equal(expected); !same {
		t.Errorf("Unexpected inequality after replication factor change -> rebuild: %s", err)
	}

	// Test a force rebuild.
	forceRebuild = true
	pm, _ = PartitionMapFromString(testGetMapString2("test_topic"))
	pmStripped := pm.Strip()
	rebuildParams.BM = BrokerMapFromPartitionMap(pm, bm, forceRebuild)

	out, _ = pmStripped.Rebuild(rebuildParams)

	expected = pm.Copy()
	expected.Partitions[0].Replicas = []int{1001, 1003}
	expected.Partitions[1].Replicas = []int{1002, 1004}
	expected.Partitions[2].Replicas = []int{1003, 1001}
	expected.Partitions[3].Replicas = []int{1004, 1002}
	expected.Partitions[4].Replicas = []int{1001, 1002}
	expected.Partitions[5].Replicas = []int{1002, 1004}
	expected.Partitions[6].Replicas = []int{1003, 1001}

	same, err := out.Equal(expected)
	if !same {
		t.Errorf("Unexpected inequality after force rebuild: %s", err)
	}
}

// Count rebuild with substitution affinities.
func TestRebuildByCountSA(t *testing.T) {
	forceRebuild := true
	withMetrics := false

	zk := NewZooKeeperStub()
	bm, _ := zk.GetAllBrokerMeta(withMetrics)
	// Simulate that we've lost broker 1002.
	delete(bm, 1002)

	pm, _ := PartitionMapFromString(testGetMapString4("test_topic"))
	// Until https://github.com/DataDog/kafka-kit/issues/187 is closed, we need to
	// pretend another broker with rack b was present.
	pm.Partitions[2].Replicas = []int{1001, 1005}

	pmm := NewPartitionMetaMap()
	brokers := BrokerMapFromPartitionMap(pm, bm, forceRebuild)

	// simulate that we've found broker 1010.
	bm[1010] = &BrokerMeta{Rack: "b"}
	brokers.Update([]int{1001, 1003, 1004, 1005, 1010}, bm)

	// Get substitution affinities.
	sa, err := brokers.SubstitutionAffinities(pm)
	if err != nil {
		t.Fatal(err)
	}

	rebuildParams := RebuildParams{
		PMM:          pmm,
		BM:           brokers,
		Strategy:     "count",
		Optimization: "distribution",
		Affinities:   sa,
	}

	// Rebuild.
	out, errs := pm.Rebuild(rebuildParams)
	if errs != nil {
		t.Errorf("Unexpected error(s): %s", errs)
	}

	expected, _ := PartitionMapFromString(testGetMapString4("test_topic"))
	expected.Partitions[0].Replicas = []int{1004, 1003}
	expected.Partitions[1].Replicas = []int{1003, 1004}
	expected.Partitions[2].Replicas = []int{1001, 1005}
	expected.Partitions[3].Replicas = []int{1003, 1010}
	expected.Partitions[4].Replicas = []int{1001, 1003}
	expected.Partitions[5].Replicas = []int{1010, 1001}

	if same, err := out.Equal(expected); !same {
		t.Errorf("Unexpected inequality after rebuild: %s", err)
	}
}

// Storage rebuild, distribution optimization.
func TestRebuildByStorageDistribution(t *testing.T) {
	forceRebuild := true
	withMetrics := true

	zk := NewZooKeeperStub()
	bm, _ := zk.GetAllBrokerMeta(withMetrics)
	pm, _ := PartitionMapFromString(testGetMapString4("test_topic"))
	pmm, _ := zk.GetAllPartitionMeta()

	// We need to reduce the test partition sizes for more accurate tests here.
	for _, partn := range pmm["test_topic"] {
		partn.Size = partn.Size / 3
	}

	brokers := BrokerMapFromPartitionMap(pm, bm, forceRebuild)

	pmStripped := pm.Strip()
	allBrokers := func(b *Broker) bool { return true }
	_ = brokers.SubStorage(pm, pmm, allBrokers)

	// Normalize storage. The stub broker storage free vs stub partition sizes
	// would actually represent brokers with varying storage sizes.
	for _, b := range brokers {
		b.StorageFree = 6000.00
	}

	rebuildParams := RebuildParams{
		PMM:           pmm,
		BM:            brokers,
		Strategy:      "storage",
		Optimization:  "distribution",
		PartnSzFactor: 1,
	}

	out, errs := pmStripped.Rebuild(rebuildParams)
	if errs != nil {
		t.Errorf("Unexpected error(s): %s", errs)
	}

	expected := pm.Copy()
	expected.Partitions[0].Replicas = []int{1003, 1001}
	expected.Partitions[1].Replicas = []int{1004, 1002}
	expected.Partitions[2].Replicas = []int{1004, 1003}
	expected.Partitions[3].Replicas = []int{1002, 1003}
	expected.Partitions[4].Replicas = []int{1003, 1004}
	expected.Partitions[5].Replicas = []int{1001, 1002}

	same, err := out.Equal(expected)
	if !same {
		t.Errorf("Unexpected inequality after rebuild: %s", err)
	}
}

// Storage rebuild, storage optimization.
func TestRebuildByStorageStorage(t *testing.T) {
	forceRebuild := true
	withMetrics := true

	zk := NewZooKeeperStub()
	bm, _ := zk.GetAllBrokerMeta(withMetrics)
	pm, _ := PartitionMapFromString(testGetMapString4("test_topic"))
	pmm, _ := zk.GetAllPartitionMeta()

	// We need to reduce the test partition sizes for more accurate tests here.
	for _, partn := range pmm["test_topic"] {
		partn.Size = partn.Size / 3
	}

	brokers := BrokerMapFromPartitionMap(pm, bm, forceRebuild)

	pmStripped := pm.Strip()
	allBrokers := func(b *Broker) bool { return true }
	_ = brokers.SubStorage(pm, pmm, allBrokers)

	// Normalize storage. The stub broker storage free vs stub partition sizes
	// would actually represent brokers with varying storage sizes.
	for _, b := range brokers {
		b.StorageFree = 6000.00
	}

	rebuildParams := RebuildParams{
		PMM:           pmm,
		BM:            brokers,
		Strategy:      "storage",
		Optimization:  "storage",
		PartnSzFactor: 1,
	}

	out, errs := pmStripped.Rebuild(rebuildParams)
	if errs != nil {
		t.Errorf("Unexpected error(s): %s", errs)
	}

	expected := pm.Copy()
	expected.Partitions[0].Replicas = []int{1002, 1001}
	expected.Partitions[1].Replicas = []int{1003, 1004}
	expected.Partitions[2].Replicas = []int{1002, 1001}
	expected.Partitions[3].Replicas = []int{1004, 1003}
	expected.Partitions[4].Replicas = []int{1003, 1004}
	expected.Partitions[5].Replicas = []int{1001, 1002}

	same, err := out.Equal(expected)
	if !same {
		t.Errorf("Unexpected inequality after rebuild: %s", err)
	}
}

func TestLocalitiesAvailable(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	bm := newStubBrokerMap()

	pm.SetReplication(2)

	localities := pm.LocalitiesAvailable(bm, bm[1001])

	expected := []string{"a", "c"}
	for i, l := range localities {
		if expected[i] != l {
			t.Error("Unexpected localities available")
		}
	}
}

func TestOptimizeLeaderFollower(t *testing.T) {
	f, _ := ioutil.ReadFile("testdata/optimize_input.json")
	optimized, err := PartitionMapFromString(string(f))
	if err != nil {
		t.Error(err)
	}

	f, _ = ioutil.ReadFile("testdata/optimize_output.json")
	expected, err := PartitionMapFromString(string(f))
	if err != nil {
		t.Error(err)
	}

	optimized.OptimizeLeaderFollower()

	if equal, _ := optimized.Equal(expected); !equal {
		t.Errorf("Unexpected OptimizeLeaderFollower results")
	}
}

func TestShuffle(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))

	expected := &PartitionMap{
		Version: 1,
		Partitions: PartitionList{
			Partition{
				Topic:     "test_topic",
				Partition: 0,
				Replicas:  []int{1001, 1002},
			},
			Partition{
				Topic:     "test_topic",
				Partition: 1,
				Replicas:  []int{1001, 1002},
			},
			Partition{
				Topic:     "test_topic",
				Partition: 2,
				Replicas:  []int{1004, 1003, 1001},
			},
			Partition{
				Topic:     "test_topic",
				Partition: 3,
				Replicas:  []int{1002, 1004, 1003},
			},
		},
	}

	pm.shuffle((func(_ Partition) bool { return true }))

	if same, _ := pm.Equal(expected); !same {
		t.Errorf("Unexpected shuffle results")
	}
}
