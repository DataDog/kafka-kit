package kafkaadmin_test

import (
	"regexp"
	"sort"
	"testing"

	"github.com/DataDog/kafka-kit/v3/kafkaadmin"
	"github.com/DataDog/kafka-kit/v3/kafkazk"
	"github.com/stretchr/testify/assert"
)

/*
	USE:  go test -v ./...
	Some test FAIL on purpose...

	GREEN test ---> Needs to PASS
	RED test ---> Needs to FAIL
*/
var (
	zkaddr     = "localhost:2181"
	zkprefix   = ""
	kafkaddr   = "localhost:9093" //"localhost:9092,localhost:9093",
	kafkprefix = ""
)

/*
Setup kafka config and create a Client for a test
*/
func SetupClient() (kafkaadmin.KafkaAdmin, error) {
	cfk := &kafkaadmin.Config{
		BootstrapServers: kafkaddr,
		Prefix:           kafkprefix,
	}

	kafk, err := kafkaadmin.NewClient(*cfk)
	if err != nil {
		return nil, err
	}
	return kafk, nil
}

/*
Setup Zookeeper config and create a ZkHandler for a test
*/
func SetupZk() (kafkazk.Handler, error) {
	cfg := &kafkazk.Config{
		Connect: zkaddr,
		Prefix:  zkprefix,
	}

	zkh, err := kafkazk.NewHandler(cfg)
	if err != nil {

		return nil, err
	}
	return zkh, nil
}

func TestGetTopics(t *testing.T) {
	//PASS - expected: ALL topics
	t.Run("T Green Test 1", func(t *testing.T) {
		expected := []string{ //We look forward to all the topics we have at kafka.(add yours topics)
			"__consumer_offsets", //same as with zookeeper
			"test",
			"test1",
			"test12",
			"test21",
			"test22",
			"test23",
			"testconfig",
			"testmeta",
			"testmeta2",
			"testp",
		}
		//setupclients
		ClientTest, er := SetupClient()
		if er != nil {
			t.Fail()
		}
		got, err := ClientTest.GetTopics()
		if err != nil {
			t.Fail()
		}
		//sort
		sort.Strings(got)
		sort.Strings(expected)

		if len(got) != len(expected) { //with len
			t.Fail()
		}

		for i := range expected { //one by one
			if got[i] != expected[i] {
				//t.Errorf("Got: %s \n expected: %s", got, expected)
				t.Fail()
			}
		}
	})

	//FAIL - No expected nothing
	t.Run("T Red Test 1", func(t *testing.T) {
		expected := []string{ //We expect nothing having "topics" in kafka
		}
		ClientTest, er := SetupClient()
		if er != nil {
			t.Fail()
		}
		got, err := ClientTest.GetTopics()
		if err != nil {
			t.Fail()
		}
		//sort strings
		sort.Strings(got)
		sort.Strings(expected)

		if len(got) != len(expected) { //len
			t.Fail()
		}
		for i := range expected { //one by one
			if got[i] != expected[i] {
				t.Fail()
			}
		}

	})

	//FAIL - ALL topics, except "one"
	t.Run("T Red Test 2", func(t *testing.T) {
		expected := []string{ //We look forward to all the topics we have at kafka, EXCEPT ONE .(add yours topics)
			"__consumer_offsets", //same as with zookeeper
			"test",
			"test1",
			"test12",
			"test21",
			"test22",
			"test23",
			"testconfig",
			"testmeta",
			"testmeta2",
		}
		ClientTest, er := SetupClient()
		if er != nil {
			t.Fail()
		}
		got, err := ClientTest.GetTopics()
		if err != nil {
			t.Fail()
		}
		sort.Strings(got)
		sort.Strings(expected)

		if len(got) != len(expected) {
			//t.Errorf("Got: %s \n expexted: %s", got, expected)
			t.Fail()
		}

		for i := range expected { //one by one
			if got[i] != expected[i] {
				//t.Errorf("Got: %s \n expexted: %s", got, expected)
				t.Fail()
			}
		}

	})

	//zkhandler.GetTopics == kafkaclient.GetTopics
	t.Run("T- Zk == Kafka", func(t *testing.T) {
		//zk handler
		var allTopicsRegexp = regexp.MustCompile(".*")
		Zkhandler, _ := SetupZk()
		expected, _ := Zkhandler.GetTopics([]*regexp.Regexp{allTopicsRegexp})
		sort.Strings(expected) //Sort Expected zk
		//Kafka Client
		ClientTest, er := SetupClient()
		if er != nil {
			t.Fail()
		}
		got, err := ClientTest.GetTopics()
		if err != nil {
			t.Fail()
		}
		sort.Strings(got) //Sort got Kafka client

		for i := range expected { //one by one all topics.
			if got[i] != expected[i] {
				t.Fail()
			}
		}

	})
}

func TestGetTopicState(t *testing.T) {
	//Exist topic name no error
	t.Run("TS Green Test 1", func(t *testing.T) {
		ClientTest, er := SetupClient()
		if er != nil {
			t.Fail()
		}
		_, err := ClientTest.GetTopicState("test") //
		if err != nil {
			t.Fail()
		}

	})

	//Exist topic partitions and are the same [all]
	t.Run("TS Green Test 2", func(t *testing.T) {
		type TopicState struct {
			Partitions map[string][]int `json:"partitions"`
		}

		temp := make(map[string][]int)
		//expected partitions - "to declare"
		temp["0"] = []int{3, 1} //Partitions 0  more Replicas {1, 2,.., m}
		temp["1"] = []int{1, 3} //..
		temp["2"] = []int{3, 1}
		temp["3"] = []int{1, 3} //Partitions N

		testts := &TopicState{Partitions: temp}

		ClientTest, er := SetupClient()
		if er != nil {
			t.Fail()
		}
		ts, err := ClientTest.GetTopicState("test23") //
		if err != nil {
			t.Fail()
		}
		//same key same values
		for k := range temp {
			assert.EqualValues(t, testts.Partitions[k], ts.Partitions[k]) //are equals, if one != return error
		}

	})

	//NO EXIST topic, and return error
	t.Run("TS Red Test 1", func(t *testing.T) {
		ClientTest, er := SetupClient()
		if er != nil {
			t.Fail()
		}
		_, err := ClientTest.GetTopicState("Notopic")
		if err != nil {
			t.Fail()
		}

	})

	//Exist topic partitions and are the same [all] - except "one" partitions
	t.Run("TS Red Test 2", func(t *testing.T) {
		type TopicState struct {
			Partitions map[string][]int `json:"partitions"`
		}

		temp := make(map[string][]int)
		//expected Partitions - "to declare"
		temp["0"] = []int{3, 1} //Partitions 0  more Replicas {1, 2,.., m}
		temp["1"] = []int{1, 3} //..
		// NO partitions 2 == //temp["2"] = []int{3, 1}
		temp["3"] = []int{1, 3} //Partitions N

		testts := &TopicState{Partitions: temp}

		ClientTest, er := SetupClient()
		if er != nil {
			t.Fail()
		}
		ts, err := ClientTest.GetTopicState("test23") //
		if err != nil {
			t.Fail()
		}
		//same key same values
		for k := range ts.Partitions {
			assert.EqualValues(t, testts.Partitions[k], ts.Partitions[k]) //are equals, if one != return error
		}

	})

	//zkhandler.GetTopicState == kafkaclient.GetTopicState ?
	t.Run("TS- Zk == Kafka", func(t *testing.T) {
		//zk handler
		Zkhandler, _ := SetupZk()
		expected, _ := Zkhandler.GetTopicState("test")
		//Kafka Client
		ClientTest, _ := SetupClient()
		ts, err := ClientTest.GetTopicState("test")
		if err != nil {
			t.Fail()
		}
		for k := range expected.Partitions {
			assert.EqualValues(t, expected.Partitions[k], ts.Partitions[k]) //are equals for the same key, if one != return error
		}
	})

}
