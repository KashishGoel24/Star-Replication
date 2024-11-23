package main

import (
	"bufio"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/anishathalye/porcupine"
)

type crInputOutput struct {
	op    bool
	key   string
	value string
}

type crState map[string]string

func cloneMap(m crState) crState {
	m2 := make(crState)
	for k, v := range m {
		m2[k] = v
	}
	return m2
}

var crModel = porcupine.Model{
	Init: func() interface{} {
		return make(crState)
	},

	Step: func(state, input, output interface{}) (bool, interface{}) {
		m := state.(crState)
		in := input.(crInputOutput)

		if in.op {
			// Put
			m2 := cloneMap(m)
			m2[in.key] = in.value
			return true, m2
		} else {
			// Get
			out := output.(crInputOutput)
			if val, ok := m[out.key]; ok {
				return val == out.value, state
			} else {
				// Initial default value is "0"
				return out.value == "0", state
			}
		}
	},

	Equal: func(state1, state2 interface{}) bool {
		return reflect.DeepEqual(state1, state2)
	},

	// Add descriptions for visualization
	DescribeOperation: func(input, output interface{}) string {
		in := input.(crInputOutput)
		out := output.(crInputOutput)
		if in.op {
			return fmt.Sprintf("set(%s, %s)", in.key, in.value)
		} else {
			return fmt.Sprintf("get(%s) → %s", in.key, out.value)
		}
	},

	DescribeState: func(state interface{}) string {
		m := state.(crState)
		return fmt.Sprintf("%v", m)
	},
}

func getWorkerId(matches []string) int {
	workerId, err := strconv.Atoi(matches[1])
	if err != nil {
		fmt.Println("Error parsing worker id: ", err)
		os.Exit(1)
	}
	return workerId
}

func parseLog(filename string) []porcupine.Event {
	// Read log file
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file: ", err)
		os.Exit(1)
	}
	defer file.Close()

	// Parse log file
	var events []porcupine.Event

	// Regular expression to capture INFO level logs for setter and getter with key and value
	reSetter := regexp.MustCompile(`INFO\s+worker_(?P<worker_id>\d+)\s+Setting\s(?P<key>\w+)\s=\s(?P<value>\d+)`)
	reSetterSet := regexp.MustCompile(`INFO\s+worker_(?P<worker_id>\d+)\s+Set\s(?P<key>\w+)\s=\s(?P<value>\d+)`)
	reGetterGet := regexp.MustCompile(`INFO\s+worker_(?P<worker_id>\d+)\s+Get\s(?P<key>\w+)\s=\s(?P<value>\d+)`)
	reGetter := regexp.MustCompile(`INFO\s+worker_(?P<worker_id>\d+)\s+Getting\s(?P<key>\w+)`)

	id := 0
	procIdMap := make(map[int]int)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		switch {
		case reSetter.MatchString(line):
			matches := reSetter.FindStringSubmatch(line)
			workerId := getWorkerId(matches)
			events = append(events, porcupine.Event{ClientId: workerId, Kind: porcupine.CallEvent, Value: crInputOutput{true, matches[2], matches[3]}, Id: id})
			procIdMap[workerId] = id
			id++
		case reSetterSet.MatchString(line):
			matches := reSetterSet.FindStringSubmatch(line)
			workerId := getWorkerId(matches)
			events = append(events, porcupine.Event{ClientId: workerId, Kind: porcupine.ReturnEvent, Value: crInputOutput{true, matches[2], matches[3]}, Id: procIdMap[workerId]})
			delete(procIdMap, workerId)
		case reGetter.MatchString(line):
			matches := reGetter.FindStringSubmatch(line)
			workerId := getWorkerId(matches)
			events = append(events, porcupine.Event{ClientId: workerId, Kind: porcupine.CallEvent, Value: crInputOutput{false, matches[2], ""}, Id: id})
			procIdMap[workerId] = id
			id++
		case reGetterGet.MatchString(line):
			matches := reGetterGet.FindStringSubmatch(line)
			workerId := getWorkerId(matches)
			events = append(events, porcupine.Event{ClientId: workerId, Kind: porcupine.ReturnEvent, Value: crInputOutput{false, matches[2], matches[3]}, Id: procIdMap[workerId]})
			delete(procIdMap, workerId)
		}
	}

	return events
}

func checkLinearizabilityWithVisualization(filename string) bool {
	fmt.Println("Checking linearizability of log file: ", filename)

	events := parseLog(filename)
	if len(events) == 0 {
		fmt.Println("No events found in log file!")
		return false
	}

	// Use CheckEventsVerbose with a timeout
	timeout := 10 * time.Second
	result, info := porcupine.CheckEventsVerbose(crModel, events, timeout)

	// Create visualization filename based on input log
	visualizationFile := filename + ".visualization.html"
	
	// Add some annotations for interesting events
	annotations := []porcupine.Annotation{
		{
			ClientId:        0,
			Start:          0,
			End:            0,
			Description:    "Test Start",
			TextColor:      "#ffffff",
			BackgroundColor: "#4CAF50",
		},
	}
	info.AddAnnotations(annotations)

	// Generate visualization
	err := porcupine.VisualizePath(crModel, info, visualizationFile)
	if err != nil {
		fmt.Printf("Error generating visualization: %v\n", err)
	} else {
		fmt.Printf("Visualization saved to: %s\n", visualizationFile)
	}

	// Print partial linearizations if available
	partialLinearizations := info.PartialLinearizations()
	if len(partialLinearizations) > 0 {
		fmt.Println("\nPartial linearizations found:")
		for i, partition := range partialLinearizations {
			fmt.Printf("Partition %d:\n", i)
			for j, sequence := range partition {
				fmt.Printf("  Sequence %d: %v\n", j, sequence)
			}
		}
	}

	if result == porcupine.Ok {
		fmt.Println("Linearizability check passed!")
		return true
	} else if result == porcupine.Unknown {
		fmt.Println("Linearizability check timed out!")
		return false
	} else {
		fmt.Println("Linearizability check failed!")
		return false
	}
}

func main() {
	fmt.Println("Testing linearizability with Porcupine...")

	if len(os.Args) != 2 {
		fmt.Println("Usage: go run main.go <log-file-path>")
		os.Exit(1)
	}

	filename := os.Args[1]
	fmt.Println("Reading log file: ", filename)

	checkLinearizabilityWithVisualization(filename)
}
