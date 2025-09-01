package main

import (
	"fmt"
	"strings"
)

func main() {
	topics := []string{
		"A/B/C",
		"A/B/#",
		"A/B/Z",
		"A/+/C",
		"A/+/#",
		"A/Y/Z",
		"X/B/C",
		"+/B/C",
		"+/B",
		"X/Y/Z",
		"X/#",
		"+/+/#",
	}

	//test(topics, "\"%v\" is a subset of \"%v\"\n", "\"%v\" is NOT a subset of \"%v\"\n", IsSubset)
	//test(topics, "\"%v\" is a superset of \"%v\"\n", "\"%v\" is NOT a superset of \"%v\"\n", IsSuperset)
	testing(topics)
}

func testing(topics []string) {

	runTest := func(topics []string, msgTrue string, msgFalse string, op func(string, string) (bool, error)) {
		for _, str1 := range topics {
			for _, str2 := range topics {
				if result, _ := op(str1, str2); result {
					fmt.Printf(msgTrue, str1, str2)
				} else {
					fmt.Printf((msgFalse), str1, str2)
				}
			}
		}
	}

	fmt.Printf("\nTesting IsSubset...\n")
	runTest(topics, "\"%v\" is a subset of \"%v\"\n", "\"%v\" is NOT a subset of \"%v\"\n", IsSubset)
	fmt.Printf("\nTesting IsSuperset...\n")
	runTest(topics, "\"%v\" is a superset of \"%v\"\n", "\"%v\" is NOT a superset of \"%v\"\n", IsSuperset)
}

// Returns true if the topic in op1 is a subset of op2
func IsSubset(op1 string, op2 string) (bool, error) {
	//string is empty
	if len(op1) == 0 || len(op2) == 0 {
		var err error
		return false, err
	}
	//strings are equal
	if op1 == op2 {
		return true, nil
	}

	return isSubset(strings.TrimSpace(op1), strings.TrimSpace(op2))
}

// Recursive function
func isSubset(op1 string, op2 string) (bool, error) {
	current1, next1, _ := strings.Cut(op1, "/")
	current2, next2, _ := strings.Cut(op2, "/")

	//Case 1: both strings are empty
	if len(current1) == 0 && len(current2) == 0 {
		return true, nil
	}
	if current1 != current2 {
		//Case 2: one of the strings is empty
		if len(current1) == 0 || len(current2) == 0 {
			return false, nil
		}
		//Case 3: string2 is a # wildcard
		if current2 == "#" {
			var err error
			//string2 has topics after the wildcard
			if len(next2) > 0 {
				return false, err
			}
			return true, nil
		}
		//Case 4: strings aren't equal, and string2 isn't a + wildcard
		if current2 != "+" {
			return false, nil
		}
	}
	return isSubset(next1, next2)
}

func IsSuperset(op1 string, op2 string) (bool, error) {
	return IsSubset(op2, op1)
}
