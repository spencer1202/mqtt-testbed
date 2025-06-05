// Policy Enforcement Agent
//
// Proof-of-concept prototype
// This is the first baby-step toward inserting a Policy Enforcement Agent (PEA) into a Mochi MQTT agent.
// We're proving here that we can insert a policy management agent using the broker event hooks, use those
// to control whether subscribers can get access to topics and/or published messages selectively, and even
// do so based on the content of the message payloads themselves. We'll also demo transforming the message
// payload.
//
// For now we aren't building a general-purpose policy manager which has to parse attribute expressions
// and all that yet, so the policies themselves are hard-coded in the compiled code here. We're just showing
// how an agent like this can be inserted into an MQTT broker in principle and what the baseline message
// latency impact of it is, before we start expanding on the concept further by adding more and more features
// on top of this.
package policy

import (
	"bytes"
	"encoding/json"
	"fmt"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

type PEAHook struct {
	mqtt.HookBase
}

func (h PEAHook) ID() string {
	return "PEA"
}

func (h PEAHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnConnectAuthenticate,
		mqtt.OnACLCheck,
		mqtt.OnSubscribe,
		mqtt.OnSubscribed,
		mqtt.OnPacketEncode,
	}, []byte{b})
}

func showPacket(pk packets.Packet) {
	fmt.Printf("Connect\n")
	fmt.Printf(" Username %s Password %s ID %s pwd %v usr %v\n",
		string(pk.Connect.Username), string(pk.Connect.Password),
		pk.Connect.ClientIdentifier,
		pk.Connect.PasswordFlag, pk.Connect.UsernameFlag,
	)
	fmt.Printf("Topic %s\n", pk.TopicName)
	fmt.Printf("Content %s Reason %s\n",
		pk.Connect.WillProperties.ContentType,
		pk.Connect.WillProperties.ReasonString,
	)
}

func showClient(cl *mqtt.Client) {
	if cl == nil {
		fmt.Println("no client")
	} else {
		fmt.Printf("Client: Username %s ID %s\n",
			string(cl.Properties.Username), cl.ID)
	}
}

func (h PEAHook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	fmt.Println("AUTHENTICATE")
	showClient(cl)
	showPacket(pk)

	// in our test, publishers don't have creds so we let them in
	u := string(pk.Connect.Username)
	p := string(pk.Connect.Password)
	if u == "" && p == "" {
		return true
	}
	for _, ps := range policyStore {
		if u == ps.username && p == ps.password {
			return true
		}
	}
	return false
}

func (h PEAHook) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	fmt.Println("SUBSCRIBE")
	showClient(cl)
	showPacket(pk)
	return pk
}

func (h PEAHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	fmt.Println("SUBSCRIBED", reasonCodes)
	showClient(cl)
	showPacket(pk)
}

func (h PEAHook) OnPacketEncode(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	if pk.TopicName == "temperature/basement" {
		var d map[string]any
		if err := json.Unmarshal(pk.Payload, &d); err != nil {
			fmt.Printf("FAIL %v\n", err)
			return pk
		}
		temp, ok := d["temperature"]
		if ok {
			ftemp, ok := temp.(float64)
			if ok && ftemp > 30.0 {
				fmt.Printf("ENCODE temp=%v DENY\n", ftemp)
				fmt.Printf("redacting %d bytes\n", len(pk.Payload))
				for i := 0; i < len(pk.Payload); i++ {
					pk.Payload[i] = 0
				}
				return pk
			}
			fmt.Printf("ENCODE temp=%v OK\n", temp)
		}
	}

	return pk
}

func (h PEAHook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	fmt.Println("ACL OK", topic, write, string(cl.Properties.Username))
	if !write {
		// hardcoded rules for now
		u := string(cl.Properties.Username)
		if topic == "lamp/1" && u != "user1" {
			fmt.Println("DENY")
			return false
		}
		if topic == "lamp/2" && u != "user2" {
			fmt.Println("DENY")
			return false
		}
		if topic == "freezer" && u != "user2" {
			fmt.Println("DENY")
			return false
		}
	}
	return true
}

// Hardwired test policy for proof of concept until we have a general-purpose
// policy input method and user interface built
//
// first cut: without a more sophisticated full-scale policy definition, let's
// identify user profile just by user/password.
//

type policyDefinition struct {
	username string
	password string
}

var policyStore = []policyDefinition{
	{
		username: "user1",
		password: "password1",
	},
	{
		username: "user2",
		password: "password2",
	},
	{
		username: "user3",
		password: "password3",
	},
}
