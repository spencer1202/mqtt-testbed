// Policy Enforcement Agent
//
// # Proof-of-concept prototype
//
// This is the first baby-step toward inserting a Policy Enforcement Agent (PEA) into a Mochi MQTT agent.
// We're proving here that we can insert a policy management agent using the broker event hooks, use those
// to control whether subscribers can get access to topics and/or published messages selectively, and even
// do so based on the content of the message payloads themselves. We'll also demo transforming the message
// payload.
//
// The full-scale functioning PEA unit is under construction but so far this is a demonstration
// piece to show what the interface between the PEA and broker looks like.
// This simple PoC demo doesn't manage a policy store or parse attribute expressions
// and all that yet, so the policies themselves are hard-coded in the compiled code here. We're just showing
// how an agent like this can be inserted into an MQTT broker in principle and what the baseline message
// latency impact of it is, before we start expanding on the concept further by adding more and more features
// on top of this.
package policy

import (
	"bytes"
	"fmt"
	"log"

	"database/sql"

	"github.com/go-sql-driver/mysql"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

type Options struct {
	Options *mysql.Config
}

// Placeholder function that se
func (op *Options) InitDefault() {
	op.Options = mysql.NewConfig()
	op.Options.User = "policy_db"
	op.Options.Passwd = "mqtt_pea"
	op.Options.Net = "tcp"
	op.Options.Addr = "127.0.0.1:3306"
	op.Options.DBName = "policy_db"
}

// PEAHook defines our Policy Enforcement Agent type as a hook into the Mochi MQTT broker
// to intercept telemetry traffic events at any points in the processing we choose.
type PEAHook struct {
	mqtt.HookBase
	policy_db *sql.DB
	config    *Options
}

// Called when the hook is created and attached by the broker.
// This is where we open a connection to the database.
func (h PEAHook) Init(config any) error {
	h.config = new(Options)
	h.config.InitDefault()
	var err error
	h.policy_db, err = sql.Open("mysql", h.config.Options.FormatDSN())
	if err != nil {
		return fmt.Errorf("failed to open connection to MySQL server: %v", err)
	}
	if err = h.policy_db.Ping(); err != nil {
		return fmt.Errorf("failed to ping MySQL server: %v", err)
	}
	return nil
}

func (h PEAHook) Stop() error {
	log.Println("disconnecting from MySQL server")
	return h.policy_db.Close()
}

// ID identifies our hook to other interested subsystems.
func (h PEAHook) ID() string {
	return "PEA"
}

// Provides informs the broker which event handlers our hook provides.
// In other words, we will be intercepting packets at these points in their
// path through the broker, and these methods will be invoked via our
// hooks accordingly by the broker.
func (h PEAHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnConnectAuthenticate,
		mqtt.OnACLCheck,
		mqtt.OnSubscribe,
		mqtt.OnSubscribed,
		mqtt.OnPacketEncode,
	}, []byte{b})
}

// showPacket is an internal debugging function used while developing
// the code to examine attributes of the packets.
func showPacket(pk packets.Packet) {
	log.Printf("Connect")
	log.Printf(" Username %s Password %s ID %s pwd %v usr %v",
		string(pk.Connect.Username), string(pk.Connect.Password),
		pk.Connect.ClientIdentifier,
		pk.Connect.PasswordFlag, pk.Connect.UsernameFlag,
	)
	log.Printf("Topic %s", pk.TopicName)
	log.Printf("Content %s Reason %s",
		pk.Connect.WillProperties.ContentType,
		pk.Connect.WillProperties.ReasonString,
	)
}

// showClient is an internal debugging function used while developing
// the code to examing attributes of the clients involved in communication
// with the broker.
func showClient(cl *mqtt.Client) {
	if cl == nil {
		log.Println("no client")
	} else {
		log.Printf("Client: Username %s ID %s",
			string(cl.Properties.Username), cl.ID)
	}
}

// OnConnectAuthenticate is an event handler hook method which intercepts
// an incoming MQTT client (publisher or subscriber) who wishes to join the
// broker. We can decide to allow or deny the connection here by returning
// a boolean true or false value respectively.
//
// For our PoC testcase, we will simply allow unauthenticated clients since
// all our publishers arrive that way, and validate the credentials of
// the others against our hardcoded list for subscribers.
//
// In a production system, obviously this will be replaced with an actual
// authentication system which checks the subject and object attibutes
// presented against what is allowed by policy.
func (h PEAHook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	log.Println("AUTHENTICATE")
	showClient(cl)
	showPacket(pk)

	// TODO: validate credentials based on authentication rules
	return true
}

// OnSubscribe is an event handler hook method which intercepts an incoming MQTT client
// subscriber wishing to subscribe to an MQTT topic pattern. We have the opportunity
// here to alter their request packet before returning it back to the broker.
// In this PoC test we won't, because we're demonstrating publish-time policy enforcement
// but for subscribe-time enforcement we could impose a number of subscription rewrite
// transformations.
func (h PEAHook) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	log.Println("SUBSCRIBE")
	showClient(cl)
	showPacket(pk)
	return pk
}

// OnSubscribed is an event handler hook method which intercepts an incoming MQTT client
// subscriber packet after the subscription has happened, in case we want to take action
// in response to it afterward.
// We don't right now, but in the final production system where we make extensions to the
// topic tree ourselves, this may be needed.
func (h PEAHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	log.Println("SUBSCRIBED", reasonCodes)
	showClient(cl)
	showPacket(pk)

	var purpose string
	for _, prop := range pk.Properties.User {
		if prop.Key == "purpose" {
			purpose = prop.Val
			break
		}
	}
	if purpose == "" {
		log.Println("Error: no subscription purpose")
	}

	h.policy_db.Exec("CALL add_subscription(?, ?, ?)", pk.TopicName, cl.ID, purpose)
}

// OnPacketEncode is an event handler hook method which intercepts an outgoing MQTT published
// message packet just before it is encoded into its final binary form before being shipped
// out to all the subscribed clients.
//
// Here is where we make our FILTER() rule resolution based on
// the packet content.
func (h PEAHook) OnPacketEncode(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	// TODO: hook in policy from store and evaluate here
	return pk
}

// OnACLCheck is an event handler hook method which intercepts incoming and
// outgoing MQTT packets to check whether they should be allowed to be sent
// or received. This allows for the imposition of arbitrary packet dropping
// rules.
//
// If write is true, it means client cl is publishing on the given topic.
// otherwise, client cl is about to receive data on the topic. The hook
// returns true if this should be allowed.
func (h PEAHook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	if !write {
		log.Println("ACL sub hook", topic, write, string(cl.Properties.Username))
		// TODO get and evaluate policy from database for this subject
	} else {
		log.Println("ACL pub hook", topic, write, string(cl.Properties.Username))
		// TODO get and evaluate policy from database for this subject
	}
	return true
}

// Hardwired test policy for proof of concept until we have a general-purpose
// policy input method and user interface built
//
// first cut: without a more sophisticated full-scale policy definition, let's
// identify user profile just by user/password.
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
