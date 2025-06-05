This is our Proof-of-concept Policy Enforcement Agent which is added to a stock Mochi MQTT server.
(There's a lot more work in progress going on to build out the functioning PEA unit, but for the purposes of
this demonstration project, this first step proof-of-concept was ready to show at the moment which shows
the impact of inserting an agent of any sort into the broker, so we're starting with that and then bringing
the other pieces online incrementally.)

To use this, clone a copy of [mochi-mqtt server](https://github.com/mochi-mqtt/server) in its own directory. Then set up your own server based on it and add the `policy` hook defined here to it.  The simplest way to do that is to add it to the basic MQTT
server that comes with Mochi itself:

In Mochi, go into the `cmd` directory and edit `main.go`.

Add a line to import this module:

`import "github.com/MadScienceZone/mqtt-testbed/server/policy"`

Now, remove the default `AllowHook` from the server because it'll allow everyone to access everything:

`// _ = server.AddHook(new(auth.AllowHook), nil)`

In its place, create a new hook for our Policy Enforcement Agent (PEA):

```go
if err := server.AddHook(new(policy.PEAHook), nil); err != nil {
    panic(err)
}
```

That's it. Now either compile the binary or just run it directly:

`go run main.go`

and start running clients to through it as documented for the simulator in its directory in this repo.

The API documentation for our PEA hook module may be found at [pkg.go.dev](http://pkg.go.dev/github.com/MadScienceZone/mqtt-testbed/server/).
