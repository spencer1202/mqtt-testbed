## Deprecated
This directory was originally for an orchestration script that has now been superceded by the simulator.

-----

### Original Design Notes Below


This directory holds the configuration files and orchestration scripts which execute experimental runs.

## General Design
The lab setup is governed by the scripts here. The launch script will start up the server (MQTT broker) and then the publishers and subscribers according to its configuration.
We then collect the data after the run is complete and analyze the data.

## Configuration
The experimental runs are controlled by a JSON file which governs how the clients and servers are configured as well as what the experimental parameters will be. The format of this file and the meaning of these parameters are explained by example below:
```
{
	"experiment": {
		"collectiondir": "path",		/* directory where results collect */
		"jobid": "id",				/* identifier for this experiment */
		"description": "text..."		/* description of this experiment */
	},
	"server": {
		"port": 1883,
		"protcol": "tcp"
	},
	"publishers": [                                 /* each entry in this list spins up a containerized client instance (publisher) */
		"topic/a/b/c": {                        /* MQTT topic name for this client to publish */
			"frequency": 10000,		/* number of milliseconds between published messages */
			"size": 32                      /* size in bytes of random data payload to be published */
		},
		"topic/d/e/f": {                        /* next topic... */
			"frequency": 10000,		
			"size": 32                      
		}
	],
	"subscribers": [				/* each entry in this list spins up a containerized client instance (subscriber) */
		"topic/a/b/c": {
			"number": 1			/* number of clients to spin up, all listening to this topic at the same time */
		},
		"topic/a/#": {				/* next set of clients for another topic */
			"number": 12
		}
	]
}
```

## Launch Script
The launch script starts up the MQTT server from the `server` directory to listen to incoming MQTT traffic on the configured TCP port, then launches
the client containers with appropriate parameters per its configuration file.

The clients write stats about the data they receive to data collection files in the *collectiondir*`/`*jobid*`/rawdata` directory under a unique file for each running client instance. The launch script tracks the running clients and manages
which ones are still running until they're all done and we have all their data.

## Data Collection
The containerized clients receive their subscribed data and record their results into a data file with one line per received message in the form:

*datestamp* *topic* *bytes_received*

## Extensibility and Updates
This is a baseline to get started. As we build out the capabilities of the experimental system we'll add more parameters related to access control to how we launch the clients and server for the experiments we'll run.
