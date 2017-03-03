package main

import (
	"fmt"
	"log"
	"net"
	"strings"
)

import "github.com/jjneely/buckytools/hashing"
import . "github.com/jjneely/buckytools"

type ClusterConfig struct {
	// Port is the port remote buckyd daemons listen on
	Port string

	// Servers is a list of all bucky server hostnames.  This does not
	// include port information
	Servers []string

	// Hash is a HashRing interface that implements the hashring algorithm
	// that the cluster is using
	Hash hashing.HashRing

	// Healthy is true if the cluster configuration represents a Healthy
	// cluster
	Healthy bool
}

// Cluster is the working and cached cluster configuration
var Cluster *ClusterConfig

func (c *ClusterConfig) HostPorts() []string {
	if c == nil {
		return nil
	}
	ret := make([]string, 0)
	for _, v := range c.Servers {
		ret = append(ret, fmt.Sprintf("%s:%s", v, c.Port))
	}
	return ret
}

// GetClusterConfig returns either the cached ClusterConfig object or
// builds it if needed.  The initial HOST:PORT of the buckyd daemon
// must be given.
func GetClusterConfig(hostport string) (*ClusterConfig, error) {
	if Cluster != nil {
		return Cluster, nil
	}

	master, err := GetSingleHashRing(hostport)
	if err != nil {
		log.Printf("Abort: Cannot communicate with initial buckyd daemon.")
		return nil, err
	}

	server, port, err := net.SplitHostPort(HostPort)
	if err != nil {
		log.Printf("Abort: Invalid host:port representation: %s", hostport)
		return nil, err
	}

	Cluster = new(ClusterConfig)
	Cluster.Port = port
	Cluster.Servers = make([]string, 0)
	switch master.Algo {
	case "carbon":
		Cluster.Hash = hashing.NewCarbonHashRing()
	case "jump_fnv1a":
		Cluster.Hash = hashing.NewJumpHashRing(master.Replicas)
	default:
		log.Printf("Unknown consistent hash algorithm: %s", master.Algo)
		return nil, fmt.Errorf("Unknown consistent hash algorithm: %s", master.Algo)
	}

	for _, v := range master.Nodes {
		// strip instance values
		fields := strings.Split(v, ":")
		Cluster.Servers = append(Cluster.Servers, fields[0])
		if len(fields) < 2 {
			fields = append(fields, "")
		}
		Cluster.Hash.AddNode(hashing.Node{fields[0], fields[1]})
	}

	members := make([]*JSONRingType, 0)
	for _, srv := range Cluster.Servers {
		if srv == server {
			// Don't query the initial daemon again
			continue
		}
		host := fmt.Sprintf("%s:%s", srv, Cluster.Port)
		member, err := GetSingleHashRing(host)
		if err != nil {
			log.Printf("Cluster unhealthy: %s: %s", server, err)
		}
		members = append(members, member)
	}

	Cluster.Healthy = isHealthy(master, members)
	return Cluster, nil
}

// isHealthy will return true if the cluster ring data represents
// a healthy cluster.  The master is the initial buckyd daemon we
// built the list from.
func isHealthy(master *JSONRingType, ring []*JSONRingType) bool {

  /*
    I'm not even sure that replicas are part of the carbon-cache.py
    hash ring implementation. I'm nullifying this check by equating the
    'masters' length of nodes to the length of the hash ring.

    As far as I can tell, this doesn't affect core functionality.
  */
	// XXX: Take replicas into account
	if len(master.Nodes) != len(ring) {
	  log.Printf("The length of master nodes is not equal to a hash ring+1, therefore the cluster is unhealthy")
    log.Printf("The length of the masters nodes is: %s", len(master.Nodes))
    log.Printf("The length of the ring is: %s", len(ring)+1)
		return false
	}

	// We compare each ring to the first one
	for _, v := range ring {
		// Order, host:instance pair, must be the same.  You configured
		// your cluster with a CM tool, right?
		if master.Algo != v.Algo {
		  log.Printf("Cluster is unhealthy because of unmatched algorithms between %s and %s", master)
			return false
		}
		if len(v.Nodes) != len(master.Nodes) {
		  log.Printf("Cluster is unhealthy because the number of reported nodes is different between %s and %s", master, v)
			return false
		}
		for j, _ := range v.Nodes {
			if v.Nodes[j] != master.Nodes[j] {
			  log.Printf("Cluster is unhealthy because the node order is different between %s and %s", master, v)
				return false
			}
		}
	}

	return true
}
