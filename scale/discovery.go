package scale

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/leverutil"
	"github.com/miekg/dns"
)

var (
	discoveryLogger = leverutil.GetLogger(PackageName, "discovery")
)

var (
	// DNSServerFlag is the DNS server address in format <ip>:<port>. Used for
	// service discovery.
	DNSServerFlag = config.DeclareString(
		PackageName, "dnsServer", "leverosconsul:8600")
)

// ErrServiceNotFound is an error that is returned when a service with provided
// name is not found.
var ErrServiceNotFound = fmt.Errorf("Service not found")

const (
	consulResourcePrefix       = "res/"
	consulConstrResourcePrefix = "constrres/"
)

// GetOwnNodeName returns the node name of the current node.
func GetOwnNodeName() (string, error) {
	consul := config.GetConsulClient()
	return consul.Agent().NodeName()
}

// RegisterServiceLocal registers the service as running on the current node.
func RegisterServiceLocal(
	service string, instanceID string, target string,
	ttl time.Duration) (err error) {
	consul := config.GetConsulClient()
	agent := consul.Agent()

	// Parse target into address + port.
	addressSplit := strings.Split(target, ":")
	if len(addressSplit) != 2 {
		return fmt.Errorf("Invalid address")
	}
	port, err := strconv.Atoi(addressSplit[1])
	if err != nil {
		return fmt.Errorf("Invalid address")
	}
	address := addressSplit[0]
	return agent.ServiceRegister(&consulapi.AgentServiceRegistration{
		Name:    service,
		ID:      instanceID,
		Address: address,
		Port:    port,
		Check: &consulapi.AgentServiceCheck{
			TTL:    ttl.String(),
			Status: "passing",
		},
	})
}

// DeregisterService deregisters a service from Consul.
func DeregisterService(instanceID string) error {
	consul := config.GetConsulClient()
	agent := consul.Agent()
	return agent.ServiceDeregister(instanceID)
}

// ServiceKeepAlive maintains the TTL for a service.
func ServiceKeepAlive(instanceID string) error {
	consul := config.GetConsulClient()
	agent := consul.Agent()
	return agent.PassTTL("service:"+instanceID, "")
}

// DereferenceService returns a random target and a node associated with given
// service name.
func DereferenceService(
	service string) (target string, node string, err error) {
	// Perform DNS lookup.
	msg := new(dns.Msg)
	msg.SetQuestion(service+".service.consul.", dns.TypeSRV)
	r, err := dns.Exchange(msg, DNSServerFlag.Get())
	if err != nil {
		return "", "", err
	}
	if r.Rcode == dns.RcodeNameError {
		return "", "", ErrServiceNotFound
	}
	if r.Rcode != dns.RcodeSuccess {
		return "", "", fmt.Errorf("DNS lookup failed with code %v", r.Rcode)
	}
	if len(r.Answer) == 0 {
		return "", "", ErrServiceNotFound
	}

	// Pick the first entry.
	answer := r.Answer[0]
	srvRecord, ok := answer.(*dns.SRV)
	if !ok {
		return "", "", fmt.Errorf("Found non-SRV answer")
	}

	node = strings.SplitN(srvRecord.Target, ".", 2)[0]

	// Look for the IP address of that entry.
	for _, extra := range r.Extra {
		aRecord, ok := extra.(*dns.A)
		if !ok {
			continue
		}
		if extra.Header().Name == srvRecord.Target {
			addr := aRecord.A.String() + ":" + strconv.Itoa(int(srvRecord.Port))
			return addr, node, nil
		}
	}
	return "", "", fmt.Errorf("Service node has no IP associated")
}

type resourceEntry struct {
	Target     string `json:"t,omitempty"`
	TargetNode string `json:"tn,omitempty"`
	SessionID  string `json:"sid,omitempty"`
}

// Resource represents a resource key in Consul. It can be used to associate
// instances of objects with owners in a distributed environment.
type Resource struct {
	consul          *consulapi.Client
	service         string
	resource        string
	key             string
	sessionID       string
	constrKey       string
	constrSessionID string
	target          string
	targetNode      string
	loseLockCh      <-chan struct{}
	constrLock      *consulapi.Lock
}

// ConstructResource begins constructing a resource. This operation blocks if
// resource is already being constructed. The success return value indicates
// whether the construction lock has been successfully acquired. If it is false
// (but no error), the resource has already been constructed. In this case, the
// target of the returned object is populated with the already constructed
// resource.
func ConstructResource(
	service string, resource string, timeout time.Duration) (
	res *Resource, success bool, err error) {
	res = newResouce(service, resource, "")
	res.constrKey =
		consulConstrResourcePrefix +
			url.QueryEscape(service) + "/" +
			url.QueryEscape(resource)

	session := res.consul.Session()
	res.constrSessionID, _, err = session.Create(&consulapi.SessionEntry{
		Name:     "constr_" + service + "_" + resource,
		Behavior: "delete",
		TTL:      timeout.String(),
	}, nil)
	if err != nil {
		return nil, false, err
	}

	res.constrLock, err = res.consul.LockOpts(&consulapi.LockOptions{
		Key:          res.constrKey,
		Session:      res.constrSessionID,
		LockWaitTime: timeout,
		LockTryOnce:  true,
	})
	if err != nil {
		res.destroyConstrSession()
		return nil, false, err
	}

	res.loseLockCh, err = res.constrLock.Lock(nil)
	if err != nil {
		res.destroyConstrSession()
		return nil, false, err
	}

	// Get, to check if someone else constructed before us.
	kv := res.consul.KV()
	pair, _, err := kv.Get(
		res.key,
		&consulapi.QueryOptions{RequireConsistent: true})
	if err != nil {
		res.destroyConstrSession()
		return nil, false, err
	}
	if pair == nil {
		// Not yet constructed.
		return res, true, nil
	}

	// Already constructed. Populate our fields.
	res.destroyConstrSession()
	entry := new(resourceEntry)
	err = json.Unmarshal(pair.Value, entry)
	if err != nil {
		return nil, false, err
	}
	res.target = entry.Target
	res.targetNode = entry.TargetNode
	res.sessionID = entry.SessionID
	return res, false, nil
}

// DoneConstructing can be used to release the construction lock and populate
// the resource's target.
func (res *Resource) DoneConstructing(
	target string, targetNode string, ttl time.Duration) (err error) {
	if res.LostLock() {
		res.destroyConstrSession()
		return fmt.Errorf("Lost construction lock")
	}
	res.target = target
	res.targetNode = targetNode
	success, err := res.register(ttl)
	if err != nil {
		res.destroyConstrSession()
		return err
	}
	if !success {
		res.destroyConstrSession()
		return fmt.Errorf("Resource already taken")
	}

	err = res.constrLock.Unlock()
	res.destroyConstrSession()
	if err != nil {
		return err
	}

	return nil
}

// RegisterResource selects a random service instance as the target for a new
// resource. The operation fails if the resource already exists.
func RegisterResource(
	service string, resource string, ttl time.Duration) (
	res *Resource, success bool, err error) {
	// Pick a random server for given service.
	target, targetNode, err := DereferenceService(service)
	if err != nil {
		return nil, false, err
	}

	return RegisterResourceCustom(service, resource, target, targetNode, ttl)
}

// RegisterResourceLocal registers a resource being served by this process.
func RegisterResourceLocal(
	resource string, ttl time.Duration) (
	res *Resource, success bool, err error) {
	service := leverutil.ServiceFlag.Get()
	instanceID := leverutil.InstanceIDFlag.Get()
	res = newResouce(service, resource, "")
	agent := res.consul.Agent()
	localServices, err := agent.Services()
	if err != nil {
		return nil, false, err
	}
	agentService, ok := localServices[instanceID]
	if !ok {
		return nil, false, ErrServiceNotFound
	}
	res.target = agentService.Address + ":" + strconv.Itoa(agentService.Port)
	success, err = res.register(0)
	if !success || err != nil {
		res = nil
	}
	return res, success, err
}

// DereferenceOrRegisterResource attempts to dereference a resource but
// registers as new if it fails. This is similar to
// ConstructResource+DoneConstructing except that it doesn't use a
// construction lock.
func DereferenceOrRegisterResource(
	service string, resource string, ttl time.Duration) (
	res *Resource, isNew bool, err error) {
	res, err = DereferenceResource(service, resource)
	if err != nil {
		return nil, false, err
	}
	if res == nil {
		// Not yet registered. Attempt to register.
		res, success, err := RegisterResource(service, resource, ttl)
		if err != nil {
			return nil, false, err
		}
		if !success {
			// Perhaps someone else constructed at the same time. Try again.
			return DereferenceOrRegisterResource(service, resource, ttl)
		}
		// Constructed new resource.
		return res, true, nil
	}

	// Already registered.
	return res, false, nil
}

// ExistingResource can be used to operate on a resource that is already
// registered (perhaps in a different process).
func ExistingResource(
	service string, resource string, sessionID string) (res *Resource) {
	return newResouce(service, resource, sessionID)
}

// RegisterResourceCustom registers an arbitrary target for a new resource.
// The operation fails altogether if the resource already exists.
func RegisterResourceCustom(
	service string, resource string, target string,
	targetNode string, ttl time.Duration) (
	res *Resource, success bool, err error) {
	res = newResouce(service, resource, "")
	res.target = target
	res.targetNode = targetNode
	success, err = res.register(ttl)
	if !success || err != nil {
		res = nil
	}
	return res, success, err
}

func newResouce(
	service string, resource string, sessionID string) (res *Resource) {
	service = url.QueryEscape(service)
	resource = url.QueryEscape(resource)
	return &Resource{
		key:       consulResourcePrefix + service + "/" + resource,
		consul:    config.GetConsulClient(),
		service:   service,
		resource:  resource,
		sessionID: sessionID,
	}
}

func (res *Resource) register(ttl time.Duration) (success bool, err error) {
	session := res.consul.Session()
	kv := res.consul.KV()

	res.sessionID, _, err = session.Create(&consulapi.SessionEntry{
		Name:     res.service + "_" + res.resource,
		Behavior: "delete",
		Node:     res.targetNode,
		TTL:      ttl.String(),
	}, nil)
	if err != nil {
		return false, err
	}

	entry := &resourceEntry{
		Target:     res.target,
		TargetNode: res.targetNode,
		SessionID:  res.sessionID,
	}
	value, err := json.Marshal(entry)
	if err != nil {
		res.destroySession()
		return false, fmt.Errorf("Error trying to encode json")
	}

	ok, _, err := kv.Acquire(&consulapi.KVPair{
		Key:     res.key,
		Value:   value,
		Session: res.sessionID,
	}, nil)
	if !ok || err != nil {
		res.destroySession()
		if err != nil {
			return false, fmt.Errorf("Error trying to Acquire resource")
		}
		return false, nil
	}

	return true, nil
}

// DereferenceResource populates the target fields for a resource that already
// exists.
func DereferenceResource(
	service string, resource string) (res *Resource, err error) {
	return dereferenceResourceInternal(service, resource, false)
}

// DereferenceResourceConsistent is like DereferenceResource, except it does
// consistent read.
func DereferenceResourceConsistent(
	service string, resource string) (res *Resource, err error) {
	return dereferenceResourceInternal(service, resource, true)
}

// Deregister removes the resource from Consul.
func (res *Resource) Deregister() error {
	_, err := res.consul.KV().Delete(res.key, nil)
	if err != nil {
		return err
	}
	if res.sessionID != "" {
		_, err = res.consul.Session().Destroy(res.sessionID, nil)
		return err
	}
	return nil
}

// KeepAlive maintains the resource's TTL to prevent expiry.
func (res *Resource) KeepAlive() error {
	session := res.consul.Session()
	_, _, err := session.Renew(res.sessionID, nil)
	return err
}

// GetTarget returns the resource's target.
func (res *Resource) GetTarget() string {
	return res.target
}

// GetTargetNode returns the resource's target node.
func (res *Resource) GetTargetNode() string {
	return res.targetNode
}

// GetSessionID returns the resource's owning session.
func (res *Resource) GetSessionID() string {
	return res.sessionID
}

// LostLock returns true if the construction lock was lost in the mean time.
func (res *Resource) LostLock() bool {
	select {
	case <-res.loseLockCh:
		return true
	default:
		return false
	}
}

// LoseLockCh returns the construction lose lock channel. This channel is closed
// if for any reason the construction lock is lost.
func (res *Resource) LoseLockCh() <-chan struct{} {
	return res.loseLockCh
}

func dereferenceResourceInternal(
	service string, resource string, consistent bool) (
	res *Resource, err error) {
	res = newResouce(service, resource, "")

	kv := res.consul.KV()
	pair, _, err := kv.Get(
		res.key,
		&consulapi.QueryOptions{RequireConsistent: consistent})
	if err != nil {
		return nil, err
	}
	if pair == nil {
		return nil, nil
	}

	entry := new(resourceEntry)
	err = json.Unmarshal(pair.Value, entry)
	if err != nil {
		return nil, err
	}
	res.target = entry.Target
	res.targetNode = entry.TargetNode
	res.sessionID = entry.SessionID

	return res, nil
}

func (res *Resource) destroyConstrSession() {
	_, err := res.consul.Session().Destroy(res.constrSessionID, nil)
	if err != nil {
		discoveryLogger.WithFields("sessionID", res.constrSessionID).Error(
			"Error trying to destroy unnecessary session")
	}
	// GC.
	res.constrSessionID = ""
	res.constrLock = nil
}

func (res *Resource) destroySession() {
	_, err := res.consul.Session().Destroy(res.sessionID, nil)
	if err != nil {
		discoveryLogger.WithFields("sessionID", res.sessionID).Error(
			"Error trying to destroy unnecessary session")
	}
}

// WaitResource monitors a resource and blocks until that resource is
// released or there is some other error.
func WaitResource(service string, resource string) error {
	service = url.QueryEscape(service)
	resource = url.QueryEscape(resource)

	consul := config.GetConsulClient()
	kv := consul.KV()

	lastIndex := uint64(0)
	for {
		pair, qm, err := kv.Get(
			consulResourcePrefix+service+"/"+resource,
			&consulapi.QueryOptions{
				WaitIndex:         lastIndex,
				RequireConsistent: true,
			})
		if err != nil {
			if !consulapi.IsServerError(err) {
				return err
			}
			// Consul unresponsive. Wait a bit and try again.
			time.Sleep(3 * time.Second)
			continue
		}
		if pair == nil {
			return nil
		}
		lastIndex = qm.LastIndex
	}
}

// ResourceReleased returns a channel which closes once the provided resource
// is released.
func ResourceReleased(
	service string, resource string) (released chan error) {
	released = make(chan error, 1)
	go func() {
		err := WaitResource(service, resource)
		if err != nil {
			released <- err
		}
		close(released)
	}()
	return released
}

// SelfKeepAlive automatically maintains the TTL of a resource by sending
// regular KeepAlive's from a goroutine.
type SelfKeepAlive struct {
	ID        string
	stopCh    chan struct{}
	interval  time.Duration
	isSession bool
}

// NewSessionSelfKeepAlive returns a new instance of a SelfKeepAlive for a
// Consul session.
func NewSessionSelfKeepAlive(
	sessionID string, interval time.Duration) *SelfKeepAlive {
	ska := &SelfKeepAlive{
		ID:        sessionID,
		stopCh:    make(chan struct{}),
		interval:  interval,
		isSession: true,
	}
	go ska.worker()
	return ska
}

// NewServiceSelfKeepAlive returns a new instance of a SelfKeepAlive for a
// Consul service.
func NewServiceSelfKeepAlive(
	instanceID string, interval time.Duration) *SelfKeepAlive {
	ska := &SelfKeepAlive{
		ID:        instanceID,
		stopCh:    make(chan struct{}),
		interval:  interval,
		isSession: false,
	}
	go ska.worker()
	return ska
}

// Stop stops the SelfKeepAlive goroutine (and no longer sends keep alives).
func (ska *SelfKeepAlive) Stop() {
	close(ska.stopCh)
}

func (ska *SelfKeepAlive) worker() {
	timer := time.NewTimer(ska.interval)
	for {
		select {
		case <-timer.C:
			ska.send()
			timer = time.NewTimer(ska.interval)
		case <-ska.stopCh:
			return
		}
	}
}

func (ska *SelfKeepAlive) send() {
	consul := config.GetConsulClient()
	if ska.isSession {
		// Session TTL.
		session := consul.Session()
		_, _, err := session.Renew(ska.ID, nil)
		if err != nil {
			discoveryLogger.WithFields(
				"err", err,
				"sessionID", ska.ID,
			).Error("Error trying to send keep alive")
			ska.Stop()
		}
	} else {
		// Service TTL.
		agent := consul.Agent()
		err := agent.PassTTL("service:"+ska.ID, "")
		if err != nil {
			discoveryLogger.WithFields(
				"err", err,
				"serviceID", ska.ID,
			).Error("Error trying to send keep alive")
			ska.Stop()
		}
	}
}

// KeepAliveBuffer is an object that maintains a resource's TTL without
// overwhelming consul if calls to keep alive are very frequent. In case
// of frequent firing, the object absorbs most of the calls and ensures that
// actual keep alives are sent within maxWait apart.
type KeepAliveBuffer struct {
	ID        string
	lastSent  time.Time
	maxWait   time.Duration
	msg       chan struct{}
	isSession bool
}

// NewSessionKeepAliveBuffer returns a new instance of KeepAliveBuffer for a
// session TTL.
func NewSessionKeepAliveBuffer(
	sessionID string, maxWait time.Duration) *KeepAliveBuffer {
	kab := &KeepAliveBuffer{
		ID:        sessionID,
		maxWait:   maxWait,
		msg:       make(chan struct{}, 1),
		isSession: true,
	}
	go kab.worker()
	return kab
}

// NewServiceKeepAliveBuffer returns a new instance of KeepAliveBuffer for a
// service TTL.
func NewServiceKeepAliveBuffer(
	instanceID string, maxWait time.Duration) *KeepAliveBuffer {
	kab := &KeepAliveBuffer{
		ID:        instanceID,
		maxWait:   maxWait,
		msg:       make(chan struct{}, 1),
		isSession: false,
	}
	go kab.worker()
	return kab
}

// KeepAlive maintains the TTL of the resource.
func (kab *KeepAliveBuffer) KeepAlive() {
	kab.msg <- struct{}{}
}

// Close destroys the object so that keep alives are no longer sent.
func (kab *KeepAliveBuffer) Close() {
	close(kab.msg)
}

func (kab *KeepAliveBuffer) worker() {
	var timer *time.Timer
	for {
		if timer == nil {
			_, ok := <-kab.msg
			if !ok {
				return
			}
			now := time.Now()
			if kab.lastSent.Add(kab.maxWait).Before(now) {
				// Haven't sent in a long time. Do it immediately.
				kab.send()
			} else {
				// Recently sent. Schedule for later.
				timer = time.NewTimer(kab.lastSent.Add(kab.maxWait).Sub(now))
			}
		} else {
			select {
			case _, ok := <-kab.msg:
				if !ok {
					return
				}
				// Don't do anything. Timer already scheduled to send.
				// (Absorb keep alive load).
			case <-timer.C:
				kab.send()
				timer = nil
			}
		}
	}
}

func (kab *KeepAliveBuffer) send() {
	kab.lastSent = time.Now()
	// Send but don't block worker (to ensure fast service of channel).
	go func() {
		consul := config.GetConsulClient()
		if kab.isSession {
			session := consul.Session()
			_, _, err := session.Renew(kab.ID, nil)
			if err != nil {
				discoveryLogger.WithFields(
					"err", err,
					"sessionID", kab.ID,
				).Error("Error trying to send keep alive")
				kab.Close()
				return
			}
		} else {
			// Service TTL.
			agent := consul.Agent()
			err := agent.PassTTL("service:"+kab.ID, "")
			if err != nil {
				discoveryLogger.WithFields(
					"err", err,
					"serviceID", kab.ID,
				).Error("Error trying to send keep alive")
				kab.Close()
			}
		}
	}()
}
