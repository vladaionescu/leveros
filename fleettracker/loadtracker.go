package fleettracker

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/leverutil"
	"github.com/leveros/leveros/scale"
)

var trackerLogger = leverutil.GetLogger(PackageName, "loadtracker")

var (
	// TickNumEventsFlag is the number of events that the load tracker aims to
	// put in a tick length. Yet, due to {Min,Max}TickPeriodFlag, the number of
	// events may vary.
	TickNumEventsFlag = config.DeclareInt(PackageName, "tickNumEvents", 16)
	// MinTickPeriodFlag is the minimum time between load tracker ticks.
	MinTickPeriodFlag = config.DeclareDuration(
		PackageName, "minTickPeriod", 250*time.Millisecond)
	// MaxTickPeriodFlag is the maximum time between load tracker
	// ticks. Note: The period may still be longer than this if no events at all
	// come up for a while.
	MaxTickPeriodFlag = config.DeclareDuration(
		PackageName, "maxTickPeriod", 15*time.Second)
	// StatsExpPeriodFlag is the amount of time stats are taken into
	// consideration. This value is used for the computation of alpha in EWMA.
	StatsExpPeriodFlag = config.DeclareDuration(
		PackageName, "statsExpPeriod", 15*time.Second)
	// ScaleDownAfterFlag is the amount of time to delay scaling down (just
	// in case resource requirements pop back up).
	ScaleDownAfterFlag = config.DeclareDuration(
		PackageName, "scaleDownAfter", 45*time.Second)
	// ChangesExpectedAfterFlag is the maxium amount of time a change in number
	// of instances is expected to be reflected in Consul. (Otherwise
	// it is assumed there have been other changes outside of this
	// library's control).
	ChangesExpectedAfterFlag = config.DeclareDuration(
		PackageName, "changesExpectedAfter", 45*time.Second)
	// MaxDeltaFlag represents the maximum number of instances that may be
	// added at a time (per tick).
	MaxDeltaFlag = config.DeclareInt(PackageName, "maxDelta", 10)
)

type deltaEntry struct {
	delta      int
	expectedAt time.Time
}

// LoadTracker keeps track of the total load of a Lever service.
type LoadTracker struct {
	servingID       string
	maxInstanceLoad float64
	minInstances    int
	sessionKAB      *scale.KeepAliveBuffer

	// The trackers.
	rateT     *rateTracker
	rpcNanosT *valueTracker

	// Guard the following (write acquired on each tick).
	lock sync.RWMutex
	// The time of the previous tick.
	lastTick time.Time
	// The number of instances currently serving the service.
	numInstances int
	// The number of instances serving the service as reported by Consul.
	queriedNumInstances int
	// The next time we will look in Consul for the number of instances.
	nextInstancesQuery time.Time
	// Total changes expected to propagate.
	totalDeltaInstances int
	// Individual changes expected to propagate, sorted by expected propagation
	// time.
	deltas []deltaEntry
	// Is the load lower than necessary?
	decreaseTriggered bool
	// When should we scale down?
	decreaseTime time.Time
}

// NewLoadTracker creates a new LoadTracker.
func NewLoadTracker(
	servingID string, maxInstanceLoad float64, minInstances int,
	sessionID string) *LoadTracker {
	sessionKAB := scale.NewSessionKeepAliveBuffer(sessionID, TTLFlag.Get()/2)
	tracker := &LoadTracker{
		servingID:       servingID,
		rateT:           newRateTracker(),
		rpcNanosT:       newValueTracker(),
		maxInstanceLoad: maxInstanceLoad,
		minInstances:    minInstances,
		sessionKAB:      sessionKAB,
		numInstances:    -1,
	}
	return tracker
}

// OnRPC should be called on each RPC to register its stats.
func (tracker *LoadTracker) OnRPC(rpcNanos uint64) (deltaInstances int) {
	tracker.sessionKAB.KeepAlive()
	tracker.lock.RLock()
	// Keep these two under read lock to make sure they are not updated
	// during a tick.
	tracker.rpcNanosT.value(rpcNanos)
	eventsSoFar := tracker.rateT.event()

	tickNumEvents := uint32(TickNumEventsFlag.Get())
	maxTickPeriod := MaxTickPeriodFlag.Get()
	minTickPeriod := MinTickPeriodFlag.Get()
	maxTickTime := tracker.lastTick.Add(maxTickPeriod)
	minTickTime := tracker.lastTick.Add(minTickPeriod)
	now := time.Now()
	if (eventsSoFar >= tickNumEvents && now.After(minTickTime)) ||
		now.After(maxTickTime) {
		// Time for a tick. Switch locks.
		tracker.lock.RUnlock()
		tracker.lock.Lock()
		// Make sure nothing changed while we switched locks.
		eventsSoFar = tracker.rateT.getNumEvents()
		maxTickTime := tracker.lastTick.Add(maxTickPeriod)
		minTickTime := tracker.lastTick.Add(minTickPeriod)
		if !((eventsSoFar >= tickNumEvents && now.After(minTickTime)) ||
			now.After(maxTickTime)) {
			// Someone else did the tick for us.
			tracker.lock.Unlock()
			return 0
		}
		avgRPCNanos, rpcNanosVariance := tracker.rpcNanosT.get()
		avgRate, rateVariance := tracker.rateT.tick(StatsExpPeriodFlag.Get())
		delta := tracker.tick(
			float64(avgRPCNanos), rpcNanosVariance, avgRate, rateVariance)
		tracker.lastTick = now
		tracker.lock.Unlock()
		return delta
	}
	tracker.lock.RUnlock()
	return 0
}

// Recalculate reevaluates the number of instances necessary without registering
// an RPC event.
func (tracker *LoadTracker) Recalculate() (deltaInstances int) {
	tracker.lock.Lock()
	defer tracker.lock.Unlock()
	avgRPCNanos, rpcNanosVariance := tracker.rpcNanosT.get()
	avgRate, rateVariance := tracker.rateT.get()
	return tracker.tick(
		float64(avgRPCNanos), rpcNanosVariance, avgRate, rateVariance)
}

// Close clears any internal resources.
func (tracker *LoadTracker) Close() {
	tracker.sessionKAB.Close()
}

func (tracker *LoadTracker) tick(
	avgRPCNanos, rpcNanosVariance, avgRate,
	rateVariance float64) (deltaInstances int) {
	// TODO: This model works decently well for real-time, non-streaming RPCs.
	//       It doesn't do so well with streaming or RPCs taking a long time
	//       because it is imprecise and reacts very late (after the
	//       RPC / stream has finished). Need another strategy for those cases.

	// Assume RPC rate and time are the worst possible (max in confidence
	// interval). This gives us a theoretical 97.5% certainity (in reality
	// we are much less sure, due to many other factors not modeled here).
	RPCNanosCI := 1.96 * math.Sqrt(rpcNanosVariance)
	maxRPCNanos := float64(avgRPCNanos) + RPCNanosCI
	rateCI := 1.96 * math.Sqrt(rateVariance)
	maxRate := avgRate + rateCI
	totalLoad := maxRate * (maxRPCNanos / float64(1000000000))

	// Work out how many instances are theoretically necessary right now, given
	// the current load.
	requiredNumInstances := int(math.Ceil(totalLoad / tracker.maxInstanceLoad))
	requiredNumInstances = max(requiredNumInstances, tracker.minInstances)

	// Work out how many instances are expected to be healthy in the near
	// future.
	if time.Now().After(tracker.nextInstancesQuery) {
		// Refresh number of healthy instances from Consul.
		consulHealth := config.GetConsulClient().Health()
		entries, _, err := consulHealth.Service(
			tracker.servingID, "", true, &consulapi.QueryOptions{
				AllowStale:        true,
				RequireConsistent: false,
			})
		if err != nil {
			trackerLogger.WithFields(
				"err", err,
				"servingID", tracker.servingID,
			).Error("Error trying to ask Consul for instances")
		} else {
			tracker.queriedNumInstances = len(entries)
		}
		// We don't need this number to be fresh. Don't do this very often.
		tracker.nextInstancesQuery =
			time.Now().Add(ChangesExpectedAfterFlag.Get())
	}
	tracker.maybeExpireDeltas()
	var assumedNumInstances int
	if tracker.queriedNumInstances ==
		tracker.numInstances+tracker.totalDeltaInstances ||
		tracker.numInstances == -1 ||
		tracker.totalDeltaInstances == 0 {
		// All expected changes have been applied or load oscillating or
		// we made no changes but the number of instances changed for reasons
		// external to this algorithm (eg instances expired or died on their
		// own) or the tracker is completely new.
		// TODO: What if there is a widespread crash across instances. We might
		//       want to react quickly to bring up replacements. Maybe.
		//       Maybe not. We would need to avoid bringing up replacement(s)
		//       forever in situations where they eg just crash on startup.
		// Just reset deltas.
		tracker.numInstances = tracker.queriedNumInstances
		tracker.totalDeltaInstances = 0
		tracker.deltas = nil
		assumedNumInstances = tracker.queriedNumInstances
	} else {
		// Not all changes propagated. Use the number of instances we expect.
		assumedNumInstances = tracker.numInstances + tracker.totalDeltaInstances
	}

	delta := requiredNumInstances - assumedNumInstances
	trackerLogger.WithFields(
		"avgRate", avgRate,
		"rateCI", rateCI,
	).Debug("RATE")
	trackerLogger.WithFields(
		"avgRPCNanos", avgRPCNanos,
		"RPCNanosCI", RPCNanosCI,
	).Debug("NANOS")
	trackerLogger.WithFields(
		"servingID", tracker.servingID,
		"avgRPCNanos", avgRPCNanos,
		"RPCNanosCI", RPCNanosCI,
		"avgRate", avgRate,
		"rateCI", rateCI,
		"totalLoad", totalLoad,
		"requiredNumInstances", requiredNumInstances,
		"assumedNumInstances", assumedNumInstances,
		"delta", delta,
	).Debug("Tick")
	if delta == 0 {
		// In balance - nothing to do.
		tracker.decreaseTriggered = false
		return 0
	}

	if delta > 0 {
		// Add instances.
		tracker.decreaseTriggered = false
		maxDelta := MaxDeltaFlag.Get()
		if delta > maxDelta {
			delta = maxDelta
		}
		tracker.applyDelta(delta)
		return delta
	}

	// Maybe remove instances (if the load was low for a while).
	if tracker.decreaseTriggered && time.Now().After(tracker.decreaseTime) {
		// Time for a decrease.
		tracker.applyDelta(delta)
		tracker.decreaseTriggered = false
		return delta
	} else if !tracker.decreaseTriggered {
		// Just noticed decrease is necessary. If after ScaleDownAfterFlag
		// we still need to decrease, do it then.
		tracker.decreaseTriggered = true
		tracker.decreaseTime = time.Now().Add(ScaleDownAfterFlag.Get())
		return 0
	} else {
		// Just wait patiently until tracker.decreaseTime.
		return 0
	}

	// Note: It is possible that a future tick may not take place at all
	//       and a decrease does not take effect. In that case, the
	//       instance expiry timer within each instance would take care of
	//       clearing unnecessary instances.
}

func (tracker *LoadTracker) applyDelta(delta int) {
	tracker.totalDeltaInstances += delta
	tracker.deltas = append(
		tracker.deltas, deltaEntry{
			delta:      delta,
			expectedAt: time.Now().Add(ChangesExpectedAfterFlag.Get()),
		})
}

func (tracker *LoadTracker) maybeExpireDeltas() {
	now := time.Now()
	upToIndex := 0
	for index, entry := range tracker.deltas {
		upToIndex = index
		if !now.After(entry.expectedAt) {
			break
		}
		tracker.totalDeltaInstances -= entry.delta
	}
	if upToIndex == 0 {
		// Optimization.
		return
	}
	// Reslice to remove everything up to upToIndex. But use a copy to avoid
	// the underlying array from growing forever.
	newDeltas := tracker.deltas[upToIndex:]
	tracker.deltas = make([]deltaEntry, len(newDeltas))
	copy(tracker.deltas, newDeltas)
}

const (
	valueTrackerCapacity    = 32
	valueTrackerDataBuckets = 4
)

// valueTracker maintains an average and confidence intervals for a number of
// uint64 values over time.
// The values are sampled as the actual space for data is limited.
type valueTracker struct {
	// TODO: The algorithm to calculate average is not ideal because there are
	//       cases where precision is lost (eg sumSqValuesS32). There might be
	//       a better way.

	lock sync.RWMutex
	// The sum of all values in the sample.
	sumValues uint64
	// The sum of the squares of all values in the sample, shifted 32 bits
	// to make sure they fit (at the loss of precission). This is used for
	// the calculation of the confidence intervals.
	sumSqValuesS32 uint64
	// Sample values that form up the avgValue.
	data [valueTrackerCapacity]uint64
	// The number of values currently held.
	dataSize int
	// Mutexes for data (data is split into n buckets, each with its own lock).
	// TODO: This is actually overkill given that the contention is the rand
	//       pool lock.
	dataLocks [valueTrackerDataBuckets]sync.Mutex
}

func newValueTracker() *valueTracker {
	return &valueTracker{}
}

func (tracker *valueTracker) value(value uint64) {
	tracker.lock.RLock()
	if tracker.dataSize < valueTrackerCapacity {
		// Did not yet fill data vector.
		// Switch locks.
		tracker.lock.RUnlock()
		tracker.lock.Lock()
		// Make sure nothing changed.
		if tracker.dataSize == valueTrackerCapacity {
			// Sample space filled up while switching locks.
			// Try again.
			tracker.lock.Unlock()
			tracker.value(value)
		}

		tracker.data[tracker.dataSize] = value
		tracker.dataSize++
		atomic.AddUint64(&tracker.sumValues, value)
		atomic.AddUint64(&tracker.sumSqValuesS32, (value>>16)*(value>>16))
		tracker.lock.Unlock()
	} else {
		// Data vector full. Replace a random value in the sample.
		tmpRand := leverutil.GetRand()
		index := tmpRand.Intn(tracker.dataSize)
		leverutil.PutRand(tmpRand)
		tracker.dataLocks[index%valueTrackerDataBuckets].Lock()
		oldValue := tracker.data[index]
		tracker.data[index] = value
		tracker.dataLocks[index%valueTrackerDataBuckets].Unlock()

		// Update the sums as a result (subtract old value and add new value).
		delta := value + ^uint64(oldValue-1)
		atomic.AddUint64(&tracker.sumValues, delta)
		deltaSq := (value>>16)*(value>>16) +
			^uint64((oldValue>>16)*(oldValue>>16)-1)
		atomic.AddUint64(&tracker.sumSqValuesS32, deltaSq)
		tracker.lock.RUnlock()
	}
}

func (tracker *valueTracker) get() (avg uint64, variance float64) {
	tracker.lock.RLock()
	sumValues := atomic.LoadUint64(&tracker.sumValues)
	sumSqValuesS32 := atomic.LoadUint64(&tracker.sumSqValuesS32)
	avg, variance = avgAndVar(sumValues, sumSqValuesS32, tracker.dataSize)
	tracker.lock.RUnlock()
	return
}

func avgAndVar(
	sumValuesInt uint64, sumSqValuesS32Int uint64, nInt int) (
	avgInt uint64, variance float64) {
	sumValues := float64(sumValuesInt)
	sumSqValues := float64(sumSqValuesS32Int) * math.Pow(2, 32)
	n := float64(nInt)
	avg := sumValues / n
	if n == 1 {
		return uint64(avg), 0
	}
	// var = (sum(x-mean(x)))^2/(n-1) =>
	// var = (sum(x^2) - 2*mean(x)*sum(x) + n*(mean(x))^2)/(n-1) =>
	// var = sum(x^2)/(n-1) - 2*mean(x)*sum(x)/(n-1) + n*(mean(x))^2/(n-1)
	// Last form saves extra ~5 bits precision by dividing by n-1 early.
	variance = sumSqValues/(n-1) - 2*avg*(sumValues/(n-1)) + n*(avg/(n-1))*avg
	// 2^32/(n-1) is the variance due to using truncated fixed point for
	// sumSqValues. This works out to a contribution of ~2.3 micros to the CI.
	extraVariance := math.Pow(2, 32) / (n - 1)
	if variance < 0 {
		// Square of something < 0 => floating point errors.
		variance = extraVariance
	} else {
		variance += extraVariance
	}
	return uint64(avg), variance
}

// rateTracker maintains an average rate and confidence interval based on
// exponentially weighted moving averages.
// Note: tick and get should be synchornized externally.
type rateTracker struct {
	// Events since last tick (atomic).
	eventsAcc uint32

	// Events per second.
	avgRate float64
	// The variance (sigma squared) of the rate.
	variance float64
	// Time of last tick.
	lastTick time.Time
}

func newRateTracker() *rateTracker {
	return new(rateTracker)
}

func (tracker *rateTracker) event() uint32 {
	return atomic.AddUint32(&tracker.eventsAcc, 1)
}

func (tracker *rateTracker) getNumEvents() uint32 {
	return atomic.LoadUint32(&tracker.eventsAcc)
}

func (tracker *rateTracker) tick(
	statsExpPeriod time.Duration) (avgRate float64, variance float64) {
	now := time.Now()
	var tickLength time.Duration
	if tracker.lastTick.IsZero() {
		// Special case - first tick.
		// The tickLength is not reliable on the first tick because the
		// tracker has just been created - we don't really know the tickLength.
		// So we assume no event has come up for a long time: TTL-long. This is
		// equivalent to a service having expired from fleettracker and then
		// getting another event which wakes it up again.
		tickLength = TTLFlag.Get()
	} else {
		tickLength = now.Sub(tracker.lastTick)
	}
	eventsAcc := atomic.LoadUint32(&tracker.eventsAcc)
	instantRate := float64(eventsAcc) / tickLength.Seconds()
	// Update.
	diff := instantRate - tracker.avgRate
	alpha := 1 - 1/math.Exp(tickLength.Seconds()/statsExpPeriod.Seconds())
	tracker.variance = (1 - alpha) * (tracker.variance + alpha*diff*diff)
	tracker.avgRate += alpha * diff
	avgRate = tracker.avgRate
	variance = tracker.variance
	tracker.lastTick = now
	// Reset eventsAcc to 0 (without losing any updates that may have
	// happened in the meantime).
	atomic.AddUint32(&tracker.eventsAcc, ^uint32(eventsAcc-1))
	return avgRate, variance
}

func (tracker *rateTracker) get() (avgRate float64, variance float64) {
	avgRate = tracker.avgRate
	variance = tracker.variance
	return
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
