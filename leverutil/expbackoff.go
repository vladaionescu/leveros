package leverutil

import "time"

// TryFun is the signature of the function that can be passed to ExpBackoff.
type TryFun func() (result interface{}, err error, finalErr error)

// ExpBackoff retries tryFun with exponential backoff, until error is nil or
// finalErr is not nil, or the total time spent trying exceeds maxTriesDuration.
func ExpBackoff(
	tryFun TryFun, startBackoff time.Duration,
	maxTriesDuration time.Duration) (result interface{}, err error) {
	start := time.Now()
	backoff := startBackoff
	for {
		result, err, finalErr := tryFun()
		if finalErr != nil {
			return nil, finalErr
		}
		if err == nil {
			return result, nil
		}
		if time.Since(start) > maxTriesDuration {
			return nil, err
		}
		time.Sleep(backoff)
		backoff *= 2
	}
}
