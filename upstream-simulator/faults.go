package main

import "fmt"

type faultPlan struct {
	Seed         uint64 `json:"seed"`
	DelayMS      int    `json:"delay_ms"`
	SpikeMS      int    `json:"spike_ms"`
	SpikeEvery   uint64 `json:"spike_every"`
	SpikeLength  uint64 `json:"spike_length"`
	BurstEvery   uint64 `json:"burst_503_every"`
	BurstLength  uint64 `json:"burst_503_length"`
	DropPermille uint64 `json:"drop_permille"`
	Unavailable  bool   `json:"unavailable"`
	HealthStatus int    `json:"health_status"`
}

func (p faultPlan) validate() error {
	if p.DelayMS < 0 || p.DelayMS > 5000 || p.SpikeMS < 0 || p.SpikeMS > 5000 {
		return fmt.Errorf("delay_ms and spike_ms must be in 0..5000")
	}
	if p.SpikeLength > p.SpikeEvery || p.BurstLength > p.BurstEvery {
		return fmt.Errorf("spike and burst lengths must fit inside their periods")
	}
	if p.DropPermille > 1000 {
		return fmt.Errorf("drop_permille must be in 0..1000")
	}
	if p.HealthStatus != 0 && p.HealthStatus != 204 && p.HealthStatus != 503 {
		return fmt.Errorf("health_status must be 0, 204, or 503")
	}
	return nil
}

type decision struct {
	delayMS int
	reject  bool
	drop    bool
}

func inWindow(sequence, period, length uint64) bool {
	return period > 0 && (sequence-1)%period < length
}

func (p faultPlan) decide(sequence uint64) decision {
	delay := p.DelayMS
	if inWindow(sequence, p.SpikeEvery, p.SpikeLength) {
		delay += p.SpikeMS
	}
	return decision{
		delayMS: delay,
		reject:  inWindow(sequence, p.BurstEvery, p.BurstLength),
		drop:    mix(p.Seed+sequence*0x9e3779b97f4a7c15)%1000 < p.DropPermille,
	}
}

// SplitMix64 finalizer. An ordinal has the same decision for a given seed
// regardless of handler completion order.
func mix(value uint64) uint64 {
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}
