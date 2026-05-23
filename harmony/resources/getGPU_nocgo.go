//go:build !darwin && !cgo

package resources

import (
	"os"
	"strconv"
)

var GpuOverprovisionFactor = 1

func init() {
	if nstr := os.Getenv("HARMONY_GPU_OVERPROVISION_FACTOR"); nstr != "" {
		n, err := strconv.Atoi(nstr)
		if err != nil {
			logger.Errorf("parsing HARMONY_GPU_OVERPROVISION_FACTOR failed: %+v", err)
		} else {
			GpuOverprovisionFactor = n
		}
	}
}

// getGPUDevices is the non-CGo stub. curio-core ships pure-Go; GPU
// discovery via filecoin-ffi isn't compiled in. The HARMONY_OVERRIDE_GPUS
// env var still works for operators who want to declare GPU resources
// explicitly. Default 0 reflects "no GPU-bound tasks scheduled on this
// machine," which matches the PDP-only deployment shape.
func getGPUDevices() float64 {
	if nstr := os.Getenv("HARMONY_OVERRIDE_GPUS"); nstr != "" {
		n, err := strconv.ParseFloat(nstr, 64)
		if err != nil {
			logger.Errorf("parsing HARMONY_OVERRIDE_GPUS failed: %+v", err)
		} else {
			return n
		}
	}
	return 0
}
