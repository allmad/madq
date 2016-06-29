// +build !go1.5

package bench

func EnableTrace() {
	println("trace is not supported below go1.5")
}

func DisableTrace() {

}
