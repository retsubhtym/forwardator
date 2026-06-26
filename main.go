package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	protocolTCP             = byte('T')
	maxUDPPayload           = 65507
	udpRegistrationMagic    = "forwardator-udp-v1"
	udpRegistrationInterval = 5 * time.Second
	udpClientTTL            = 30 * time.Second
	udpCleanupPeriod        = time.Minute
)

type config struct {
	clientServer string
	serverDevice string
	tcpPort      int
	udpPort      int
	tunnelPort   int
	bind         string
}

type registeredUDPClient struct {
	addr *net.UDPAddr
	last time.Time
}

type udpClientRegistry struct {
	mu     sync.RWMutex
	client *registeredUDPClient
}

var copyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 32*1024)
		return &buf
	},
}

func main() {
	cfg := parseFlags()
	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	errCh := make(chan error, 4)
	start := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("%s: %w", name, err)
			}
		}()
	}

	if cfg.clientServer != "" {
		serverTCPAddr := joinHostPort(cfg.clientServer, cfg.tunnelPort)
		serverUDPAddr := joinHostPort(cfg.clientServer, cfg.tunnelPort)
		bind := cfg.bind
		if bind == "" {
			bind = "127.0.0.1"
		}
		if cfg.tcpPort > 0 {
			local := joinHostPort(bind, cfg.tcpPort)
			start("tcp client", func(ctx context.Context) error {
				return runClientTCP(ctx, local, serverTCPAddr)
			})
		}
		if cfg.udpPort > 0 {
			local := joinHostPort(bind, cfg.udpPort)
			start("udp client", func(ctx context.Context) error {
				return runClientUDP(ctx, local, serverUDPAddr)
			})
		}
	} else {
		deviceTCPAddr := joinHostPort(cfg.serverDevice, cfg.tcpPort)
		bind := cfg.bind
		if bind == "" {
			bind = "0.0.0.0"
		}
		if cfg.tcpPort > 0 {
			listen := joinHostPort(bind, cfg.tunnelPort)
			start("tcp server", func(ctx context.Context) error {
				return runServerTCP(ctx, listen, deviceTCPAddr)
			})
		}
		if cfg.udpPort > 0 {
			tunnelListen := joinHostPort(bind, cfg.tunnelPort)
			deviceListen := joinHostPort(bind, cfg.udpPort)
			start("udp server", func(ctx context.Context) error {
				return runServerUDP(ctx, tunnelListen, deviceListen, cfg.serverDevice)
			})
		}
	}

	select {
	case err := <-errCh:
		stop()
		log.Printf("stopping: %v", err)
	case <-ctx.Done():
		log.Printf("stopping")
	}
	wg.Wait()
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.clientServer, "c", "", "client mode: server IP/host to connect to")
	flag.StringVar(&cfg.serverDevice, "s", "", "server mode: device IP/host to forward to")
	flag.IntVar(&cfg.tcpPort, "tcp", 0, "TCP port to forward; 0 disables TCP")
	flag.IntVar(&cfg.udpPort, "udp", 0, "UDP port to forward; 0 disables UDP")
	flag.IntVar(&cfg.tunnelPort, "tunnel", 9000, "client/server tunnel port")
	flag.StringVar(&cfg.bind, "bind", "", "bind address; default is 127.0.0.1 in client mode and 0.0.0.0 in server mode")
	flag.Parse()
	return cfg
}

func (c config) validate() error {
	if (c.clientServer == "") == (c.serverDevice == "") {
		return errors.New("choose exactly one mode: -c <server_ip> or -s <device_ip>")
	}
	if c.tcpPort == 0 && c.udpPort == 0 {
		return errors.New("set --tcp and/or --udp")
	}
	for name, port := range map[string]int{"--tcp": c.tcpPort, "--udp": c.udpPort, "--tunnel": c.tunnelPort} {
		if port < 0 || port > 65535 {
			return fmt.Errorf("%s port out of range", name)
		}
	}
	if c.tunnelPort == 0 {
		return errors.New("--tunnel must be greater than 0")
	}
	return nil
}

func joinHostPort(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func runClientTCP(ctx context.Context, localAddr, serverAddr string) error {
	ln, err := net.Listen("tcp", localAddr)
	if err != nil {
		return err
	}
	defer ln.Close()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	log.Printf("client tcp: listening on %s -> %s", localAddr, serverAddr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go handleClientTCPConn(conn, serverAddr)
	}
}

func handleClientTCPConn(local net.Conn, serverAddr string) {
	defer local.Close()

	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	tunnel, err := dialer.Dial("tcp", serverAddr)
	if err != nil {
		log.Printf("tcp: tunnel dial failed: %v", err)
		return
	}
	defer tunnel.Close()
	setTCPNoDelay(local)
	setTCPNoDelay(tunnel)

	if _, err := tunnel.Write([]byte{protocolTCP}); err != nil {
		log.Printf("tcp: protocol write failed: %v", err)
		return
	}
	proxyTCP(local, tunnel)
}

func runServerTCP(ctx context.Context, listenAddr, deviceAddr string) error {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	defer ln.Close()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	log.Printf("server tcp: listening on %s -> %s", listenAddr, deviceAddr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go handleServerTCPConn(conn, deviceAddr)
	}
}

func handleServerTCPConn(tunnel net.Conn, deviceAddr string) {
	defer tunnel.Close()
	setTCPNoDelay(tunnel)

	_ = tunnel.SetReadDeadline(time.Now().Add(10 * time.Second))
	var header [1]byte
	if _, err := io.ReadFull(tunnel, header[:]); err != nil {
		log.Printf("tcp: protocol read failed: %v", err)
		return
	}
	_ = tunnel.SetReadDeadline(time.Time{})
	if header[0] != protocolTCP {
		log.Printf("tcp: unsupported protocol byte %q", header[0])
		return
	}

	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	device, err := dialer.Dial("tcp", deviceAddr)
	if err != nil {
		log.Printf("tcp: device dial failed: %v", err)
		return
	}
	defer device.Close()
	setTCPNoDelay(device)

	proxyTCP(tunnel, device)
}

func proxyTCP(a, b net.Conn) {
	done := make(chan struct{}, 2)
	go func() {
		copyPooled(a, b)
		closeWrite(a)
		done <- struct{}{}
	}()
	go func() {
		copyPooled(b, a)
		closeWrite(b)
		done <- struct{}{}
	}()
	<-done
	_ = a.Close()
	_ = b.Close()
	<-done
}

func copyPooled(dst io.Writer, src io.Reader) {
	bufPtr := copyBufferPool.Get().(*[]byte)
	defer copyBufferPool.Put(bufPtr)
	_, _ = io.CopyBuffer(dst, src, *bufPtr)
}

func closeWrite(c net.Conn) {
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}
}

func setTCPNoDelay(c net.Conn) {
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
}

func runClientUDP(ctx context.Context, localDestAddr, serverAddr string) error {
	localDest, err := net.ResolveUDPAddr("udp", localDestAddr)
	if err != nil {
		return err
	}
	server, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		return err
	}

	tunnel, err := net.ListenUDP("udp", nil)
	if err != nil {
		return err
	}
	defer tunnel.Close()

	localOut, err := net.DialUDP("udp", nil, localDest)
	if err != nil {
		return err
	}
	defer localOut.Close()

	go func() {
		<-ctx.Done()
		_ = tunnel.Close()
		_ = localOut.Close()
	}()
	go udpRegistrationLoop(ctx, tunnel, server)

	if err := sendUDPRegistration(tunnel, server); err != nil {
		return err
	}

	log.Printf("client udp: receiving from %s and delivering to %s", serverAddr, localDestAddr)

	buf := make([]byte, maxUDPPayload)
	for {
		n, addr, err := tunnel.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if !sameUDPAddr(addr, server) {
			continue
		}
		if _, err := localOut.Write(buf[:n]); err != nil {
			log.Printf("udp: write to local receiver %s failed: %v", localDest, err)
		}
	}
}

func udpRegistrationLoop(ctx context.Context, conn *net.UDPConn, server *net.UDPAddr) {
	ticker := time.NewTicker(udpRegistrationInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := sendUDPRegistration(conn, server); err != nil {
				log.Printf("udp: registration failed: %v", err)
			}
		}
	}
}

func sendUDPRegistration(conn *net.UDPConn, server *net.UDPAddr) error {
	_, err := conn.WriteToUDP([]byte(udpRegistrationMagic), server)
	return err
}

func runServerUDP(ctx context.Context, tunnelListenAddr, deviceListenAddr, deviceHost string) error {
	if tunnelListenAddr == deviceListenAddr {
		return errors.New("--udp and --tunnel must be different UDP ports in server mode")
	}

	tunnelAddr, err := net.ResolveUDPAddr("udp", tunnelListenAddr)
	if err != nil {
		return err
	}
	deviceAddr, err := net.ResolveUDPAddr("udp", deviceListenAddr)
	if err != nil {
		return err
	}
	deviceIPs, err := net.LookupIP(deviceHost)
	if err != nil {
		return err
	}
	if len(deviceIPs) == 0 {
		return fmt.Errorf("no IP addresses found for device host %q", deviceHost)
	}

	tunnel, err := net.ListenUDP("udp", tunnelAddr)
	if err != nil {
		return err
	}
	defer tunnel.Close()
	deviceIn, err := net.ListenUDP("udp", deviceAddr)
	if err != nil {
		return err
	}
	defer deviceIn.Close()

	clients := newUDPClientRegistry()
	go func() {
		<-ctx.Done()
		_ = tunnel.Close()
		_ = deviceIn.Close()
	}()
	go receiveUDPRegistrations(ctx, tunnel, clients)
	go cleanupUDPClients(ctx, clients)

	log.Printf("server udp: receiving device packets from %s on %s -> registered clients via %s", deviceHost, deviceListenAddr, tunnelListenAddr)

	buf := make([]byte, maxUDPPayload)
	for {
		n, addr, err := deviceIn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if !ipAllowed(addr.IP, deviceIPs) {
			continue
		}

		client := clients.active()
		if client == nil {
			continue
		}
		if _, err := tunnel.WriteToUDP(buf[:n], client); err != nil {
			log.Printf("udp: write to client %s failed: %v", client, err)
		}
	}
}

func receiveUDPRegistrations(ctx context.Context, conn *net.UDPConn, clients *udpClientRegistry) {
	buf := make([]byte, 64)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("udp: registration read failed: %v", err)
			return
		}
		if n == len(udpRegistrationMagic) && bytes.Equal(buf[:n], []byte(udpRegistrationMagic)) {
			clients.upsert(addr)
		}
	}
}

func newUDPClientRegistry() *udpClientRegistry {
	return &udpClientRegistry{}
}

func (r *udpClientRegistry) upsert(addr *net.UDPAddr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.client = &registeredUDPClient{addr: cloneUDPAddr(addr), last: time.Now()}
}

func (r *udpClientRegistry) active() *net.UDPAddr {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.client == nil {
		return nil
	}
	return r.client.addr
}

func cleanupUDPClients(ctx context.Context, clients *udpClientRegistry) {
	ticker := time.NewTicker(udpCleanupPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			clients.cleanup()
		}
	}
}

func (r *udpClientRegistry) cleanup() {
	cutoff := time.Now().Add(-udpClientTTL)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.client != nil && r.client.last.Before(cutoff) {
		r.client = nil
	}
}

func ipAllowed(ip net.IP, allowed []net.IP) bool {
	for _, candidate := range allowed {
		if ip.Equal(candidate) {
			return true
		}
	}
	return false
}

func sameUDPAddr(a, b *net.UDPAddr) bool {
	if a == nil || b == nil {
		return false
	}
	return a.Port == b.Port && a.IP.Equal(b.IP)
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	ip := make(net.IP, len(addr.IP))
	copy(ip, addr.IP)
	return &net.UDPAddr{IP: ip, Port: addr.Port, Zone: addr.Zone}
}
