# forwardator

`forwardator` forwards traffic between a client machine and a server machine that can reach a device.

Topology:

```text
TCP: Client app <-> localhost:<tcpPort> <-> forwardator client <-> forwardator server <-> Device:<tcpPort>
UDP: Client app <-  localhost:<udpPort> <-  forwardator client <-  forwardator server <-  Device
```

TCP is bidirectional. UDP is receive-only on the client side: device UDP datagrams are forwarded from the server to the client and delivered unchanged to `localhost:<udpPort>`.

## Build

```bash
go build -o forwardator .
```

## Usage

On the server node, where `<device_ip>` can send UDP packets to this server and is reachable for TCP:

```bash
./forwardator -s <device_ip> --tcp <tcpPort> --udp <udpPort>
```

On the client node:

```bash
./forwardator -c <server_ip> --tcp <tcpPort> --udp <udpPort>
```

Client behavior:

- TCP listens on `127.0.0.1:<tcpPort>` and forwards both directions.
- UDP does **not** listen for local sends. It receives datagrams from the server tunnel and writes each payload to `127.0.0.1:<udpPort>`. Your local UDP consumer should bind/listen on that address.

Server behavior:

- TCP listens on the tunnel port and connects to `<device_ip>:<tcpPort>`.
- UDP listens on `0.0.0.0:<udpPort>` for datagrams whose source IP is `<device_ip>`, then forwards those datagrams to the latest registered client.

By default, the client/server tunnel uses port `9000` on the server for both TCP and UDP tunnel traffic. Open/forward TCP `9000` and UDP `9000` on the server firewall.

Use a different tunnel port if needed:

```bash
./forwardator -s <device_ip> --tcp 502 --udp 47808 --tunnel 19000
./forwardator -c <server_ip> --tcp 502 --udp 47808 --tunnel 19000
```

If your device UDP port is `9000`, set `--tunnel` to a different port because the server needs separate UDP sockets for device input and tunnel registration/data.

Forward only one protocol by omitting the other or setting it to `0`:

```bash
./forwardator -s <device_ip> --tcp 502
./forwardator -c <server_ip> --tcp 502
```

## Options

```text
-c <server_ip>   client mode: server IP/host to connect to
-s <device_ip>   server mode: device IP/host to forward to/filter UDP from
--tcp <port>     TCP port to forward; 0 disables TCP
--udp <port>     UDP device/listener/delivery port; 0 disables UDP
--tunnel <port>  client/server tunnel port; default 9000
--bind <addr>    bind/delivery address; default 127.0.0.1 in client mode, 0.0.0.0 in server mode
```

## Notes

- TCP uses one tunnel connection per local TCP connection and streams data with `io.Copy` in both directions.
- UDP keeps datagram boundaries. Payload bytes and payload length are not changed by `forwardator`.
- The client sends small UDP registration keepalives to the server so the server knows where to forward device datagrams.
- Very large UDP datagrams can exceed the network MTU and may be fragmented or dropped by the network.
