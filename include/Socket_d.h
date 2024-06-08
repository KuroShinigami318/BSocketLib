#pragma once

#include "details/platforms.h"
#if defined(USE_WIN32_API)
#define PORT uint16_t
#define Socket_d UINT_PTR
#define Socket_AF int
#define Socket_Type int
#define Socket_Protocol int
#define DATA_BUFSIZE 8192
#define BS_INVALID_SOCKET -1

// AF from winsock2
#define BS_AF_UNSPEC       0               // unspecified
#define BS_AF_UNIX         1               // local to host (pipes, portals)
#define BS_AF_INET         2               // internetwork: UDP, TCP, etc.
#define BS_AF_IMPLINK      3               // arpanet imp addresses
#define BS_AF_PUP          4               // pup protocols: e.g. BSP
#define BS_AF_CHAOS        5               // mit CHAOS protocols
#define BS_AF_NS           6               // XEROX NS protocols
#define BS_AF_IPX          BS_AF_NS        // IPX protocols: IPX, SPX, etc.
#define BS_AF_ISO          7               // ISO protocols
#define BS_AF_OSI          BS_AF_ISO       // OSI is ISO
#define BS_AF_ECMA         8               // european computer manufacturers
#define BS_AF_DATAKIT      9               // datakit protocols
#define BS_AF_CCITT        10              // CCITT protocols, X.25 etc
#define BS_AF_SNA          11              // IBM SNA
#define BS_AF_DECnet       12              // DECnet
#define BS_AF_DLI          13              // Direct data link interface
#define BS_AF_LAT          14              // LAT
#define BS_AF_HYLINK       15              // NSC Hyperchannel
#define BS_AF_APPLETALK    16              // AppleTalk
#define BS_AF_NETBIOS      17              // NetBios-style addresses
#define BS_AF_VOICEVIEW    18              // VoiceView
#define BS_AF_FIREFOX      19              // Protocols from Firefox
#define BS_AF_UNKNOWN1     20              // Somebody is using this!
#define BS_AF_BAN          21              // Banyan
#define BS_AF_ATM          22              // Native ATM Services
#define BS_AF_INET6        23              // Internetwork Version 6
#define BS_AF_CLUSTER      24              // Microsoft Wolfpack
#define BS_AF_12844        25              // IEEE 1284.4 WG AF
#define BS_AF_IRDA         26              // IrDA
#define BS_AF_NETDES       28              // Network Designers OSI & gateway
#elif defined(USE_POSIX_API)
#define PORT uint16_t
#define Socket_d int
#define Socket_AF int
#define Socket_Type int
#define Socket_Protocol int
#define BS_INVALID_SOCKET -1
#else
#define PORT uint16_t
#define Socket_d size_t
#define Socket_AF int
#define Socket_Type int
#define Socket_Protocol int
#define BS_INVALID_SOCKET -1

#define BS_AF_UNSPEC       0
#define BS_AF_UNIX         0
#define BS_AF_INET         0
#define BS_AF_IMPLINK      0
#define BS_AF_PUP          0
#define BS_AF_CHAOS        0
#define BS_AF_NS           0
#define BS_AF_IPX          0
#define BS_AF_ISO          0
#define BS_AF_OSI          0
#define BS_AF_ECMA         0
#define BS_AF_DATAKIT      0
#define BS_AF_CCITT        0
#define BS_AF_SNA          0
#define BS_AF_DECnet       0
#define BS_AF_DLI          0
#define BS_AF_LAT          0
#define BS_AF_HYLINK       0
#define BS_AF_APPLETALK    0
#define BS_AF_NETBIOS      0
#define BS_AF_VOICEVIEW    0
#define BS_AF_FIREFOX      0
#define BS_AF_UNKNOWN1     0
#define BS_AF_BAN          0
#define BS_AF_ATM          0
#define BS_AF_INET6        0
#define BS_AF_CLUSTER      0
#define BS_AF_12844        0
#define BS_AF_IRDA         0
#define BS_AF_NETDES       0
#endif