package service

type PacketSend interface {
	Send(data []byte)
}
