package pump

import (
	"fmt"

	shell "github.com/ipfs/go-ipfs-api"
)

var _ Drain = &APIDrain{}

type APIDrain struct {
	s *shell.Shell
}

func NewAPIDrain(URL string) *APIDrain {
	return &APIDrain{
		s: shell.NewShell(URL),
	}
}

func (a *APIDrain) Drain(block Block) error {
	// extra parameters should match the output of the collector
	cid, err := a.s.BlockPut(block.Data, "v0", "sha2-256", -1)
	if err != nil {
		return err
	}

	if cid != block.CID.String() {
		return fmt.Errorf("CID mismatch: expected %s, got %s", block.CID, cid)
	}

	return nil
}
