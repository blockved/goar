package goar

import (
	"errors"
	"fmt"
	"github.com/everFinance/goar/types"
)

func (c *Client) BroadcastData(txId string, data []byte, numOfNodes int64, peers ...string) error {
	var err error
	if len(peers) == 0 {
		peers, err = c.GetPeers()
		if err != nil {
			return err
		}
	}

	count := int64(0)
	pNode := NewTempConn()
	for _, peer := range peers {
		pNode.SetTempConnUrl("http://" + peer)
		uploader, err := CreateUploader(pNode, txId, data)
		if err != nil {
			continue
		}

		if err = uploader.Once(); err != nil {
			continue
		}

		count++
		if count >= numOfNodes {
			return nil
		}
	}

	return fmt.Errorf("upload tx data to peers failed, txId: %s", txId)
}

func (c *Client) GetTxDataFromPeers(txId string, peers ...string) ([]byte, error) {
	var err error
	if len(peers) == 0 {
		peers, err = c.GetPeers()
		if err != nil {
			return nil, err
		}
	}

	pNode := NewTempConn()
	for _, peer := range peers {
		pNode.SetTempConnUrl("http://" + peer)
		data, downErr := pNode.DownloadChunkData(txId)
		if downErr != nil {
			err = downErr
			continue
		}
		return data, nil
	}

	return nil, err
}

func (c *Client) GetTxDataFromPeersWithBreaker(txId string) ([]byte, error) {
	var err error
	if c.peers == nil || c.peers.Len() == 0 {
		return nil, fmt.Errorf("peers is empty")
	}
	for i := 0; i < c.peers.Len(); i++ {
		c.lock.Lock()
		c.peers = c.peers.Next()
		peer := c.peers.Value.(string)
		log.Info("GetTxDataFromPeersWithBreaker", "peer", peer)
		c.lock.Unlock()
		//e, b := sentinel.Entry(peer)
		//if b != nil {
		//	continue
		//}
		//if e != nil {
		rst, internalErr := c.GetTxDataFromPeers(txId, []string{peer}...)
		//e.Exit()
		c.lock.Lock()
		ps, ok := c.PeerStatus[peer]
		if !ok {
			ps = &PeerStatus{}
			c.PeerStatus[peer] = ps
		}
		ps.Count += 1
		log.Info("GetTxDataFromPeersWithBreaker", "count", ps.Count)
		if internalErr != nil || len(rst) == 0 {
			//sentinel.TraceError(e, internalErr)
			ps.FailCount += 1
			log.Info("GetTxDataFromPeersWithBreaker", "FailCount", ps.FailCount, "errmsg", internalErr)
			c.lock.Unlock()
			continue
		} else {
			ps.SucCount += 1
			log.Info("GetTxDataFromPeersWithBreaker", "SucCount", ps.SucCount)
			c.lock.Unlock()
			return rst, internalErr
		}
	}
	//}
	return nil, err
}

func (c *Client) GetBlockFromPeers(height int64, peers ...string) (*types.Block, error) {
	var err error
	if len(peers) == 0 {
		peers, err = c.GetPeers()
		if err != nil {
			return nil, err
		}
	}

	pNode := NewTempConn()
	for _, peer := range peers {
		pNode.SetTempConnUrl("http://" + peer)
		block, err := pNode.GetBlockByHeight(height)
		if err != nil {
			continue
		}
		fmt.Printf("success get block; peer: %s\n", peer)
		return block, nil
	}

	return nil, errors.New("get block from peers failed")
}

func (c *Client) GetTxFromPeers(arId string, peers ...string) (*types.Transaction, error) {
	var err error
	if len(peers) == 0 {
		peers, err = c.GetPeers()
		if err != nil {
			return nil, err
		}
	}

	pNode := NewTempConn()
	for _, peer := range peers {
		pNode.SetTempConnUrl("http://" + peer)
		tx, err := pNode.GetTransactionByID(arId)
		if err != nil {
			continue
		}
		fmt.Printf("success get tx; peer: %s, arTx: %s\n", peer, arId)
		return tx, nil
	}

	return nil, fmt.Errorf("get tx failed; arId: %s", arId)
}

func (c *Client) GetUnconfirmedTxFromPeers(arId string, peers ...string) (*types.Transaction, error) {
	var err error
	if len(peers) == 0 {
		peers, err = c.GetPeers()
		if err != nil {
			return nil, err
		}
	}

	pNode := NewTempConn()
	for _, peer := range peers {
		pNode.SetTempConnUrl("http://" + peer)
		tx, err := pNode.GetUnconfirmedTx(arId)
		if err != nil {
			continue
		}
		fmt.Printf("success get unconfirmed tx; peer: %s, arTx: %s\n", peer, arId)
		return tx, nil
	}

	return nil, fmt.Errorf("get unconfirmed tx failed; arId: %s", arId)
}
